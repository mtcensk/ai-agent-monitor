#!/usr/bin/env python3
"""AI-агент мониторинга стенда CntsAdminCon + man_reporting_service — оркестратор.

Тонкая точка входа: фоновые циклы и обработка Telegram-команд.
Логика вынесена в модули-роли:
  common         — конфиг, состояние, HTTP/Telegram-транспорт
  bus            — очередь типизированных задач между ролями (в state.json)
  observer       — health-пробы, метрики, логи, трейсы (только наблюдение)
  diagnostician  — LLM-разбор, RAG, SLO/burn rate, аномалии (только оценка)
  executor       — плейбуки и действия (единственный, кто меняет стенд)
  reporter       — отчёты, дашборды Grafana, post-mortem, доставка

Конфигурация — переменные окружения (app.env), см. common.py.
"""
import json
import subprocess
import threading
import time
from datetime import datetime

import bus
import executor
import reporter
from common import (STATE, BASE, MSK, TELEGRAM_TOKEN, TG_ALLOWED_USERS, LLM_API_KEY,
                    RECOVERY_STABLE_SEC, POSTMORTEM_MIN_DOWNTIME,
                    METRICS_INTERVAL, DISK_ALERT_PCT,
                    REPEAT_ALERT_SEC, ACK_MUTE_SEC,
                    esc, http_json, tg_send, alert, tg_send_document, add_incident, save_state)
from observer import (check_health, check_stack, slo_record_cycle, collect_metrics,
                      read_log_chunk, tempo_traces, smoke_test, report_errors_check)
from diagnostician import (llm, SYS_ANALYST, rag_context, slo_burn_check,
                           baseline_check, process_log_chunk, budget_policy_check,
                           dependency_check, forecast_check, infra_check)

# ---------------------------------------------------------------- цикл мониторинга доступности
DOWN_SINCE = {}
RECOVER_PENDING = {}   # key -> ts первого «здорового» цикла после сбоя
LAST_ALERT = {}        # key -> ts последнего алерта (для re-alert по открытым инцидентам)

def monitor_loop():
    while True:
        app_problems = check_health()
        stack_problems = check_stack()      # стек мониторинга — отдельно от SLI приложений
        slo_record_cycle(app_problems)
        try:
            slo_burn_check()
            budget_policy_check()
        except Exception as e:
            print(f"[slo] {e}", flush=True)
        now = time.time()
        problems = app_problems + stack_problems
        prob_keys = {p.split("->")[0].split(":")[0] for p in problems}
        # --- новые сбои: коррелируем в ОДИН инцидент ---
        new = [p for p in problems if p.split("->")[0].split(":")[0] not in DOWN_SINCE]
        for p in new:
            key = p.split("->")[0].split(":")[0]
            DOWN_SINCE[key] = now
            LAST_ALERT[key] = now
            add_incident("down", p)
        if new:
            joined = "\n".join(f"• {p}" for p in new)
            traces = tempo_traces(minutes=15)
            trace_ctx = ("\nНедавние трейсы приложений (Tempo, 15 мин):\n" + "\n".join(traces)) if traces else ""
            analysis = llm(SYS_ANALYST,
                           "Проблемы мониторинга:\n" + joined + trace_ctx + rag_context(joined)) if LLM_API_KEY else None
            if analysis:
                # MTTRC ([64]): фиксируем время первой гипотезы о причине
                rc_ts = datetime.now(MSK).isoformat(timespec="seconds")
                for i in STATE["incidents"][-len(new):]:
                    i["rc_ts"] = rc_ts
            alert(f"🔴 ИНЦИДЕНТ {datetime.now(MSK):%H:%M:%S}\n{joined}" +
                  (f"\n\nАнализ: {analysis}" if analysis else ""))
        # --- восстановления с анти-флаппингом: ждём RECOVERY_STABLE_SEC стабильности ---
        recovered = []
        for key in list(DOWN_SINCE):
            if key in prob_keys:
                RECOVER_PENDING.pop(key, None)   # снова упал в период ожидания — флаппинг гасим
                continue
            first_ok = RECOVER_PENDING.setdefault(key, now)
            if now - first_ok < RECOVERY_STABLE_SEC:
                continue
            dur = int(first_ok - DOWN_SINCE.pop(key))
            RECOVER_PENDING.pop(key, None)
            LAST_ALERT.pop(key, None)
            STATE["acks"].pop(key, None)          # ack действует до восстановления
            add_incident("up", f"{key} восстановлен, простой {dur} с")
            recovered.append((key, dur, STATE["incidents"][-1]))
        if recovered:
            lines = "\n".join(f"• {k} снова в норме (простой {d} с)" for k, d, _ in recovered)
            alert(f"🟢 ВОССТАНОВЛЕНО {datetime.now(MSK):%H:%M:%S}\n{lines}")
            for k, d, inc in recovered:
                if d >= POSTMORTEM_MIN_DOWNTIME:
                    tg_send(f"⏳ Простой {d} с — готовлю post-mortem…")
                    threading.Thread(target=reporter.maybe_postmortem, args=(inc, d), daemon=True).start()
        # --- re-alert по открытым инцидентам: молчание ≠ всё хорошо (практика [81]) ---
        acks = STATE["acks"]
        reopen = [k for k in DOWN_SINCE
                  if now - LAST_ALERT.get(k, 0) >= REPEAT_ALERT_SEC and acks.get(k, 0) < now]
        if reopen:
            for k in reopen:
                LAST_ALERT[k] = now
            lines = "\n".join(f"• {k} — простой уже {int(now - DOWN_SINCE[k])} с" for k in reopen)
            alert(f"🔴 ИНЦИДЕНТ ВСЁ ЕЩЁ ОТКРЫТ {datetime.now(MSK):%H:%M:%S}\n{lines}\n"
                  f"Заглушить повторы на {ACK_MUTE_SEC // 3600} ч: /ack")
        # --- истечение /mute: отчёт о подавленных алертах (практика [143]) ---
        mu = STATE.get("mute_until", 0)
        if 0 < mu <= now:
            STATE["mute_until"] = 0
            muted = STATE["self_stats"].get("muted_alerts", 0)
            STATE["self_stats"]["muted_alerts"] = 0
            if muted:
                tg_send(f"🔔 Mute истёк. Пока он действовал, подавлено алертов: {muted} — "
                        f"проверьте /status и /errors.")
        # --- плейбуки на падение процессов (enforce -> задача в шину) ---
        try:
            executor.evaluate_process_playbooks(DOWN_SINCE)
        except Exception as e:
            print(f"[proc playbooks] {e}", flush=True)
        # --- smoke-пробы после деплоя (маркеры /app/ai_monitor/deploy_marks) ---
        try:
            handle_deploy_marks()
        except Exception as e:
            print(f"[deploy smoke] {e}", flush=True)
        # --- heartbeat для внешнего пробника ---
        try:
            (BASE / "heartbeat").write_text(str(int(now)))
        except Exception:
            pass
        save_state()
        time.sleep(60)

# ---------------------------------------------------------------- smoke-пробы после деплоя
def _smoke_verdict(app):
    """Прогон smoke-проб и verdict по деплою (практики [140]/[144])."""
    ok, per_url, p95 = smoke_test(app)
    lines = "\n".join(f"• {u}: {d['ok']}/{d['total']} ответов 200" for u, d in per_url.items())
    verdict = "✅ SMOKE OK" if ok else "🔥 SMOKE FAILED"
    add_incident("deploy", f"{app}: {verdict}, p95 {p95} мс")
    save_state()
    advice = ("" if ok else
              "\nДеплой нельзя считать завершённым: рекомендуется ОТКАТ по плану ([144]).")
    alert(f"{verdict}: {app}\n{lines}\nЛатентность p95: {p95} мс" + advice, ticket=ok)

def handle_deploy_marks():
    """Маркеры деплоя: deploy_mark.sh кладёт файл deploy_marks/<app>.

    Агент прогоняет усиленные пробы и пишет verdict; маркер снимается
    в любом случае (один маркер — один прогон).
    """
    marks_dir = BASE / "deploy_marks"
    if not marks_dir.exists():
        return
    for mark in marks_dir.iterdir():
        app = mark.name
        try:
            age = time.time() - mark.stat().st_mtime
            mark.unlink()
        except Exception:
            continue
        if age > 3600:
            print(f"[deploy smoke] маркер {app} протух ({int(age)} с), пропускаю", flush=True)
            continue
        tg_send(f"⏳ Деплой {app}: запускаю smoke-пробы ({age:.0f} с назад отмечен)…")
        threading.Thread(target=_smoke_verdict, args=(app,), daemon=True).start()

# ---------------------------------------------------------------- цикл чтения логов
def tail_loop():
    while True:
        for app in ("man_reporting", "cntsadmin"):
            try:
                chunk = read_log_chunk(app)
                if chunk:
                    process_log_chunk(app, chunk)
            except Exception as e:
                print(f"[tail {app}] {e}", flush=True)
        save_state()
        time.sleep(10)

# ---------------------------------------------------------------- цикл метрик
def metrics_loop():
    while True:
        try:
            m = collect_metrics()
            hist = STATE["metrics_history"]
            hist.append(m)
            del hist[:-600]                    # ~2 суток при интервале 5 мин
            if m["disk_pct"] >= DISK_ALERT_PCT and time.time() - STATE.get("disk_alert_ts", 0) > 21600:
                STATE["disk_alert_ts"] = time.time()
                alert(f"🟠 ДИСК заполнен на {m['disk_pct']}% (свободно {m['disk_free_gb']} ГБ) "
                      f"— порог {DISK_ALERT_PCT}%", ticket=True)
            anomalies = baseline_check(hist)
            if anomalies:
                joined = "\n".join(anomalies)
                analysis = llm(SYS_ANALYST,
                               "Аномалии метрик (отклонение >2σ от суточной нормы):\n"
                               + joined + rag_context(joined)) if LLM_API_KEY else None
                alert(f"🟠 АНОМАЛИЯ МЕТРИК {datetime.now(MSK):%H:%M:%S}\n{joined}" +
                      (f"\n\nАнализ: {analysis}" if analysis else ""), ticket=True)
            dep_problems = dependency_check(m)
            if dep_problems:
                joined = "\n".join(dep_problems)
                analysis = llm(SYS_ANALYST,
                               "Проблемы зависимости PostgreSQL (RED-метрики за warning-порогом):\n"
                               + joined + rag_context(joined)) if LLM_API_KEY else None
                alert(f"🟠 ЗАВИСИМОСТЬ PostgreSQL {datetime.now(MSK):%H:%M:%S}\n{joined}" +
                      (f"\n\nАнализ: {analysis}" if analysis else ""), ticket=True)
            forecasts = forecast_check(hist)
            if forecasts:
                alert(f"🔮 ПРОГНОЗ (линейный тренд 24 ч) {datetime.now(MSK):%H:%M:%S}\n" +
                      "\n".join(forecasts), ticket=True)
            infra = infra_check(m)
            if infra:
                alert(f"🟠 ИНФРАСТРУКТУРА {datetime.now(MSK):%H:%M:%S}\n" +
                      "\n".join(infra), ticket=True)
            rep_err = report_errors_check()
            if rep_err:
                alert(f"📄 ОШИБКА ОТЧЁТА man_reporting {datetime.now(MSK):%H:%M:%S}\n" +
                      "\n".join(rep_err), ticket=True)
            executor.evaluate_playbooks(hist)
            save_state()
        except Exception as e:
            print(f"[metrics] {e}", flush=True)
        time.sleep(METRICS_INTERVAL)

# ---------------------------------------------------------------- цикл ежедневного отчёта
def report_loop():
    while True:
        now = datetime.now(MSK)
        if now.hour == reporter.REPORT_HOUR and STATE.get("last_report_date") != now.strftime("%Y-%m-%d"):
            STATE["last_report_date"] = now.strftime("%Y-%m-%d")
            save_state()
            reporter.daily_report()
            try:
                executor.playbook_review_check()   # свежесть ранбуков ([124])
            except Exception as e:
                print(f"[playbook review] {e}", flush=True)
        time.sleep(60)

# ---------------------------------------------------------------- команды Telegram
def handle_command(text, cid):
    cmd = text.split()[0].lower().lstrip("/").split("@")[0]
    if cmd == "start":
        if len(text.split()) > 1 and text.split()[1].lower() == "ticket":
            STATE["ticket_chat_id"] = cid
            save_state()
            tg_send(f"✅ Этот чат подключён как ticket-канал (chat_id={cid}).\n"
                    f"Сюда идут P2/P3: ошибки логов, аномалии метрик, dry-run плейбуки, "
                    f"burn rate slow/long, статусы бюджета. Page-алерты — в основной чат.", cid)
            return
        STATE["chat_id"] = cid
        save_state()
        tg_send(f"✅ Агент мониторинга подключён (chat_id={cid}).\n"
                f"Команды: /status /report /metrics /slo /trace /errors /logs /restart /chaos /ack /mute /fb "
                f"/postmortem /autonomy /bus /ask\n"
                f"Подключить ticket-канал для P2/P3: /start ticket в другом чате",
                cid)
    elif STATE.get("chat_id") != cid:
        tg_send("⛔ Бот обслуживает только зарегистрированный чат.", cid)
    elif cmd == "status":
        problems = check_health()
        stack = check_stack()
        parts = []
        parts.append("🟢 Приложения в норме" if not problems
                     else "🔴 Проблемы приложений:\n" + "\n".join(problems))
        parts.append("🟢 Стек мониторинга в норме" if not stack
                     else "🟠 Проблемы стека мониторинга:\n" + "\n".join(stack))
        st = STATE["self_stats"]
        streak = st.get("tg_fail_streak", 0)
        parts.append(f"Канал Telegram: {'ок' if streak == 0 else f'⚠️ сбоев подряд: {streak}'} "
                     f"(всего сбоев {st.get('tg_fails', 0)}, отказов неавторизованным {st.get('tg_denied', 0)}) · "
                     f"LLM: {'ок' if st.get('llm_last_ok') else 'нет успешных вызовов'} "
                     f"(сбоев {st.get('llm_fails', 0)})")
        dep = (STATE["metrics_history"][-1].get("pg_dep")
               if STATE.get("metrics_history") else None) or {}
        if dep:
            parts.append(f"Зависимость PostgreSQL: latency {dep.get('latency_ms', '—')} мс, "
                         f"активных запросов {dep.get('active', '—')}, "
                         f"самый длинный {dep.get('longest_active_s', '—')} с, "
                         f"lock-wait {dep.get('lock_waits', '—')}, conns {dep.get('conns_pct', '—')}%")
        if DOWN_SINCE:
            open_lines = "\n".join(f"• {k} ({int(time.time() - v)} с)" for k, v in DOWN_SINCE.items())
            parts.append(f"Открытые инциденты:\n{open_lines}")
        tg_send("\n".join(parts))
    elif cmd == "report":
        tg_send("⏳ Собираю отчёт…")
        threading.Thread(target=reporter.daily_report, daemon=True).start()
    elif cmd == "metrics":
        hours = int(text.split()[1]) if len(text.split()) > 1 and text.split()[1].isdigit() else 24
        hours = max(1, min(hours, 48))
        msg = reporter.metrics_message()
        if msg:
            tg_send(msg, parse_mode="HTML")
        tg_send(f"⏳ Рендерю дашборды Grafana за {hours} ч…")
        if reporter.send_dashboards(hours=hours) == 0:
            tg_send("❌ Дашборды Grafana недоступны, попробуйте позже.")
        try:
            tg_send_document(reporter.build_html_report(), "Полный HTML-отчёт по метрикам")
        except Exception as e:
            print(f"[html report] {e}", flush=True)
    elif cmd == "slo":
        tg_send(reporter.slo_message(), parse_mode="HTML")
    elif cmd == "trace":
        mins = int(text.split()[1]) if len(text.split()) > 1 and text.split()[1].isdigit() else 15
        mins = max(5, min(mins, 120))
        tr = tempo_traces(minutes=mins, limit=10)
        if tr is None:
            tg_send("Tempo недоступен.")
        elif not tr:
            tg_send(f"Трейсов за {mins} мин нет.")
        else:
            tg_send(f"🔎 Трейсы за {mins} мин:\n" + "\n".join(f"• {t}" for t in tr))
    elif cmd == "errors":
        errs = [i for i in STATE["incidents"] if i["kind"] == "error"][-10:]
        tg_send("Последние ошибки:\n" + ("\n\n".join(f"{i['ts']}\n{i['text'][:250]}" for i in errs) or "нет"))
    elif cmd == "logs":
        n = int(text.split()[1]) if len(text.split()) > 1 and text.split()[1].isdigit() else 15
        out = subprocess.run(["tail", "-n", str(min(n, 50)), "/app/man_reporting/logs/app.log"],
                             capture_output=True, text=True).stdout
        tg_send(f"```\n{out[-3500:]}\n```")
    elif cmd == "restart":
        target = text.split()[1] if len(text.split()) > 1 else ""
        if target not in ("man_reporting", "cntsadmin"):
            tg_send("Формат: /restart man_reporting  или  /restart cntsadmin")
            return
        STATE["pending_action"] = {"action": f"restart {target}", "ts": time.time()}
        save_state()
        tg_send(f"⚠️ Подтвердите перезапуск <b>{esc(target)}</b>: /confirm\n"
                f"Отмена: /cancel (заявка действует 5 минут)", parse_mode="HTML")
    elif cmd == "confirm":
        pa = STATE.get("pending_action")
        if not pa or time.time() - pa["ts"] > 300:
            tg_send("Нет действия, ожидающего подтверждения.")
            return
        STATE["pending_action"] = None
        save_state()
        tg_send(f"⏳ Выполняю: {pa['action']}…")
        runner = executor.run_chaos if pa["action"] in executor.CHAOS_ACTIONS else executor.run_action
        threading.Thread(target=runner, args=(pa["action"],), daemon=True).start()
    elif cmd == "chaos":
        # /chaos kill man_reporting|cntsadmin — учебный инцидент, хаос-уровень 1 ([94]/[122])
        parts = text.split()
        if len(parts) == 3 and parts[1].lower() == "kill" and parts[2] in ("man_reporting", "cntsadmin"):
            STATE["pending_action"] = {"action": f"chaos kill {parts[2]}", "ts": time.time()}
            save_state()
            tg_send(f"⚠️ УЧЕБНЫЙ ИНЦИДЕНТ: убить процесс <b>{esc(parts[2])}</b>.\n"
                    f"Ожидаемая реакция: cron-watchdog поднимет процесс ~1 мин, агент зафиксирует "
                    f"инцидент и восстановление (проверка MTTD).\n"
                    f"Подтвердите: /confirm · Отмена: /cancel", parse_mode="HTML")
        else:
            tg_send("Формат: /chaos kill man_reporting  или  /chaos kill cntsadmin\n"
                    "Учебный инцидент: проверка обнаружения и авто-восстановления.")
    elif cmd == "cancel":
        STATE["pending_action"] = None
        save_state()
        tg_send("Отменено.")
    elif cmd == "ack":
        if not DOWN_SINCE:
            tg_send("Открытых инцидентов нет — подтверждать нечего.")
        else:
            until = time.time() + ACK_MUTE_SEC
            for k in DOWN_SINCE:
                STATE["acks"][k] = until
            save_state()
            tg_send(f"🔕 Принято. Повторы по открытым инцидентам заглушены на "
                    f"{ACK_MUTE_SEC // 3600} ч (до {time.strftime('%H:%M', time.localtime(until))}).\n"
                    f"Новые инциденты и сообщения о восстановлении придут как обычно.")
    elif cmd == "mute":
        arg = text.split()[1].lower() if len(text.split()) > 1 else ""
        if arg in ("off", "0"):
            STATE["mute_until"] = 0
            save_state()
            tg_send("🔔 Mute снят, алерты снова доставляются.")
        else:
            mins = int(arg) if arg.isdigit() else 60
            mins = max(1, min(mins, 1440))          # silence всегда временный, максимум 24 ч ([93])
            STATE["mute_until"] = time.time() + mins * 60
            save_state()
            tg_send(f"🔕 Алерты заглушены на {mins} мин "
                    f"(до {time.strftime('%H:%M %d.%m', time.localtime(STATE['mute_until']))}).\n"
                    f"Ответы на команды не глушатся. Снять раньше: /mute off")
    elif cmd == "fb":
        arg = text.split()[1].lower() if len(text.split()) > 1 else ""
        la = STATE.get("last_alert")
        if arg not in ("good", "bad"):
            tg_send("Оценка качества последнего алерта: /fb good — был полезен (actionable), "
                    "/fb bad — ложный или шум.\n"
                    f"Последний алерт: {(la or {}).get('text', '—')[:100]}")
        elif not la:
            tg_send("Алертов ещё не было — оценивать нечего.")
        else:
            fb = STATE["alert_feedback"]
            fb[arg] += 1
            fb["log"].append({"ts": time.time(), "rating": arg, "alert": la["text"][:80]})
            fb["log"] = fb["log"][-100:]
            save_state()
            g, b = fb["good"], fb["bad"]
            pct = round(100 * g / (g + b)) if g + b else 100
            tg_send(f"Спасибо, оценка учтена.\nКачество алертов: <b>{pct}%</b> actionable "
                    f"(good {g} / bad {b}) — цель ≥ 80% (правило 80/20, [1]).", parse_mode="HTML")
    elif cmd == "autonomy":
        arg = text.split()[1].lower() if len(text.split()) > 1 else ""
        if arg in ("off", "dryrun", "enforce"):
            STATE["autonomy"] = arg
            save_state()
            tg_send(f"⚙️ Режим автономии: <b>{arg}</b>\n" +
                    {"off": "плейбуки отключены.",
                     "dryrun": "плейбуки только уведомляют, действий нет.",
                     "enforce": "плейбуки выполняют действия автоматически."}[arg],
                    parse_mode="HTML")
        else:
            tg_send(f"⚙️ Режим автономии: <b>{esc(STATE.get('autonomy', 'dryrun'))}</b>\n"
                    f"Переключить: /autonomy off | dryrun | enforce", parse_mode="HTML")
    elif cmd == "bus":
        tasks = bus.pending()
        if not tasks:
            tg_send("Очередь задач пуста.")
        else:
            lines = [f"• {time.strftime('%H:%M:%S', time.localtime(t['ts']))} [{t['kind']}] "
                     f"{json.dumps(t['payload'], ensure_ascii=False)[:120]}" for t in tasks[-10:]]
            tg_send("📥 Очередь задач:\n" + "\n".join(lines))
    elif cmd == "postmortem":
        ups = [i for i in STATE["incidents"] if i["kind"] == "up" and "простой" in i["text"]]
        n = int(text.split()[1]) if len(text.split()) > 1 and text.split()[1].isdigit() else 1
        if not (1 <= n <= len(ups)):
            tg_send(f"Инцидентов с простоем: {len(ups)}. Формат: /postmortem [N с конца]")
        else:
            tg_send("⏳ Готовлю post-mortem…")
            threading.Thread(target=reporter._postmortem_cmd, args=(ups[-n],), daemon=True).start()
    elif cmd == "ask":
        q = text.partition(" ")[2]
        if not q:
            tg_send("Формат: /ask ваш вопрос о стенде")
            return
        f = reporter.collect_facts()
        ans = llm(SYS_ANALYST, f"Факты о стенде:\n{json.dumps(f, ensure_ascii=False, default=str)[:3000]}\n\nВопрос: {q}")
        tg_send(ans or "LLM недоступен, не могу ответить.")
    else:
        tg_send("Команды: /status /report /metrics /slo /trace [мин] /errors /logs [N] "
                "/restart man_reporting|cntsadmin /chaos kill <app> /ack /mute [мин|off] /fb good|bad "
                "/postmortem [N] /autonomy /bus /ask <вопрос>")

# ---------------------------------------------------------------- telegram polling
def tg_poll():
    while True:
        try:
            res = http_json(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                            {"offset": STATE["tg_offset"], "timeout": 30}, timeout=40)
            for upd in res.get("result", []):
                STATE["tg_offset"] = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = (msg.get("text") or "").strip()
                cid = (msg.get("chat") or {}).get("id")
                uid = (msg.get("from") or {}).get("id")
                if text and cid:
                    if not tg_allowed_uid(uid):
                        STATE["self_stats"]["tg_denied"] = STATE["self_stats"].get("tg_denied", 0) + 1
                        print(f"[tg auth] отказ: uid={uid} cid={cid} text={text[:60]!r}", flush=True)
                        continue
                    handle_command(text, cid)
            save_state()
        except Exception as e:
            print(f"[tg poll] {e}", flush=True)
            time.sleep(10)

def tg_allowed_uid(uid):
    """Белый список Telegram user id. Пустой TG_ALLOWED_USERS → только владелец
    зарегистрированного chat_id (в личке chat_id == user id); если никто не
    зарегистрирован — разрешаем первичное подключение (/start)."""
    allowed = TG_ALLOWED_USERS
    if not allowed:
        owner = STATE.get("chat_id")
        if owner is None:
            return True
        allowed = {owner}
    return uid in allowed

def notify_deploy():
    """Уведомление об автодеплое. CI (GitHub Actions) кладёт рядом с кодом
    deploy_info.json (sha коммита, сообщение, автор) ДО перезапуска агента.
    При старте сравниваем sha с последним оповещённым — рассылаем один раз."""
    try:
        info = json.loads((BASE / "deploy_info.json").read_text(encoding="utf-8"))
    except Exception:
        return  # маркера нет (ручной перезапуск) — молчим
    sha = info.get("sha")
    if not sha or sha == STATE.get("deploy_notified_sha"):
        return
    STATE["deploy_notified_sha"] = sha
    save_state()
    msg = (info.get("msg") or "").strip()
    short = info.get("short") or sha[:7]
    try:
        tg_send(f"🚀 Автодеплой: коммит <code>{esc(short)}</code>"
                + (f" — {esc(msg)}" if msg else "")
                + f"\n👤 {esc(info.get('actor') or '?')} · рестарт успешен, агент работает.",
                parse_mode="HTML")
    except Exception as e:
        print(f"[deploy notify] {e}", flush=True)

# ---------------------------------------------------------------- main
if __name__ == "__main__":
    print(f"ai_monitor started {datetime.now(MSK):%Y-%m-%d %H:%M:%S} MSK", flush=True)
    if not TELEGRAM_TOKEN:
        print("WARNING: TELEGRAM_TOKEN не задан — отправка сообщений отключена", flush=True)
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=tail_loop, daemon=True).start()
    threading.Thread(target=report_loop, daemon=True).start()
    threading.Thread(target=metrics_loop, daemon=True).start()
    threading.Thread(target=executor.executor_loop, daemon=True).start()
    notify_deploy()
    if TELEGRAM_TOKEN:
        tg_poll()
    else:
        while True:
            time.sleep(3600)
