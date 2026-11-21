#!/usr/bin/env python3
"""reporter — роль «докладчик»: отчёты, дашборды, доставка в Telegram.

Структурированные сообщения метрик, рендеры дашбордов Grafana,
ежедневный отчёт (LLM + fallback), HTML-отчёт, SLO-статус, post-mortem.
Ничего не исполняет на стенде — только читает и отправляет.
"""
import base64
import json
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from common import (STATE, BASE, MSK, REPORT_HOUR, GRAFANA_URL, GRAFANA_TOKEN,
                    GRAFANA_DASHBOARDS, SLO_LAT_MS, SLO_BURN_FAST, SLO_BURN_SLOW,
                    LLM_API_KEY, esc, _n, http_text, tg_send, tg_send_document,
                    tg_send_photo, save_state)
from observer import check_health, metrics_summary, tempo_traces
from diagnostician import llm, SYS_ANALYST, slo_report, budget_policy

# ---------------------------------------------------------------- таблицы метрик
def _metrics_rows(sm):
    return [
        ("Диск /, %", sm.get("disk_pct")),
        ("Свободно на диске, ГБ", sm.get("disk_free_gb")),
        ("Inode /, %", sm.get("inode_pct")),
        ("Память, %", sm.get("mem_pct")),
        ("Swap, МБ", sm.get("swap_used_mb")),
        ("Load average 1м", sm.get("load1")),
        ("RSS man_reporting, МБ", sm.get("man_rep_rss_mb")),
        ("Потоки man_reporting", sm.get("man_rep_threads")),
        ("RSS cntsadmin, МБ", sm.get("cntsadmin_rss_mb")),
        ("БД man_reporting, МБ", sm.get("pg_db_mb")),
        ("Соединения PostgreSQL", sm.get("pg_conns")),
        ("Генератор данных: запуск, ч назад", sm.get("data_gen_age_h")),
        ("Генератор данных: строк за запуск", sm.get("data_gen_rows")),
        ("Генератор данных: ошибок", sm.get("data_gen_errors")),
    ]

def metrics_message():
    """Структурированное HTML-сообщение с таблицей метрик для Telegram."""
    sm = metrics_summary()
    if not sm:
        return None
    head = f"{'Метрика':<26}{'now':>8}{'min':>8}{'avg':>8}{'max':>8}"
    rows = []
    for name, s in _metrics_rows(sm):
        if s:
            rows.append(f"{name:<26}{_n(s['now']):>8}{_n(s['min']):>8}{_n(s['avg']):>8}{_n(s['max']):>8}")
        else:
            rows.append(f"{name:<26}{'—':>8}{'—':>8}{'—':>8}{'—':>8}")
    parts = [f"<b>📈 Метрики стенда</b> (аптайм {_n(sm.get('uptime_h'))} ч, "
             f"CPU busy за период: <b>{_n(sm.get('cpu_busy_pct_за_период'))}%</b>)",
             f"<pre>{head}\n" + "\n".join(rows) + "</pre>"]
    app_dyn = sm.get("app_metrics_за_сутки_первое_и_последнее") or {}
    if app_dyn:
        lines = [f"{k.replace('man_reporting_', ''):<38}{_n(v[0]):>12} → {_n(v[1])}"
                 for k, v in list(app_dyn.items())[:12]]
        parts.append("<b>Метрики приложения</b> (24 ч назад → сейчас):\n<pre>" + "\n".join(lines) + "</pre>")
    return "\n".join(parts)

def slo_message():
    """Краткий SLO-статус для Telegram (/slo)."""
    r = slo_report()
    if not r:
        return "SLO: данных пока мало (нужно ≥ 10 проб за 30 дней — накапливаются сейчас)."
    p = budget_policy()
    policy = {"norm": "🟢 норма", "caution": "🟠 осторожность", "freeze": "🔴 заморозка"}[p["status"]] if p else "—"
    return (f"🎯 <b>SLO стенда</b> (цель {r['цель_доступности_%']}% за 30 дн)\n"
            f"Доступность: <b>{r['доступность_30д_%']}%</b> ({r['проб_за_30д']} проб)\n"
            f"Латентность проб: p95 = {_n(r['латентность_p95_мс'])} мс, "
            f"в пределах {SLO_LAT_MS} мс — {_n(r['sli_латентности_%'])}%\n"
            f"Бюджет ошибок: осталось <b>{_n(r['бюджет_осталось_%'])}%</b> "
            f"(потрачено {r['бюджет_ошибок_потрачено']} из {r['бюджет_ошибок_всего_проб']}) — {policy}\n"
            f"Burn rate: 1 ч = {_n(r['burn_rate_1ч'])}, 6 ч = {_n(r['burn_rate_6ч'])}, "
            f"3 д = {_n(r.get('burn_rate_3д'))} (пороги {SLO_BURN_FAST:g} / {SLO_BURN_SLOW:g})")

# ---------------------------------------------------------------- grafana render
def grafana_render(uid, slug, path, hours=24, width=1400, height=1000):
    """Рендер дашборда Grafana в PNG через /render API."""
    if not GRAFANA_TOKEN:
        return False
    url = (f"{GRAFANA_URL}/render/d/{uid}/{slug}?orgId=1&width={width}&height={height}"
           f"&kiosk=tv&from=now-{hours}h&to=now&tz=Europe%2FMoscow")
    for attempt in range(2):
        try:
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {GRAFANA_TOKEN}")
            req.add_header("User-Agent", "ai-monitor/1.0 (+https://localhost)")
            with urllib.request.urlopen(req, timeout=90) as r:
                data = r.read()
            if len(data) > 20000:          # настоящий рендер, не заглушка-ошибка
                Path(path).write_bytes(data)
                return True
            print(f"[grafana render {uid}] подозрительно маленький ответ: {len(data)} б", flush=True)
        except Exception as e:
            print(f"[grafana render {uid} attempt {attempt + 1}] {e}", flush=True)
            time.sleep(5)
    return False

def dashboard_images(hours=24, max_age=300):
    """Файлы рендеров дашбордов [(path, caption)]; свежие (< max_age с) переиспользуются."""
    out = []
    for uid, slug, caption in GRAFANA_DASHBOARDS:
        p = BASE / f"dash_{uid}.png"
        fresh = p.exists() and time.time() - p.stat().st_mtime < max_age
        if fresh or grafana_render(uid, slug, p, hours=hours):
            out.append((p, f"{caption} ({hours} ч)"))
    return out

def send_dashboards(chat_id=None, hours=24):
    """Шлёт фото дашбордов Grafana; возвращает число отправленных."""
    sent = 0
    for p, caption in dashboard_images(hours=hours):
        tg_send_photo(p, f"📊 {caption}", chat_id)
        sent += 1
    return sent

# ---------------------------------------------------------------- факты и отчёты
def reliability_stats(days=7):
    """MTTR по фактическим простоям; MTTD ограничен интервалом опроса (60 с).

    MTTRC ([64]) — два показателя наблюдаемости:
    - MTTRC_гипотеза: время до первой LLM-гипотезы о причине (rc_ts);
    - MTTRC_подтв: время до подтверждённой первопричины — момент генерации
      post-mortem (rc_confirmed_ts), который фиксирует разбор инцидента.
    """
    cut = (datetime.now(MSK) - timedelta(days=days)).isoformat(timespec="seconds")
    downs = [i for i in STATE["incidents"] if i["kind"] == "down" and i["ts"] >= cut]
    durs = []
    for i in STATE["incidents"]:
        if i["kind"] == "up" and i["ts"] >= cut:
            m = re.search(r"простой (\d+) с", i["text"])
            if m:
                durs.append(int(m.group(1)))
    rcs, rcs_conf = [], []
    for i in downs:
        if i.get("rc_ts"):
            rcs.append(int((datetime.fromisoformat(i["rc_ts"]) - datetime.fromisoformat(i["ts"])).total_seconds()))
        if i.get("rc_confirmed_ts"):
            rcs_conf.append(int((datetime.fromisoformat(i["rc_confirmed_ts"]) - datetime.fromisoformat(i["ts"])).total_seconds()))
    return {
        "инцидентов_за_неделю": len(downs),
        "MTTR_сек_средний": round(sum(durs) / len(durs)) if durs else None,
        "MTTR_сек_макс": max(durs) if durs else None,
        "MTTD_сек_не_хуже": 60,
        "MTTRC_сек_средний": round(sum(rcs) / len(rcs)) if rcs else None,
        "MTTRC_подтв_сек_средний": round(sum(rcs_conf) / len(rcs_conf)) if rcs_conf else None,
        "MTTRC_подтв_выборка": len(rcs_conf),
    }

def monthly_slo_summary():
    """Календарный срез SLO за прошлый месяц (практика [27]: rolling для операций,
    calendar — для отчётности). None, если данных за месяц нет."""
    prev = (datetime.now(MSK).replace(day=1) - timedelta(days=1))
    pref = prev.strftime("%Y-%m")
    ok = tot = 0
    for k, b in STATE.get("slo_hourly", {}).items():
        if k.startswith(pref):
            ok += b["ok"]
            tot += b["total"]
    if not tot:
        return None
    return {"месяц": pref, "доступность_%": round(100 * ok / tot, 3), "проб": tot}

def collect_facts():
    facts = {"time": datetime.now(MSK).strftime("%Y-%m-%d %H:%M MSK")}
    problems = check_health()
    facts["health"] = "все компоненты в норме" if not problems else "; ".join(problems)
    try:
        _, m = http_text("http://127.0.0.1:5080/metrics")
        facts["metrics"] = "\n".join(l for l in m.splitlines()
                                     if l.startswith("man_reporting_") and not l.startswith("#"))[:1500]
    except Exception as e:
        facts["metrics"] = f"недоступны: {e}"
    try:
        out = subprocess.run(["sudo", "-u", "postgres", "psql", "-X", "-t", "-A", "-d", "man_reporting", "-c",
                              "SELECT id, report_name, report_enabled, report_last_state IS NOT NULL, "
                              "report_last_runtime FROM centros_monitor_db.man_reporting_settings ORDER BY id"],
                             capture_output=True, text=True, timeout=15)
        facts["reports"] = out.stdout.strip() or "пусто"
    except Exception as e:
        facts["reports"] = f"ошибка запроса: {e}"
    try:
        facts["disk"] = subprocess.run(["df", "-h", "/"], capture_output=True, text=True).stdout.splitlines()[-1]
        facts["mem"] = subprocess.run(["free", "-h"], capture_output=True, text=True).stdout.splitlines()[1]
    except Exception:
        pass
    facts["incidents_24h"] = [i for i in STATE["incidents"]
                              if i["ts"] >= (datetime.now(MSK) - timedelta(hours=24)).isoformat(timespec="seconds")]
    today = datetime.now(MSK).strftime("%Y-%m-%d")
    facts["errors_today"] = STATE["error_counts"].get(today, 0)
    facts["sys_metrics_24h"] = metrics_summary()
    facts["reliability_7d"] = reliability_stats()
    facts["slo"] = slo_report()
    facts["slo_месяц_календарный"] = monthly_slo_summary() if datetime.now(MSK).day == 1 else None
    fb = STATE.get("alert_feedback") or {}
    g, b = fb.get("good", 0), fb.get("bad", 0)
    facts["качество_алертов"] = {
        "good": g, "bad": b,
        "actionable_%": round(100 * g / (g + b)) if g + b else None,
        "подавлено_mute": STATE.get("self_stats", {}).get("muted_alerts", 0),
    }
    facts["mute_активен_до"] = (datetime.fromtimestamp(STATE["mute_until"], MSK).strftime("%H:%M %d.%m")
                                if STATE.get("mute_until", 0) > time.time() else None)
    facts["traces_15m"] = tempo_traces(minutes=15)
    return facts

def build_html_report():
    """Самодостаточный HTML-отчёт: таблицы + дашборды Grafana (base64). Возвращает путь к файлу."""
    f = collect_facts()
    sm = f.get("sys_metrics_24h") or {}
    charts = dashboard_images()
    rows = "".join(
        f"<tr><td>{esc(name)}</td><td>{_n(s['now'])}</td><td>{_n(s['min'])}</td>"
        f"<td>{_n(s['avg'])}</td><td>{_n(s['max'])}</td></tr>" if s else
        f"<tr><td>{esc(name)}</td><td colspan=4>нет данных</td></tr>"
        for name, s in _metrics_rows(sm))
    app_dyn = sm.get("app_metrics_за_сутки_первое_и_последнее") or {}
    app_rows = "".join(
        f"<tr><td>{esc(k)}</td><td>{_n(v[0])}</td><td>{_n(v[1])}</td>"
        f"<td>{_n(round(v[1] - v[0], 2)) if isinstance(v[0], (int, float)) and isinstance(v[1], (int, float)) else '—'}</td></tr>"
        for k, v in app_dyn.items())
    inc_rows = "".join(
        f"<tr><td>{esc(i['ts'])}</td><td>{esc(i['kind'])}</td><td style='text-align:left'>{esc(i['text'][:160])}</td></tr>"
        for i in f.get("incidents_24h", []))
    imgs = "".join(
        f"<h3>📊 {esc(cap)}</h3><img alt='dashboard' src='data:image/png;base64,"
        f"{base64.b64encode(p.read_bytes()).decode()}'>"
        for p, cap in charts)
    html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<title>Отчёт стенда {esc(f['time'])}</title>
<style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; max-width: 960px; margin: 24px auto; padding: 0 16px; color: #222; }}
h1 {{ font-size: 22px; }} h2 {{ font-size: 17px; margin-top: 28px; border-bottom: 2px solid #4a82ff; padding-bottom: 4px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
td, th {{ border: 1px solid #ddd; padding: 6px 10px; text-align: right; }}
td:first-child, th:first-child {{ text-align: left; }}
th {{ background: #f4f6f8; }}
img {{ max-width: 100%; border: 1px solid #eee; margin-bottom: 16px; }}
.ok {{ color: #1a7f37; font-weight: 600; }} .bad {{ color: #cf222e; font-weight: 600; }}
pre {{ background: #f6f8fa; padding: 10px; overflow-x: auto; font-size: 13px; }}
</style></head><body>
<h1>📊 Отчёт тестового стенда — {esc(f['time'])}</h1>
<h2>Статус</h2>
<p class="{'ok' if f['health'].startswith('все') else 'bad'}">{esc(f['health'])}</p>
<p>Ошибок в логах за сутки: <b>{f['errors_today']}</b> · Инцидентов за 24 ч: <b>{len(f['incidents_24h'])}</b> ·
CPU busy за период: <b>{_n(sm.get('cpu_busy_pct_за_период'))}%</b> · Аптайм: <b>{_n(sm.get('uptime_h'))} ч</b></p>
<p>За 7 дней: инцидентов <b>{esc(f['reliability_7d']['инцидентов_за_неделю'])}</b> ·
MTTD ≤ <b>{esc(f['reliability_7d']['MTTD_сек_не_хуже'])} с</b> ·
MTTR средний <b>{_n(f['reliability_7d']['MTTR_сек_средний'])} с</b> ·
MTTR макс <b>{_n(f['reliability_7d']['MTTR_сек_макс'])} с</b> ·
MTTRC (гипотеза) <b>{_n(f['reliability_7d'].get('MTTRC_сек_средний'))} с</b> ·
MTTRC (подтверждённая, n={esc(f['reliability_7d'].get('MTTRC_подтв_выборка') or 0)}) <b>{_n(f['reliability_7d'].get('MTTRC_подтв_сек_средний'))} с</b></p>
<h2>Метрики системы и приложений за 24 ч</h2>
<table><tr><th>Метрика</th><th>Сейчас</th><th>Min</th><th>Avg</th><th>Max</th></tr>{rows}</table>
<h2>Метрики приложения man_reporting (динамика за сутки)</h2>
<table><tr><th>Метрика</th><th>24 ч назад</th><th>Сейчас</th><th>Δ</th></tr>{app_rows or '<tr><td colspan=4>нет данных</td></tr>'}</table>
<h2>Дашборды Grafana</h2>
{imgs or '<p>Дашборды Grafana недоступны.</p>'}
<h2>Инциденты за 24 ч</h2>
<table><tr><th>Время</th><th>Тип</th><th>Описание</th></tr>{inc_rows or '<tr><td colspan=3>инцидентов не было</td></tr>'}</table>
<h2>Настройки прогонов отчётов</h2>
<pre>{esc(f.get('reports', ''))}</pre>
</body></html>"""
    out = BASE / "report.html"
    out.write_text(html, encoding="utf-8")
    return out

def daily_report(chat_id=None, full_llm=True):
    f = collect_facts()
    sent = False
    if LLM_API_KEY and full_llm:
        text = llm(SYS_ANALYST + " Составь ежедневный отчёт по фактам: структура — статус, инциденты, "
                   "ошибки, прогоны отчётов, ресурсы (метрики за 24 ч: now/min/avg/max и как менялись), "
                   "рекомендации. До 20 предложений.",
                   json.dumps(f, ensure_ascii=False, default=str), max_tokens=1200)
        if text:
            tg_send(f"📊 Ежедневный отчёт {f['time']}\n\n{text}", chat_id)
            sent = True
    if not sent:
        sm = f.get("sys_metrics_24h") or {}
        def _fmt(s, u=""):
            return f"{s['now']}{u} (min {s['min']} / avg {s['avg']} / max {s['max']})" if s else "?"
        slo = f.get("slo") or {}
        slo_line = (f"SLO: доступность {slo['доступность_30д_%']}% (цель {slo['цель_доступности_%']}%), "
                    f"бюджет ошибок остался {slo['бюджет_осталось_%']}%\n" if slo else "")
        aq = f.get("качество_алертов") or {}
        aq_line = (f"Качество алертов: {aq['actionable_%']}% actionable "
                   f"(good {aq['good']} / bad {aq['bad']})\n" if aq.get("actionable_%") is not None else "")
        cal = f.get("slo_месяц_календарный") or {}
        cal_line = (f"Календарный итог {cal['месяц']}: доступность {cal['доступность_%']}% "
                    f"({cal['проб']} проб)\n" if cal else "")
        tg_send(f"📊 Ежедневный отчёт {f['time']}\nСтатус: {f['health']}\n"
                f"Ошибок в логах за сутки: {f['errors_today']}\n"
                f"Инцидентов за 24 ч: {len(f['incidents_24h'])}\n" + slo_line + aq_line + cal_line +
                f"Диск: {_fmt(sm.get('disk_pct'), '%')}\nПамять: {_fmt(sm.get('mem_pct'), '%')}\n"
                f"Load(1м): {_fmt(sm.get('load1'))}\n"
                f"БД man_reporting: {_fmt(sm.get('pg_db_mb'), ' МБ')}", chat_id)
    msg = metrics_message()
    if msg:
        tg_send(msg, chat_id, parse_mode="HTML")
    if send_dashboards(chat_id) == 0:
        tg_send("❌ Дашборды Grafana недоступны.", chat_id)
    try:
        doc = build_html_report()
        archive = BASE / "reports"
        archive.mkdir(exist_ok=True)
        (archive / f"{datetime.now(MSK):%Y-%m-%d}.html").write_bytes(doc.read_bytes())
        tg_send_document(doc, "Полный HTML-отчёт по метрикам", chat_id)
    except Exception as e:
        print(f"[html report] {e}", flush=True)

# ---------------------------------------------------------------- postmortem
def _postmortem_window(up_incident):
    """Окно инцидента: start/end, длительность, связанные записи и метрики ±30 мин."""
    m = re.search(r"простой (\d+) с", up_incident["text"])
    dur = int(m.group(1)) if m else 0
    end = datetime.fromisoformat(up_incident["ts"])
    start = end - timedelta(seconds=dur)
    lo = (start - timedelta(minutes=30)).isoformat(timespec="seconds")
    hi = (end + timedelta(minutes=30)).isoformat(timespec="seconds")
    related = [i for i in STATE["incidents"] if lo <= i["ts"] <= hi]
    hist = [h for h in STATE.get("metrics_history", []) if lo <= h.get("ts", "") <= hi]
    return start, end, dur, related, hist

def build_postmortem(up_incident, chat_id=None):
    """HTML post-mortem по инциденту простоя: хронология + метрики + LLM-разбор."""
    start, end, dur, related, hist = _postmortem_window(up_incident)
    comp = up_incident["text"].split(" восстановлен")[0]

    def wagg(get):
        vals = [v for h in hist if isinstance((v := get(h)), (int, float))]
        return (round(min(vals), 1), round(max(vals), 1)) if vals else None
    win = {
        "Память, %": wagg(lambda h: h.get("mem_pct")),
        "Load average 1м": wagg(lambda h: h.get("load1")),
        "RSS man_reporting, МБ": wagg(lambda h: (h.get("man_rep") or {}).get("rss_mb")),
        "RSS cntsadmin, МБ": wagg(lambda h: (h.get("cntsadmin") or {}).get("rss_mb")),
        "Соединения PostgreSQL": wagg(lambda h: h.get("pg_conns")),
    }
    facts = {
        "компонент": comp,
        "начало": start.strftime("%Y-%m-%d %H:%M:%S MSK"),
        "конец": end.strftime("%Y-%m-%d %H:%M:%S MSK"),
        "простой_сек": dur,
        "хронология": [f"{i['ts']} [{i['kind']}] {i['text'][:200]}" for i in related],
        "метрики_за_окно_min_max": {k: v for k, v in win.items() if v},
    }
    analysis = llm(SYS_ANALYST + " Составь post-mortem инцидента: хронология, влияние, "
                   "вероятная первопричина, что сработало хорошо, action items (до 5). "
                   "Опирайся только на присланные факты, не выдумывай.",
                   json.dumps(facts, ensure_ascii=False, default=str), max_tokens=1000)
    tl_rows = "".join(
        f"<tr><td>{esc(i['ts'])}</td><td>{esc(i['kind'])}</td>"
        f"<td style='text-align:left'>{esc(i['text'][:200])}</td></tr>" for i in related)
    win_rows = "".join(f"<tr><td>{esc(k)}</td><td>{_n(v[0])}</td><td>{_n(v[1])}</td></tr>"
                       for k, v in win.items() if v)
    html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<title>Post-mortem — {esc(comp)}, {esc(start.strftime('%Y-%m-%d %H:%M'))}</title>
<style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; max-width: 960px; margin: 24px auto; padding: 0 16px; color: #222; }}
h1 {{ font-size: 22px; }} h2 {{ font-size: 17px; margin-top: 28px; border-bottom: 2px solid #e07b39; padding-bottom: 4px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
td, th {{ border: 1px solid #ddd; padding: 6px 10px; text-align: right; }}
td:first-child, th:first-child {{ text-align: left; }}
th {{ background: #f4f6f8; }}
.meta {{ color: #555; }} pre {{ background: #f6f8fa; padding: 12px; white-space: pre-wrap; font-size: 14px; }}
</style></head><body>
<h1>🔍 Post-mortem: {esc(comp)}</h1>
<p class="meta">Начало: <b>{esc(facts['начало'])}</b> · Восстановление: <b>{esc(facts['конец'])}</b> ·
Простой: <b>{dur} с</b> · Записей в хронологии: <b>{len(related)}</b></p>
<h2>Анализ</h2>
<pre>{esc(analysis or 'LLM недоступен — анализ не сформирован. См. хронологию и метрики ниже.')}</pre>
<h2>Метрики за окно инцидента (±30 мин)</h2>
<table><tr><th>Метрика</th><th>Min</th><th>Max</th></tr>{win_rows or '<tr><td colspan=3>нет данных</td></tr>'}</table>
<h2>Хронология (журнал инцидентов)</h2>
<table><tr><th>Время</th><th>Тип</th><th>Описание</th></tr>{tl_rows}</table>
<p class="meta">Сформировано агентом мониторинга {esc(datetime.now(MSK).strftime('%Y-%m-%d %H:%M MSK'))}</p>
</body></html>"""
    archive = BASE / "postmortems"
    archive.mkdir(exist_ok=True)
    safe = re.sub(r"[^\w.=-]+", "_", comp)[:40]
    out = archive / f"{end.strftime('%Y-%m-%d_%H-%M')}_{safe}.html"
    out.write_text(html, encoding="utf-8")
    marked = _mark_rc_confirmed(up_incident, start, end)
    tg_send_document(out, f"Post-mortem: {comp}, простой {dur} с", chat_id)
    if marked:
        tg_send(f"☑️ Первопричина подтверждена post-mortem'ом — MTTRC зафиксирован "
                f"для {marked} инцидент(а/ов).", chat_id)
    return out

def _mark_rc_confirmed(up_incident, start, end):
    """MTTRC по подтверждённой причине [64]: проставляет rc_confirmed_ts на
    down-инцидент(ы) того же компонента в окне простоя. Первое подтверждение
    выигрывает — повторный /postmortem метку не двигает."""
    now_iso = datetime.now(MSK).isoformat(timespec="seconds")
    comp_key = up_incident["text"].split(" восстановлен")[0]
    lo = (start - timedelta(minutes=5)).isoformat(timespec="seconds")
    hi = end.isoformat(timespec="seconds")
    marked = 0
    for i in STATE["incidents"]:
        if i["kind"] != "down" or i.get("rc_confirmed_ts") or not (lo <= i["ts"] <= hi):
            continue
        key = i["text"].split("->")[0].split(":")[0].strip()
        if key == comp_key:
            i["rc_confirmed_ts"] = now_iso
            marked += 1
    if marked:
        save_state()
    return marked

def maybe_postmortem(up_incident, dur):
    """Автогенерация post-mortem при простое >= POSTMORTEM_MIN_DOWNTIME."""
    try:
        build_postmortem(up_incident)
    except Exception as e:
        print(f"[postmortem] {e}", flush=True)
        tg_send(f"❌ Не удалось собрать post-mortem: {e}")

def _postmortem_cmd(up_incident):
    try:
        build_postmortem(up_incident)
    except Exception as e:
        print(f"[postmortem cmd] {e}", flush=True)
        tg_send(f"❌ Не удалось собрать post-mortem: {e}")
