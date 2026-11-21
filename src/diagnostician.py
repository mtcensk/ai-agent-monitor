#!/usr/bin/env python3
"""diagnostician — роль «диагностик»: оценка наблюдений, решений не исполняет.

LLM-разбор ошибок и инцидентов, RAG по журналу инцидентов, детекция
аномалий по базовой линии, расчёт SLO/SLI и burn rate, разбор логов.
Действия не выполняет — в худшем случае публикует алерт через tg_send
или (у плейбуков) задачу в bus.
"""
import json
import re
import time
from datetime import datetime, timedelta

from common import (STATE, MSK, LLM_API_KEY, LLM_BASE_URL, LLM_MODEL,
                    SLO_TARGET, SLO_LAT_MS, SLO_BURN_FAST, SLO_BURN_SLOW, SLO_BURN_LONG,
                    SLO_ALERT_THROTTLE, tg_send, alert, add_incident, http_json)
from observer import LAST_PROBES

# ---------------------------------------------------------------- llm
def mask_secrets(text):
    """Маскирует пароли/токены и ПДн (практика [41]) перед отправкой в LLM."""
    text = re.sub(r"(?i)((?:password|passwd|pwd|secret|token|api[_-]?key|пароль)\s*[=:]\s*)[^\s;\"'&,]+",
                  r"\1***", text)
    text = re.sub(r"://([^:/\s]+):([^@/\s]+)@", r"://\1:***@", text)  # креды в URL
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "***@***", text)  # email
    text = re.sub(r"(?<!\d)(?:\+7|8)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}(?!\d)",
                  "+7***", text)                                       # телефоны РФ
    text = re.sub(r"(?<!\d)(?:\d[ -]*?){13,16}(?!\d)", "****CARD****", text)  # номера карт
    return text

def llm(system, user, max_tokens=800):
    if not LLM_API_KEY:
        return None
    try:
        res = http_json(f"{LLM_BASE_URL}/chat/completions",
                        {"model": LLM_MODEL,
                         "messages": [{"role": "system", "content": system},
                                      {"role": "user", "content": mask_secrets(user)}],
                         "max_tokens": max_tokens, "temperature": 0.2},
                        headers={"Authorization": f"Bearer {LLM_API_KEY}",
                                 # OpenCode Go требует стабильный id сессии для маршрутизации
                                 "x-opencode-session": "ai-monitor-vds-3308876"},
                        timeout=60)
        STATE["self_stats"]["llm_last_ok"] = time.time()
        return res["choices"][0]["message"]["content"].strip()
    except Exception as e:
        STATE["self_stats"]["llm_fails"] = STATE["self_stats"].get("llm_fails", 0) + 1
        print(f"[llm error] {e}", flush=True)
        return None

SYS_ANALYST = ("Ты — дежурный SRE-аналитик тестового стенда (два .NET-приложения: админка CntsAdminCon "
               "и служба отчётов man_reporting, PostgreSQL). Отвечай кратко по-русски, по делу: "
               "что произошло, насколько критично (низкая/средняя/высокая), что проверить или сделать. "
               "До 6 предложений, без markdown-заголовков.")

# ---------------------------------------------------------------- RAG по инцидентам
def similar_incidents(text, limit=3):
    """RAG: похожие прошлые инциденты по пересечению значимых слов."""
    def words(s):
        return set(re.findall(r"[A-Za-zА-Яа-я_]{4,}", s.lower()))
    q = words(text)
    scored = []
    for i in STATE["incidents"][-200:]:
        score = len(q & words(i["text"]))
        if score >= 2:
            scored.append((score, i))
    scored.sort(key=lambda x: -x[0])
    return [i for _, i in scored[:limit]]

def rag_context(text):
    sim = similar_incidents(text)
    if not sim:
        return ""
    lines = "\n".join(f"- {i['ts'][:16]} [{i['kind']}]: {i['text'][:150]}" for i in sim)
    return f"\n\nПохожие прошлые инциденты (учитывай в анализе):\n{lines}"

# ---------------------------------------------------------------- SLO / SLI
def _slo_window(hs):
    hours = STATE.get("slo_hourly") or {}
    lo = (datetime.now(MSK) - timedelta(hours=hs)).strftime("%Y-%m-%dT%H")
    keys = [k for k in hours if k >= lo]
    ok = sum(hours[k]["ok"] for k in keys)
    tot = sum(hours[k]["total"] for k in keys)
    lat = sorted(x for k in keys for x in hours[k]["lat"])
    return ok, tot, lat

def _burn_hourly(hs):
    """Burn rate по часовым корзинам за hs часов. None, если данных мало."""
    ok, tot, _ = _slo_window(hs)
    if tot < 5:
        return None
    allowed = (100 - SLO_TARGET) / 100
    return round((tot - ok) / tot / allowed, 1)

def _burn_cycles(minutes):
    """Burn rate по сырым циклам за minutes минут (короткие окна-подтверждения)."""
    cut = time.time() - minutes * 60
    cy = [c for c in STATE.get("slo_cycles", []) if c["ts"] >= cut]
    if len(cy) < 3:
        return None
    allowed = (100 - SLO_TARGET) / 100
    return round(sum(1 for c in cy if not c["ok"]) / len(cy) / allowed, 1)

def slo_report():
    """SLI и error budget за 30 дней + burn rate за 1 ч и 6 ч. None, если данных мало."""
    ok30, tot30, lat30 = _slo_window(24 * 30)
    if tot30 < 10:
        return None
    allowed = (100 - SLO_TARGET) / 100
    budget_total = allowed * tot30
    spent = tot30 - ok30
    return {
        "цель_доступности_%": SLO_TARGET,
        "доступность_30д_%": round(100 * ok30 / tot30, 3),
        "проб_за_30д": tot30,
        "латентность_p95_мс": lat30[int(len(lat30) * 0.95)] if lat30 else None,
        "sli_латентности_%": round(100 * sum(1 for x in lat30 if x <= SLO_LAT_MS) / len(lat30), 2) if lat30 else None,
        "бюджет_ошибок_всего_проб": int(budget_total),
        "бюджет_ошибок_потрачено": spent,
        "бюджет_осталось_%": round(max(0.0, 100 * (1 - spent / budget_total)), 1) if budget_total else None,
        "burn_rate_1ч": _burn_hourly(1),
        "burn_rate_6ч": _burn_hourly(6),
        "burn_rate_3д": _burn_hourly(72),
    }

def slo_burn_check():
    """Multi-window multi-burn-rate алерты (практика [49]).

    fast: burn(1ч) ≥ порога И burn(5м) ≥ порога — короткое окно подтверждает,
    что всплеск продолжается; slow: burn(6ч) + burn(30м); long: burn(3д) ≥ 1 —
    бюджет расходуется быстрее допустимого даже без острых всплесков.
    Троттлинг 4 ч на тип алерта.
    """
    r = slo_report()
    if not r:
        return
    now = time.time()
    checks = (
        ("fast", r["burn_rate_1ч"], _burn_cycles(5), SLO_BURN_FAST, "1 ч + 5 мин"),
        ("slow", r["burn_rate_6ч"], _burn_cycles(30), SLO_BURN_SLOW, "6 ч + 30 мин"),
        ("long", r["burn_rate_3д"], None, SLO_BURN_LONG, "3 дня"),
    )
    for kind, value, confirm, limit, window in checks:
        if value is None or value < limit:
            continue
        if confirm is not None and confirm < limit:   # нет подтверждения коротким окном — не алертим
            continue
        if now - STATE["slo_alert_ts"].get(kind, 0) < SLO_ALERT_THROTTLE:
            continue
        STATE["slo_alert_ts"][kind] = now
        confirm_txt = f", подтверждение {confirm}" if confirm is not None else ""
        analysis = llm(SYS_ANALYST,
                       f"Burn rate ({kind}, окна {window}) = {value}{confirm_txt} (порог {limit}). "
                       f"Доступность 30д: {r['доступность_30д_%']}% при цели {r['цель_доступности_%']}%, "
                       f"остаток бюджета ошибок: {r['бюджет_осталось_%']}%. "
                       f"Что это значит и что проверить?" + rag_context("burn rate доступность")) \
            if LLM_API_KEY else None
        # fast — будящий page; slow/long — ticket ([48])
        alert(f"🟠 SLO BURN RATE ({kind}) {datetime.now(MSK):%H:%M:%S}\n"
              f"burn rate = {value}{confirm_txt} при пороге {limit} (окна {window})\n"
              f"Доступность 30д: {r['доступность_30д_%']}% (цель {r['цель_доступности_%']}%)\n"
              f"Бюджет ошибок: осталось {r['бюджет_осталось_%']}% "
              f"(потрачено {r['бюджет_ошибок_потрачено']} из {r['бюджет_ошибок_всего_проб']} проб)" +
              (f"\n\nАнализ: {analysis}" if analysis else ""), ticket=(kind != "fast"))

# ---------------------------------------------------------------- error budget policy
BUDGET_CAUTION_PCT = 50    # остаток бюджета < 50% — осторожность
BUDGET_FREEZE_PCT = 20     # остаток < 20% — заморозка (практика [25])

def budget_policy():
    """Статус error budget policy: norm | caution | freeze. None, если данных мало."""
    r = slo_report()
    if not r or r["бюджет_осталось_%"] is None:
        return None
    left = r["бюджет_осталось_%"]
    status = "freeze" if left < BUDGET_FREEZE_PCT else "caution" if left < BUDGET_CAUTION_PCT else "norm"
    return {"status": status, "осталось_%": left, "доступность_30д_%": r["доступность_30д_%"]}

def budget_policy_check():
    """Реакции на переход статуса бюджета (вызывается из monitor_loop).

    freeze  — авто-понижение автономии enforce -> dryrun: в кризис автоматика
              не должна вносить изменения без человека;
    caution — предупреждение;
    norm    — уведомление о восстановлении (автономию НЕ возвращаем —
              это осознанное решение человека через /autonomy).
    """
    p = budget_policy()
    if not p:
        return
    prev = STATE.get("budget_status", "")
    if p["status"] == prev:
        return
    STATE["budget_status"] = p["status"]
    if p["status"] == "freeze":
        extra = ""
        if STATE.get("autonomy") == "enforce":
            STATE["autonomy"] = "dryrun"
            extra = "\n⚙️ Автономия понижена: enforce → dryrun — плейбуки только уведомляют."
        alert(f"🔴 ERROR BUDGET: осталось {p['осталось_%']}% (< {BUDGET_FREEZE_PCT}%)\n"
              f"Доступность 30д: {p['доступность_30д_%']}%. Режим «заморозки»: "
              f"только стабилизация, никаких экспериментов со стендом." + extra)
    elif p["status"] == "caution":
        alert(f"🟠 ERROR BUDGET: осталось {p['осталось_%']}% (< {BUDGET_CAUTION_PCT}%)\n"
              f"Доступность 30д: {p['доступность_30д_%']}%. Осторожность: "
              f"избегайте рискованных работ на стенде до восстановления бюджета.", ticket=True)
    elif prev:
        alert(f"🟢 ERROR BUDGET восстановился: {p['осталось_%']}%\n"
              f"Доступность 30д: {p['доступность_30д_%']}%. "
              f"Вернуть автономию: /autonomy enforce", ticket=True)

# ---------------------------------------------------------------- RED зависимостей
DEP_RULES = [
    ("latency_ms", 500, "Латентность SELECT 1 к PostgreSQL, мс"),
    ("longest_active_s", 300, "Самый длинный активный запрос, с"),
    ("conns_pct", 80, "Использование max_connections, %"),
    ("lock_waits", 1, "Сессий в ожидании блокировки"),
]
DEP_ALERT_THROTTLE = 14400                 # 4 ч на метрику

def dependency_check(m):
    """Warning-пороги по RED-метрикам зависимости (практика [62]): список нарушений."""
    dep = m.get("pg_dep") or {}
    ts_map = STATE["dep_alert_ts"]
    out = []
    for key, limit, label in DEP_RULES:
        v = dep.get(key)
        if not isinstance(v, (int, float)) or v < limit:
            continue
        if time.time() - ts_map.get(key, 0) < DEP_ALERT_THROTTLE:
            continue
        ts_map[key] = time.time()
        out.append(f"• {label}: {v:g} (порог {limit})")
    return out

# ---------------------------------------------------------------- прогноз по тренду
FORECAST_RULES = [
    ("disk", lambda h: h.get("disk_pct"), 85, "Диск /, %"),
    ("man_rss", lambda h: (h.get("man_rep") or {}).get("rss_mb"), 1500, "RSS man_reporting, МБ"),
    ("cnts_rss", lambda h: (h.get("cntsadmin") or {}).get("rss_mb"), 1500, "RSS cntsadmin, МБ"),
]
FORECAST_ALERT_THROTTLE = 43200            # 12 ч на метрику

def forecast_check(hist):
    """Линейный тренд за ~сутки: предупреждение, если порог пересечётся в ближайшие 24 ч ([37]).

    Медиана/линейная регрессия — базовый уровень прогноза; Holt-Winters и ML
    оставлены для более сложных паттернов, как рекомендует книга.
    """
    out = []
    window = hist[-289:]
    if len(window) < 30:
        return out
    ts_map = STATE["forecast_alert_ts"]

    def parse_ts(h):
        return datetime.fromisoformat(h["ts"]).timestamp()

    for name, get, limit, label in FORECAST_RULES:
        pts = [(parse_ts(h), get(h)) for h in window]
        pts = [(t, v) for t, v in pts if isinstance(v, (int, float))]
        if len(pts) < 30:
            continue
        t0 = pts[0][0]
        xs = [t - t0 for t, _ in pts]
        ys = [v for _, v in pts]
        n = len(xs)
        sx, sy = sum(xs), sum(ys)
        sxx = sum(x * x for x in xs)
        sxy = sum(x * y for x, y in zip(xs, ys))
        denom = n * sxx - sx * sx
        if denom == 0:
            continue
        slope = (n * sxy - sx * sy) / denom     # единиц в секунду
        if slope <= 0:
            continue
        cur = ys[-1]
        if cur >= limit:
            continue                            # уже за порогом — чужой алерт
        hours = (limit - cur) / slope / 3600
        if hours <= 24 and time.time() - ts_map.get(name, 0) > FORECAST_ALERT_THROTTLE:
            ts_map[name] = time.time()
            out.append(f"• {label}: тренд +{slope * 3600:.2f}/ч, сейчас {cur:g}, "
                       f"порог {limit} — пересечение через ~{hours:.1f} ч")
    return out

# ---------------------------------------------------------------- «скучные» ресурсы ОС
INFRA_ALERT_THROTTLE = 21600               # 6 ч на проверку

def infra_check(m):
    """Контроль «скучных» протоколов и ресурсов ([134]/[147]): inode, NTP, DNS, бэкапы."""
    out = []
    ts_map = STATE["infra_alert_ts"]
    now = time.time()

    def throttled(key, text):
        if now - ts_map.get(key, 0) > INFRA_ALERT_THROTTLE:
            ts_map[key] = now
            out.append(text)

    v = m.get("inode_pct")
    if isinstance(v, (int, float)) and v >= 85:
        throttled("inode", f"• Inode на / заполнены на {v}% (порог 85%)")
    if m.get("ntp_synced") is False:
        throttled("ntp", "• NTP не синхронизирован (timedatectl) — риск рассинхрона метрик и логов")
    if m.get("dns_ok") is False:
        throttled("dns", "• DNS-резолв не работает — внешние интеграции (Telegram, LLM) умрут")
    b = m.get("backup_age_h")
    if b is None:
        throttled("backup", "• Бэкапы БД не найдены (/app/backups пуст или нет каталога)")
    elif b > 30:
        throttled("backup", f"• Последний бэкап БД старше 30 ч ({b:g} ч) — проверить cron")
    g = m.get("data_gen_age_h")
    if g is None:
        throttled("datagen", "• Генератор тестовых данных не запускался (нет /app/data_gen/state.json)")
    elif g > 30:
        throttled("datagen", f"• Генератор тестовых данных не запускался более 30 ч ({g:g} ч) — проверить cron")
    e = m.get("data_gen_errors")
    if isinstance(e, int) and e > 0:
        throttled("datagen_err", f"• Генератор тестовых данных: {e} ошибок в последнем запуске — см. /app/data_gen/state.json")
    return out

# ---------------------------------------------------------------- аномалии по базовой линии
BASELINE_KEYS = [
    ("mem_pct", lambda h: h.get("mem_pct"), "Память, %"),
    ("man_rss", lambda h: (h.get("man_rep") or {}).get("rss_mb"), "RSS man_reporting, МБ"),
    ("cnts_rss", lambda h: (h.get("cntsadmin") or {}).get("rss_mb"), "RSS cntsadmin, МБ"),
    ("load1", lambda h: h.get("load1"), "Load average 1м"),
    ("pg_conns", lambda h: h.get("pg_conns"), "Соединения PostgreSQL"),
]

def baseline_check(hist):
    """Аномалии: текущее значение > mean + 2σ по истории ~суток. Троттлинг 4 ч на метрику."""
    window = hist[-289:]                 # ~сутки при 5-мин интервале
    if len(window) < 20:                 # минимум ~100 мин наблюдений
        return []
    cur, base = window[-1], window[:-1]
    ts_map = STATE["baseline_alert_ts"]
    out = []
    for name, get, label in BASELINE_KEYS:
        vals = [v for h in base if isinstance((v := get(h)), (int, float))]
        v = get(cur)
        if len(vals) < 10 or not isinstance(v, (int, float)):
            continue
        mean = sum(vals) / len(vals)
        std = (sum((x - mean) ** 2 for x in vals) / len(vals)) ** 0.5
        if std < 1e-9:
            continue
        if v > mean + 2 * std and v > mean * 1.3 and time.time() - ts_map.get(name, 0) > 14400:
            ts_map[name] = time.time()
            out.append(f"• {label}: {v:g} при норме {mean:g} ± {std:g} (>2σ)")
    return out

# ---------------------------------------------------------------- разбор логов
LOG_RE = re.compile(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2} : (\w+) : ")

def process_log_chunk(app, chunk):
    lines = chunk.splitlines()
    i = 0
    while i < len(lines):
        m = LOG_RE.match(lines[i])
        if m and m.group(1) == "ERROR":
            block = [lines[i]]
            j = i + 1
            while j < len(lines) and not LOG_RE.match(lines[j]):
                block.append(lines[j])
                j += 1
            on_error(app, "\n".join(block)[:2000])
            i = j
        else:
            i += 1

def on_error(app, text):
    today = datetime.now(MSK).strftime("%Y-%m-%d")
    STATE["error_counts"][today] = STATE["error_counts"].get(today, 0) + 1
    add_incident("error", f"[{app}] {text.splitlines()[0]}")
    sig = re.sub(r"\d+", "N", text.splitlines()[0])[:120]      # сигнатура без чисел
    last = STATE["dedupe"].get(sig, 0)
    if time.time() - last < 1800:                              # не чаще раза в 30 мин на сигнатуру
        return
    STATE["dedupe"][sig] = time.time()
    analysis = llm(SYS_ANALYST,
                   f"Ошибка в логе приложения {app}:\n{text}" + rag_context(text)) if LLM_API_KEY else None
    alert(f"⚠️ ERROR в {app} {datetime.now(MSK):%H:%M:%S}\n```\n{text[:600]}\n```" +
          (f"\nАнализ: {analysis}" if analysis else ""), ticket=True)
