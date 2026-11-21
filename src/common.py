#!/usr/bin/env python3
"""common — общая инфраструктура агента: конфигурация, состояние, HTTP, Telegram-транспорт.

Конфигурация — переменные окружения (app.env):
  TELEGRAM_TOKEN  - токен бота
  LLM_BASE_URL    - например https://api.openai.com/v1
  LLM_API_KEY     - ключ
  LLM_MODEL       - модель (по умолчанию gpt-4o-mini)
  REPORT_HOUR     - час ежедневного отчёта по МСК (по умолчанию 9)
  AUTONOMY        - режим плейбуков: off | dryrun (по умолчанию) | enforce
  POSTMORTEM_MIN_DOWNTIME - минимальный простой (сек) для авто-post-mortem (по умолчанию 300)
  SLO_TARGET      - цель доступности в % за 30 дней (по умолчанию 99.5)
  SLO_LAT_MS      - порог латентности пробы для SLI, мс (по умолчанию 2000)
  SLO_BURN_FAST / SLO_BURN_SLOW - пороги burn rate за 1 ч / 6 ч (по умолчанию 6 / 2)
  TEMPO_URL       - API Grafana Tempo для трейсов (по умолчанию http://127.0.0.1:3200)

Модули-роли: observer (наблюдение), diagnostician (оценка и LLM),
executor (действия по задачам из bus), reporter (отчёты и доставка).
Обмен между ролями — bus.py (типизированные задачи в STATE) и общий STATE.
"""
import json
import os
import re
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
STATE_FILE = BASE / "state.json"
LOGS = {
    "man_reporting": Path("/app/man_reporting/logs/app.log"),
    "cntsadmin": Path("/app/cntsadmin/logs/app.log"),
}
MSK = timezone(timedelta(hours=3))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
# Белый список Telegram user id, которым бот отвечает и выполняет команды.
# Пусто = fallback: только владелец зарегистрированного chat_id; если и его нет —
# первичное подключение открыто (первый /start регистрирует владельца).
TG_ALLOWED_USERS = {int(x) for x in re.split(r"[,\s]+", os.environ.get("TG_ALLOWED_USERS", "").strip()) if x.lstrip("-").isdigit()}
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "9"))
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000").rstrip("/")
GRAFANA_TOKEN = os.environ.get("GRAFANA_TOKEN", "")
TEMPO_URL = os.environ.get("TEMPO_URL", "http://127.0.0.1:3200").rstrip("/")
GRAFANA_DASHBOARDS = [
    ("stand-overview", "stend-e28094-obzor", "Обзор стенда"),
    ("stand-sys", "stend-e28094-sistema", "Системные метрики"),
    ("stand-app", "stend-e28094-prilozhenija", "Прикладные метрики"),
]
RECOVERY_STABLE_SEC = int(os.environ.get("RECOVERY_STABLE_SEC", "120"))  # анти-флаппинг
METRICS_INTERVAL = int(os.environ.get("METRICS_INTERVAL", "300"))        # сек между снимками
DISK_ALERT_PCT = float(os.environ.get("DISK_ALERT_PCT", "85"))
SLO_TARGET = float(os.environ.get("SLO_TARGET", "99.5"))     # цель доступности, % за 30 дней
SLO_LAT_MS = int(os.environ.get("SLO_LAT_MS", "2000"))       # латентность пробы в пределах SLI, мс
SLO_BURN_FAST = float(os.environ.get("SLO_BURN_FAST", "6"))  # burn rate за 1 ч
SLO_BURN_SLOW = float(os.environ.get("SLO_BURN_SLOW", "2"))  # burn rate за 6 ч
SLO_BURN_LONG = float(os.environ.get("SLO_BURN_LONG", "1"))  # burn rate за 3 дня (ticket-алерт)
SLO_ALERT_THROTTLE = 14400                                   # 4 ч на тип алерта
POSTMORTEM_MIN_DOWNTIME = int(os.environ.get("POSTMORTEM_MIN_DOWNTIME", "300"))  # сек простоя
PLAYBOOK_COOLDOWN = int(os.environ.get("PLAYBOOK_COOLDOWN", "3600"))   # 1 ч на плейбук
PLAYBOOK_DAILY_LIMIT = int(os.environ.get("PLAYBOOK_DAILY_LIMIT", "3"))
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090").rstrip("/")
REPEAT_ALERT_SEC = int(os.environ.get("REPEAT_ALERT_SEC", "900"))      # re-alert по открытому инциденту
ACK_MUTE_SEC = int(os.environ.get("ACK_MUTE_SEC", "14400"))            # /ack глушит повторы на 4 ч
TG_FAIL_ALERT_STREAK = 3            # столько сбоев tg подряд -> резервный канал
TG_FAIL_EMAIL_THROTTLE = 3600       # не чаще раза в час на резервный канал
# Резервный канал (email) — опционально; без SMTP_* только журналируется
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
MAIL_TO = os.environ.get("MAIL_TO", "")

# ---------------------------------------------------------------- state
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}
STATE = load_state()
STATE.setdefault("chat_id", None)
STATE.setdefault("log_offsets", {})
STATE.setdefault("incidents", [])          # {ts, kind, text}
STATE.setdefault("error_counts", {})       # дата -> count
STATE.setdefault("dedupe", {})             # хэш ошибки -> ts последнего алерта
STATE.setdefault("tg_offset", 0)
STATE.setdefault("last_report_date", "")
STATE.setdefault("metrics_history", [])    # снимки метрик, ~2 суток
STATE.setdefault("disk_alert_ts", 0)
STATE.setdefault("baseline_alert_ts", {})  # метрика -> ts последнего алерта аномалии
STATE.setdefault("pending_action", None)   # действие, ожидающее /confirm
STATE.setdefault("autonomy", os.environ.get("AUTONOMY", "dryrun"))  # off | dryrun | enforce
STATE.setdefault("playbook_runs", {})      # плейбук -> [ts срабатываний за сутки]
STATE.setdefault("slo_hourly", {})         # "YYYY-MM-DDTHH" -> {ok, total, lat[мс]}
STATE.setdefault("slo_cycles", [])         # последние ~2000 циклов {ts, ok} — для sub-hour окон burn rate
STATE.setdefault("slo_alert_ts", {})       # тип burn-алерта -> ts последнего
STATE.setdefault("budget_status", "")      # error budget policy: norm | caution | freeze
STATE.setdefault("dep_alert_ts", {})       # метрика зависимости -> ts последнего warning-алерта
STATE.setdefault("forecast_alert_ts", {})  # метрика прогноза -> ts последнего алерта
STATE.setdefault("infra_alert_ts", {})     # инфра-проверка -> ts последнего алерта
STATE.setdefault("bus_tasks", [])          # очередь задач между ролями (см. bus.py)
STATE.setdefault("self_stats", {})         # самонаблюдение: сбои собственных каналов
STATE["self_stats"].setdefault("tg_fails", 0)        # всего сбоев отправки в Telegram
STATE["self_stats"].setdefault("tg_fail_streak", 0)  # сбоев подряд (сброс при успехе)
STATE["self_stats"].setdefault("tg_last_ok", 0)      # ts последней успешной отправки
STATE["self_stats"].setdefault("tg_email_ts", 0)     # ts последнего письма на резервный канал
STATE["self_stats"].setdefault("llm_fails", 0)       # всего сбоев LLM API
STATE["self_stats"].setdefault("llm_last_ok", 0)     # ts последнего успешного ответа LLM
STATE["self_stats"].setdefault("muted_alerts", 0)    # алертов, подавленных /mute
STATE.setdefault("acks", {})               # ключ инцидента -> ts, до которого заглушены повторы
STATE.setdefault("mute_until", 0)          # алерты заглушены до ts (silence, максимум 24 ч)
STATE.setdefault("ticket_chat_id", None)   # чат для P2/P3-алертов (подключается /start ticket)
STATE.setdefault("last_alert", None)       # последний отправленный алерт {ts, text} — для /fb
STATE.setdefault("alert_feedback", {"good": 0, "bad": 0, "log": []})  # качество алертов

STATE_LOCK = threading.Lock()

def save_state():
    with STATE_LOCK:                      # save_state зовут несколько потоков
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(STATE, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(STATE_FILE)

def add_incident(kind, text):
    STATE["incidents"].append({"ts": datetime.now(MSK).isoformat(timespec="seconds"),
                               "kind": kind, "text": text[:500]})
    STATE["incidents"] = STATE["incidents"][-500:]

# ---------------------------------------------------------------- http
def http_json(url, payload=None, headers=None, timeout=25, method=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"))
    # UA urllib блокируется некоторыми API-шлюзами (OpenCode Go -> 403)
    req.add_header("User-Agent", "ai-monitor/1.0 (+https://localhost)")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def http_text(url, timeout=8):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read().decode(errors="replace")

# ---------------------------------------------------------------- telegram (транспорт)
def send_email(subject, text):
    """Резервный канал: письмо через SMTP. False, если не настроен или ошибка."""
    if not (SMTP_HOST and MAIL_TO):
        return False
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(text, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = SMTP_USER or "ai-monitor@localhost"
        msg["To"] = MAIL_TO
        if SMTP_PORT == 465:
            srv = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20)
        else:
            srv = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20)
            srv.starttls()
        if SMTP_USER:
            srv.login(SMTP_USER, SMTP_PASS)
        srv.sendmail(msg["From"], [MAIL_TO], msg.as_string())
        srv.quit()
        return True
    except Exception as e:
        print(f"[email error] {e}", flush=True)
        return False

def _note_tg(ok, text=""):
    """Учёт сбоев Telegram-канала; при серии сбоев — резервный email (практика [50]/[80])."""
    st = STATE["self_stats"]
    if ok:
        if st.get("tg_fail_streak", 0) >= TG_FAIL_ALERT_STREAK:
            send_email("[ai-monitor] Telegram-канал восстановлен",
                       f"Отправка в Telegram снова работает ({datetime.now(MSK):%Y-%m-%d %H:%M MSK}).")
        st["tg_fail_streak"] = 0
        st["tg_last_ok"] = time.time()
        return
    st["tg_fails"] = st.get("tg_fails", 0) + 1
    st["tg_fail_streak"] = st.get("tg_fail_streak", 0) + 1
    if (st["tg_fail_streak"] >= TG_FAIL_ALERT_STREAK
            and time.time() - st.get("tg_email_ts", 0) > TG_FAIL_EMAIL_THROTTLE):
        st["tg_email_ts"] = time.time()
        send_email("[ai-monitor] Telegram-канал недоступен",
                   f"Подряд {st['tg_fail_streak']} сбоев отправки в Telegram.\n"
                   f"Последнее сообщение, которое не удалось доставить:\n\n{text[:1500]}")

def tg_send(text, chat_id=None, parse_mode=None):
    cid = chat_id or STATE.get("chat_id")
    if not (TELEGRAM_TOKEN and cid):
        print(f"[tg skip] {text[:100]}", flush=True)
        return False
    ok = True
    for chunk in [text[i:i + 3800] for i in range(0, len(text), 3800)] or [""]:
        sent = False
        for attempt in range(3):
            try:
                payload = {"chat_id": cid, "text": chunk, "disable_web_page_preview": True}
                if parse_mode:
                    payload["parse_mode"] = parse_mode
                http_json(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          payload, timeout=20)
                sent = True
                break
            except Exception as e:
                print(f"[tg error attempt {attempt + 1}] {e}", flush=True)
                time.sleep(3)
        if not sent:
            ok = False
    _note_tg(ok, text)
    return ok

def alert(text, chat_id=None, parse_mode=None, ticket=False):
    """Отправка АЛЕРТА (не ответа на команду): учитывает /mute и маршрутизацию.

    ticket=True — P2/P3-алерт: уходит в ticket-чат (если подключён командой
    /start ticket), иначе в основной. Page/ticket разведены по практике [48].
    Фиксирует last_alert для оценки качества через /fb.
    Подавленные mute'ом алерты считаются в self_stats.muted_alerts.
    """
    STATE["last_alert"] = {"ts": time.time(), "text": text[:200]}
    if STATE.get("mute_until", 0) > time.time():
        st = STATE["self_stats"]
        st["muted_alerts"] = st.get("muted_alerts", 0) + 1
        print(f"[muted] {text[:100]}", flush=True)
        return False
    if ticket and STATE.get("ticket_chat_id"):
        return tg_send(text, STATE["ticket_chat_id"], parse_mode)
    return tg_send(text, chat_id, parse_mode)

def esc(s):
    """Экранирование для HTML (Telegram parse_mode=HTML и html-отчёт)."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _tg_multipart(method, fields, file_field, path, mime):
    cid = STATE.get("chat_id")
    boundary = "----ai-monitor-boundary"
    data = Path(path).read_bytes()
    parts = []
    for name, value in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{file_field}"; '
                 f'filename="{Path(path).name}"\r\nContent-Type: {mime}\r\n\r\n'.encode() + data + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    body = b"".join(parts)
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}", data=body, method="POST")
            req.add_header("User-Agent", "ai-monitor/1.0 (+https://localhost)")
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            with urllib.request.urlopen(req, timeout=40) as r:
                r.read()
            return True
        except Exception as e:
            print(f"[tg {method} error attempt {attempt + 1}] {e}", flush=True)
            time.sleep(3)
    return False

def tg_send_document(path, caption="", chat_id=None):
    cid = chat_id or STATE.get("chat_id")
    if not (TELEGRAM_TOKEN and cid):
        return
    _tg_multipart("sendDocument", {"chat_id": str(cid), "caption": caption},
                  "document", path, "text/html")

def tg_send_photo(path, caption="", chat_id=None):
    """Отправка PNG через sendPhoto."""
    cid = chat_id or STATE.get("chat_id")
    if not (TELEGRAM_TOKEN and cid):
        return
    _tg_multipart("sendPhoto", {"chat_id": str(cid), "caption": caption},
                  "photo", path, "image/png")

# ---------------------------------------------------------------- мелкие утилиты
def _read_file(p):
    try:
        return Path(p).read_text()
    except Exception:
        return ""

def _n(x):
    return f"{x:g}" if isinstance(x, (int, float)) else "—"
