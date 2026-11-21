#!/usr/bin/env python3
"""executor — роль «исполнитель»: ЕДИНСТВЕННАЯ роль, меняющая стенд.

Забирает задачи "action" из шины (bus) и выполняет их (рестарты сервисов).
Плейбуки описаны декларативно в playbooks.json (версионируется, дата ревизии
обязательна — практики [53]/[124]); в режиме enforce они публикуют задачу
в шину, а executor_loop её выполняет — рестарты не блокируют metrics_loop.
Хаос-действия (учебные инциденты, [94]) — только по команде человека.
"""
import json
import subprocess
import time
from datetime import datetime, date

import bus
from common import (STATE, BASE, MSK, PLAYBOOK_COOLDOWN, PLAYBOOK_DAILY_LIMIT,
                    tg_send, alert, add_incident, save_state)

# ---------------------------------------------------------------- действия
ACTIONS = {
    "restart man_reporting": "/app/man_reporting/ctl.sh restart",
    "restart cntsadmin": "/app/cntsadmin/ctl.sh restart",
}

# Учебные инциденты (хаос-уровень 1, [122]): только вручную через /chaos + /confirm.
# cron-watchdog поднимет процесс примерно за минуту — заодно проверяется и он.
CHAOS_ACTIONS = {
    "chaos kill man_reporting": "pkill -f man_reporting_service",
    "chaos kill cntsadmin": "pkill -f CntsAdminCon",
}

def _run(action, audit_kind, audit_note, ok_verb="выполнен", fail_tpl=None):
    """Общее тело выполнения действия с аудитом."""
    cmdline = ACTIONS.get(action) or CHAOS_ACTIONS.get(action)
    if not cmdline:
        tg_send(f"Неизвестное действие: {action}")
        return
    try:
        out = subprocess.run(cmdline, shell=True, capture_output=True, text=True, timeout=120)
        result = (out.stdout + out.stderr).strip()
        add_incident(audit_kind, f"{action} (rc={out.returncode}) {audit_note}")
        save_state()
        alert(f"✅ {action} {ok_verb} (rc={out.returncode}):\n```\n{result[-800:] or 'ok'}\n```")
    except Exception as e:
        add_incident(audit_kind, f"{action} — ОШИБКА: {e}")
        save_state()
        alert((fail_tpl or "❌ {action} не выполнен: {e}").format(action=action, e=e))

def run_action(action):
    """L2-действие, подтверждённое человеком. Аудит — в лог и в инциденты."""
    print(f"[action] {action} — подтверждено через Telegram", flush=True)
    _run(action, "action", "по команде из Telegram")

def run_chaos(action):
    """Учебный инцидент по команде человека. Аудит — chaos ([95])."""
    print(f"[chaos] {action} — подтверждён через Telegram", flush=True)
    _run(action, "chaos", "— учебный инцидент (хаос-уровень 1)")

def run_action_auto(action, reason):
    """Автоматическое действие плейбука (режим enforce). Аудит — action-auto."""
    print(f"[playbook] {action} — автозапуск: {reason}", flush=True)
    _run(action, "action-auto", f"— playbook: {reason}",
         ok_verb="выполнен автоматически", fail_tpl="❌ Автодействие {action} не выполнено: {e}")

# ---------------------------------------------------------------- плейбуки: декларативные, из playbooks.json
def _rss_growth(app_key, min_mb, grow_snapshots, grow_mb):
    """Условие: RSS > min_mb и монотонно растёт grow_snapshots снимков, набор >= grow_mb."""
    def cond(hist):
        vals = [(h.get(app_key) or {}).get("rss_mb") for h in hist[-(grow_snapshots + 1):]]
        vals = [v for v in vals if isinstance(v, (int, float))]
        if len(vals) < grow_snapshots + 1 or vals[-1] < min_mb:
            return None
        if vals[-1] - vals[0] < grow_mb or any(b < a for a, b in zip(vals, vals[1:])):
            return None
        return (f"RSS вырос с {vals[0]:g} до {vals[-1]:g} МБ за {grow_snapshots} "
                f"снимков подряд (порог {min_mb} МБ)")
    return cond

# Дефолт на случай отсутствия/битости playbooks.json ([124]: ранбук должен быть всегда)
_DEFAULT_PLAYBOOKS = {
    "playbooks": [
        {"name": "restart man_reporting при утечке RSS", "type": "rss_growth",
         "app_key": "man_rep", "min_mb": 1500, "grow_snapshots": 6, "grow_mb": 300,
         "action": "restart man_reporting"},
        {"name": "restart cntsadmin при утечке RSS", "type": "rss_growth",
         "app_key": "cntsadmin", "min_mb": 1500, "grow_snapshots": 6, "grow_mb": 300,
         "action": "restart cntsadmin"},
        {"name": "restart man_reporting при падении процесса", "type": "proc_down",
         "probe_key": "процесс man_reporting_service не найден", "down_sec": 180,
         "action": "restart man_reporting"},
        {"name": "restart cntsadmin при падении процесса", "type": "proc_down",
         "probe_key": "процесс CntsAdminCon не найден", "down_sec": 180,
         "action": "restart cntsadmin"},
    ]
}

def load_playbooks():
    """Читает playbooks.json -> (метриковые, процессные) плейбуки + даты ревизии.

    Поля записи: name, type (rss_growth|proc_down), action, review_date,
    steps (шаги для человека — включаются в алерт, [100]).
    """
    try:
        data = json.loads((BASE / "playbooks.json").read_text(encoding="utf-8"))
        if not data.get("playbooks"):
            raise ValueError("пустой список playbooks")
    except Exception as e:
        print(f"[playbooks] используются встроенные дефолты: {e}", flush=True)
        data = _DEFAULT_PLAYBOOKS
    metric, proc = {}, {}
    for pb in data["playbooks"]:
        entry = {"action": pb["action"], "steps": pb.get("steps", ""),
                 "review_date": pb.get("review_date", "")}
        if pb["type"] == "rss_growth":
            entry["cond"] = _rss_growth(pb["app_key"], pb["min_mb"],
                                        pb["grow_snapshots"], pb["grow_mb"])
            metric[pb["name"]] = entry
        elif pb["type"] == "proc_down":
            entry["probe_key"] = pb["probe_key"]
            entry["down_sec"] = int(pb.get("down_sec", 180))
            proc[pb["name"]] = entry
    return metric, proc

PLAYBOOKS, PROC_PLAYBOOKS = load_playbooks()

# ---------------------------------------------------------------- guardrails и запуск
def _playbook_gate(name, now):
    """Общие ограничители плейбуков: cooldown 1 ч и дневной лимит.

    Возвращает True, если плейбук может сработать; факт срабатывания
    фиксируется в playbook_runs (используется и dry-run, и enforce).
    """
    runs = [t for t in STATE["playbook_runs"].get(name, []) if t > now - 86400]
    if runs and now - runs[-1] < PLAYBOOK_COOLDOWN:
        return False
    if len(runs) >= PLAYBOOK_DAILY_LIMIT:
        print(f"[playbook {name}] дневной лимит исчерпан", flush=True)
        return False
    STATE["playbook_runs"].setdefault(name, []).append(now)
    STATE["playbook_runs"][name] = [t for t in STATE["playbook_runs"][name] if t > now - 86400]
    return True

def _playbook_fire(name, action, reason, steps=""):
    """Реакция плейбука по режиму автономии: enforce — задача в шину, dryrun — уведомление.

    Шаги плейбука прикладываются к алерту ([99]/[100]).
    """
    playbook_txt = f"\nПлейбук:\n{steps}" if steps else ""
    mode = STATE.get("autonomy", "dryrun")
    if mode == "enforce":
        alert(f"🟠 PLAYBOOK: {name}\nУсловие: {reason}\nСтавлю в очередь: {action}" + playbook_txt)
        bus.post("action", {"action": action, "reason": f"{name}: {reason}"})
    else:  # dryrun — только уведомление (ticket: действий не требуется)
        alert(f"🟡 PLAYBOOK (dry-run): {name}\nУсловие: {reason}\n"
              f"В режиме enforce выполнил бы: {action}\n"
              f"Переключить: /autonomy enforce" + playbook_txt, ticket=True)

def evaluate_playbooks(hist):
    """Проверка метриковых плейбуков по истории метрик. Режимы: off | dryrun | enforce.

    В enforce действие НЕ выполняется здесь — публикуется задача в шину,
    которую разбирает executor_loop (рестарты не блокируют metrics_loop).
    """
    if STATE.get("autonomy", "dryrun") == "off":
        return
    now = time.time()
    for name, pb in PLAYBOOKS.items():
        try:
            reason = pb["cond"](hist)
        except Exception as e:
            print(f"[playbook {name}] {e}", flush=True)
            continue
        if not reason:
            continue
        if not _playbook_gate(name, now):
            continue
        _playbook_fire(name, pb["action"], reason, pb.get("steps", ""))

def evaluate_process_playbooks(down_since):
    """Плейбуки на падение процессов (практики [99]/[141]).

    Вызывается из monitor_loop с картой DOWN_SINCE (ключ пробы -> ts начала сбоя).
    Те же guardrails, что у метриковых плейбуков; действие — через шину.
    После успешного рестарта пробы восстанавливаются сами, cooldown
    защищает от повторного срабатывания, пока сервис поднимается.
    """
    if STATE.get("autonomy", "dryrun") == "off":
        return
    now = time.time()
    for name, pb in PROC_PLAYBOOKS.items():
        since = down_since.get(pb["probe_key"])
        if since is None or now - since < pb["down_sec"]:
            continue
        if not _playbook_gate(name, now):
            continue
        reason = f"процесс отсутствует уже {int(now - since)} с (порог {pb['down_sec']} с)"
        _playbook_fire(name, pb["action"], reason, pb.get("steps", ""))

# ---------------------------------------------------------------- ревизия плейбуков
REVIEW_ALERT_DAYS = 180                    # плейбук старше 6 мес — требует ревизии ([124])

def playbook_review_check():
    """Ежедневная проверка свежести плейбуков (вызывается из report_loop)."""
    today = datetime.now(MSK).date()
    stale = []
    for name, pb in list(PLAYBOOKS.items()) + list(PROC_PLAYBOOKS.items()):
        rd = pb.get("review_date") or ""
        try:
            age = (today - date.fromisoformat(rd)).days
        except ValueError:
            stale.append(f"• {name}: нет даты ревизии")
            continue
        if age > REVIEW_ALERT_DAYS:
            stale.append(f"• {name}: ревизия {rd} ({age} дн. назад)")
    if stale:
        alert("📋 Плейбуки требуют ревизии (старше 6 мес — [124]):\n" + "\n".join(stale) +
              "\nОбновите review_date в playbooks.json после пересмотра.", ticket=True)

# ---------------------------------------------------------------- цикл исполнителя
def executor_loop():
    """Разбирает очередь задач bus; выполняет действия по одному (FIFO)."""
    while True:
        try:
            task = bus.take("action")
            if task:
                run_action_auto(task["action"], task.get("reason", ""))
        except Exception as e:
            print(f"[executor] {e}", flush=True)
        time.sleep(5)
