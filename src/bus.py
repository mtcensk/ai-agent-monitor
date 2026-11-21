#!/usr/bin/env python3
"""bus — шина задач между ролями агента.

Диагностика и плейбуки публикуют типизированные задачи (kind + payload),
исполнитель забирает их FIFO. Очередь хранится в STATE["bus_tasks"] —
переживает перезапуск агента.

Типы задач:
  "action"  — {"action": <ключ из executor.ACTIONS>, "reason": <причина>}
"""
import time

from common import STATE, save_state

MAX_TASKS = 100

def post(kind, payload):
    STATE["bus_tasks"].append({"ts": time.time(), "kind": kind, "payload": payload})
    STATE["bus_tasks"] = STATE["bus_tasks"][-MAX_TASKS:]
    save_state()

def take(kind):
    """FIFO: первая задача данного типа или None. Задача снимается с очереди."""
    tasks = STATE["bus_tasks"]
    for i, t in enumerate(tasks):
        if t["kind"] == kind:
            return tasks.pop(i)["payload"]
    return None

def pending(kind=None):
    """Список ожидающих задач (для диагностики)."""
    return [t for t in STATE["bus_tasks"] if kind is None or t["kind"] == kind]
