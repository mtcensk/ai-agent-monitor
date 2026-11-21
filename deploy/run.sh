#!/usr/bin/env bash
# Обёртка для ctl.sh: запускает агента мониторинга.
exec /usr/bin/python3 -u /app/ai_monitor/agent.py
