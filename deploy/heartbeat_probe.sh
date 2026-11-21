#!/usr/bin/env bash
# Внешний пробник: если ai_monitor молчит > 30 мин — алерт в Telegram напрямую.
# Запускается из cron каждые 5 минут, не зависит от агента.
set -u
. /app/ai_monitor/app.env

HB=/app/ai_monitor/heartbeat
FLAG=/tmp/ai_monitor_hb_alerted
NOW=$(date +%s)
LAST=$(stat -c %Y "$HB" 2>/dev/null || echo 0)
AGE=$(( NOW - LAST ))
[ "$AGE" -le 1800 ] && exit 0

# троттлинг: не чаще раза в час
if [ -f "$FLAG" ] && [ $(( NOW - $(stat -c %Y "$FLAG") )) -lt 3600 ]; then
  exit 0
fi
touch "$FLAG"

CHAT_ID=$(python3 -c "import json;print(json.load(open('/app/ai_monitor/state.json')).get('chat_id',''))" 2>/dev/null)
[ -z "$CHAT_ID" ] && exit 0

curl -s -m 20 -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${CHAT_ID}" \
  --data-urlencode "text=🟥 AI-АГЕНТ МОНИТОРИНГА МОЛЧИТ
Heartbeat не обновлялся уже $(( AGE / 60 )) мин ($(date '+%Y-%m-%d %H:%M:%S %Z')).
Проверьте на сервере: /app/ai_monitor/ctl.sh status
Лог: /app/ai_monitor/logs/app.log" > /dev/null 2>&1
exit 0
