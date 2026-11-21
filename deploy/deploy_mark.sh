#!/usr/bin/env bash
# Отметка о деплое для ai_monitor: агент прогонит smoke-пробы и вынесет verdict ([140]).
# Использование: ./deploy_mark.sh man_reporting | cntsadmin
# Вызов добавляется в конец деплой-скрипта после ctl.sh restart.
set -euo pipefail
APP="${1:?имя приложения: man_reporting | cntsadmin}"
MARKS=/app/ai_monitor/deploy_marks
mkdir -p "$MARKS"
date +%s > "$MARKS/$APP"
echo "deploy mark: $APP — ai_monitor выполнит smoke-пробы"
