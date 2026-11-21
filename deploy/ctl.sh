#!/usr/bin/env bash
# Управление приложением без root и без systemd.
#   ./ctl.sh start | stop | restart | status | watchdog
# Параметры приложения - в ctl.conf рядом со скриптом:
#   APP_NAME   - имя для сообщений
#   APP_EXE    - исполняемый файл (относительно каталога приложения)
#   ENV_FILE   - файл переменных окружения (необязательно; права 600)
#   STOP_TIMEOUT - сколько секунд ждать корректной остановки до SIGKILL (по умолчанию 45)
#   LOG_MAX_MB / LOG_KEEP - ротация logs/app.log (по умолчанию 50 МБ, 10 архивов)
#
# Автозапуск и перезапуск при падении - через crontab пользователя:
#   * * * * * /app/<приложение>/ctl.sh watchdog >/dev/null 2>&1
# watchdog запускает приложение, если оно не работает и не было остановлено вручную (ctl.sh stop),
# а также ротирует журнал по размеру.
set -euo pipefail

DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
cd "$DIR"
. ./ctl.conf
: "${APP_NAME:?}" "${APP_EXE:?}"
STOP_TIMEOUT="${STOP_TIMEOUT:-45}"
LOG_MAX_MB="${LOG_MAX_MB:-50}"
LOG_KEEP="${LOG_KEEP:-10}"

PID_FILE="$DIR/app.pid"
STOP_FLAG="$DIR/stopped"     # есть файл - watchdog не поднимает приложение
LOG_DIR="$DIR/logs"
LOG="$LOG_DIR/app.log"
EXE="$DIR/$APP_EXE"

running_pid() {
    [ -f "$PID_FILE" ] || return 1
    local pid; pid="$(cat "$PID_FILE")"
    # Процесс жив и это именно наш исполняемый файл (pid мог достаться другому процессу).
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null \
        && grep -aq "agent.py" "/proc/$pid/cmdline" 2>/dev/null \
        && echo "$pid"
}

archive_log() {
    [ -s "$LOG" ] || return 0
    local archive; archive="$LOG_DIR/app.$(date +%Y%m%d-%H%M%S).log"
    # copy + truncate: приложение продолжает писать в тот же файл (открыт с O_APPEND).
    cp "$LOG" "$archive" && : > "$LOG"
    gzip -f "$archive" 2>/dev/null || true
    ls -1t "$LOG_DIR"/app.*.log* 2>/dev/null | tail -n +"$((LOG_KEEP + 1))" | xargs -r rm -f
}

do_start() {
    rm -f "$STOP_FLAG"
    if pid="$(running_pid)"; then echo "$APP_NAME уже работает (pid $pid)"; return 0; fi
    mkdir -p "$LOG_DIR"
    archive_log
    (
        set -a
        if [ -n "${ENV_FILE:-}" ]; then . "$DIR/$ENV_FILE"; fi
        set +a
        exec setsid nohup "$EXE" >> "$LOG" 2>&1 < /dev/null
    ) &
    echo $! > "$PID_FILE"
    sleep 3
    if pid="$(running_pid)"; then
        echo "$APP_NAME запущен (pid $pid), журнал: $LOG"
    else
        echo "$APP_NAME не запустился, последние строки журнала:" >&2
        tail -n 20 "$LOG" >&2
        return 1
    fi
}

do_stop() {
    touch "$STOP_FLAG"
    local pid
    if ! pid="$(running_pid)"; then echo "$APP_NAME не запущен"; rm -f "$PID_FILE"; return 0; fi
    kill -TERM "$pid"
    for _ in $(seq "$STOP_TIMEOUT"); do
        kill -0 "$pid" 2>/dev/null || { rm -f "$PID_FILE"; echo "$APP_NAME остановлен"; return 0; }
        sleep 1
    done
    echo "$APP_NAME не остановился за ${STOP_TIMEOUT} с, SIGKILL" >&2
    kill -KILL "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
}

do_status() {
    if pid="$(running_pid)"; then echo "$APP_NAME работает (pid $pid)"; return 0; fi
    if [ -f "$STOP_FLAG" ]; then echo "$APP_NAME остановлен вручную"; else echo "$APP_NAME не работает"; fi
    return 3
}

do_watchdog() {
    if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt "$((LOG_MAX_MB * 1024 * 1024))" ]; then archive_log; fi
    [ -f "$STOP_FLAG" ] && return 0
    running_pid > /dev/null && return 0
    echo "$(date '+%F %T') watchdog: $APP_NAME не работает, запуск" >> "$LOG_DIR/watchdog.log"
    do_start >> "$LOG_DIR/watchdog.log" 2>&1
}

case "${1:-}" in
    start)    do_start ;;
    stop)     do_stop ;;
    restart)  do_stop; do_start ;;
    status)   do_status ;;
    watchdog) mkdir -p "$LOG_DIR"; do_watchdog ;;
    *) echo "Использование: $0 {start|stop|restart|status|watchdog}" >&2; exit 2 ;;
esac
