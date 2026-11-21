# Документация кода AI-агента мониторинга стенда

**Версия: 2026-11-19 (после спринтов A–G по книге «SRE: Коллективный разум»).**
**Стенд:** CntsAdminCon (админка, порт 5000) + man_reporting_service (порт 5080) + PostgreSQL 5432 на VPS 185.209.31.133.
**Расположение:** `/app/ai_monitor/`. Запуск: `run.sh` / `ctl.sh start|stop|restart|status`, конфиг — `app.env`, состояние — `state.json`, плейбуки — `playbooks.json`.
**Стек:** Python 3, только stdlib. Интеграции: Telegram Bot API, LLM API (OpenAI-совместимый), Grafana render API, Tempo search API, OTel Collector.
**Наблюдаемость стенда:** Prometheus (метрики) → Grafana (дашборды); приложения → OTel Collector (tail sampling) → Tempo (трейсы); файловые логи приложений и агента → Promtail → Loki. В Grafana datasource Tempo связан с Prometheus (`tracesToMetrics`) и Loki (`tracesToLogsV2`: drill-down из трейса в логи того же сервиса по лейблу `service`, окно ±5 мин).

## 1. Архитектура

7 модулей-ролей; обмен — шина задач (`bus.py`) и разделяемое `STATE` (state.json).

```
                ┌─────────────────────────────────────────────┐
                │                  agent.py                    │
                │  оркестратор: 5 потоков + Telegram-команды   │
                └──┬───────┬───────┬───────┬───────┬──────────┘
        monitor_loop  tail_loop metrics_loop report_loop  tg_poll
        ┌──────────▼──┐ ┌──▼────────┐ ┌────▼─────┐ ┌───────▼──────┐
        │ observer    │ │diagnosti- │ │ executor │ │  reporter    │
        │ (наблюдает) │ │cian (оце- │ │(исполня- │ │ (докладывает)│
        │             │ │нивает)    │ │ет)       │ │              │
        └──────┬──────┘ └────┬──────┘ └────┬─────┘ └───────┬──────┘
               └─────────────┴──────┬──────▼──────┬────────┘
                                    │  common.py  │
                                    │ STATE, HTTP │
                                    │ Telegram    │
                                    └─────────────┘
        executor_loop ── bus.take("action") ──> единственный, кто меняет стенд
```

**Инвариант:** решения, меняющие стенд, принимает только `executor` через очередь задач; observer и diagnostician ничего не исполняют.

**Потоки** (daemon, запуск в `__main__` agent.py):

| Поток | Период | Назначение |
|---|---|---|
| `monitor_loop` | 60 с | пробы приложений и стека мониторинга, SLI, burn rate, budget policy, инциденты down/up, re-alert, mute-expiry, плейбуки процессов, deploy-smoke, heartbeat |
| `tail_loop` | 10 с | инкрементальное чтение логов, разбор ERROR-блоков |
| `metrics_loop` | 300 с | снимки метрик, алерты диска/аномалий/зависимостей, прогнозы, инфра-проверки, ошибки отчётов, плейбуки RSS |
| `report_loop` | 60 с | ежедневный отчёт в `REPORT_HOUR` + проверка свежести плейбуков |
| `executor_loop` | 5 с | разбор очереди bus, выполнение действий |
| основной | — | `tg_poll` (long polling Telegram) |

## 2. Конфигурация (common.py, app.env)

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `TELEGRAM_TOKEN` | — | токен бота; без него отправка отключена |
| `TG_ALLOWED_USERS` | — | белый список Telegram user id через запятую; пусто = только владелец зарегистрированного `chat_id`; посторонним бот не отвечает и команд не выполняет (учёт в `self_stats.tg_denied`) |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | endpoint OpenAI-совместимого API |
| `LLM_API_KEY` | — | ключ LLM; без него деградация в шаблонные сообщения |
| `LLM_MODEL` | `gpt-4o-mini` | модель |
| `REPORT_HOUR` | 9 | час ежедневного отчёта по МСК |
| `AUTONOMY` | `dryrun` | стартовый режим плейбуков: off / dryrun / enforce |
| `RECOVERY_STABLE_SEC` | 120 | анти-флаппинг восстановления |
| `METRICS_INTERVAL` | 300 | период снимков метрик |
| `DISK_ALERT_PCT` | 85 | порог диска (алерт и прогноз) |
| `SLO_TARGET` | 99.5 | цель доступности, % за 30 дней |
| `SLO_LAT_MS` | 2000 | порог латентности пробы для SLI |
| `SLO_BURN_FAST` / `SLO_BURN_SLOW` / `SLO_BURN_LONG` | 6 / 2 / 1 | пороги burn rate (1ч+5м / 6ч+30м / 3д) |
| `POSTMORTEM_MIN_DOWNTIME` | 300 | мин. простой (с) для авто-post-mortem |
| `PLAYBOOK_COOLDOWN` / `PLAYBOOK_DAILY_LIMIT` | 3600 / 3 | ограничители плейбуков |
| `REPEAT_ALERT_SEC` | 900 | период re-alert по открытому инциденту |
| `ACK_MUTE_SEC` | 14400 | длительность /ack |
| `GRAFANA_URL` / `GRAFANA_TOKEN` | `http://127.0.0.1:3000` | рендер дашбордов |
| `PROMETHEUS_URL` | `http://127.0.0.1:9090` | проба Prometheus |
| `TEMPO_URL` | `http://127.0.0.1:3200` | API трейсов |
| `SMTP_HOST/PORT/USER/PASS`, `MAIL_TO` | — / 465 | резервный email-канал при недоступности Telegram |

Константы: `LOGS` — пути к логам; `MSK` = UTC+3; `GRAFANA_DASHBOARDS` — 3 дашборда (uid, slug, подпись); `SLO_ALERT_THROTTLE` = 14400 (4 ч).

## 3. Состояние (STATE, state.json)

Единый dict; атомарная запись (tmp + replace, UTF-8) под `STATE_LOCK`.

| Ключ | Содержимое |
|---|---|
| `chat_id` / `ticket_chat_id` | основной чат / ticket-канал P2/P3 (`/start ticket`) |
| `log_offsets` | приложение → byte offset в app.log |
| `incidents` ≤500 | `{ts, kind, text[, rc_ts, rc_confirmed_ts]}`; kind: down / up / error / action / action-auto / deploy / chaos |
| `error_counts` | дата → число ERROR |
| `dedupe` | сигнатура ошибки → ts (троттлинг 30 мин) |
| `tg_offset` | update_id для getUpdates |
| `last_report_date` | дата последнего дайджеста |
| `metrics_history` ≤600 | снимки метрик (~2 суток) |
| `disk_alert_ts`, `baseline_alert_ts`, `dep_alert_ts`, `forecast_alert_ts`, `infra_alert_ts` | троттлинги алертов по типам |
| `report_err_seen` | id отчёта → last_runtime (дедуп алертов о падении отчётов) |
| `pending_action` | заявка на действие/хаос, ждёт /confirm (5 мин) |
| `autonomy` | режим плейбуков |
| `playbook_runs` | плейбук → [ts за 24 ч] |
| `slo_hourly` ≤31 сутки | `"YYYY-MM-DDTHH"` → `{ok, total, lat[]}` |
| `slo_cycles` ≤2000 | сырые исходы циклов `{ts, ok}` — субчасовые окна burn rate |
| `slo_alert_ts` | тип burn-алерта → ts |
| `budget_status` | norm / caution / freeze (error budget policy) |
| `bus_tasks` ≤100 | очередь задач между ролями |
| `self_stats` | сбои собственных каналов: tg_fails, tg_fail_streak, tg_last_ok, tg_email_ts, llm_fails, llm_last_ok, muted_alerts |
| `acks` | ключ инцидента → ts конца заглушения re-alert |
| `mute_until` | silence алертов до ts (макс 24 ч) |
| `last_alert` | последний алерт {ts, text} — для /fb |
| `alert_feedback` | `{good, bad, log[≤100]}` — качество алертов |

## 4. common.py — инфраструктура

- **`load_state()` / `save_state()`** — JSON UTF-8; атомарная запись; setdefault'ы всех ключей (обратная совместимость).
- **`add_incident(kind, text)`** — запись с МСК-меткой, ≤500 символов, журнал ≤500.
- **`http_json(url, payload, headers, timeout, method)`** — POST/GET JSON, UA `ai-monitor/1.0`.
- **`http_text(url, timeout)`** — `(status, text)`.
- **`tg_send(text, chat_id, parse_mode)` → bool** — чанки по 3800, 3 попытки; вызывает `_note_tg`.
- **`_note_tg(ok, text)`** — учёт сбоев канала; при 3 подряд — резервный email (троттлинг 1 ч), при восстановлении — письмо `[50]`/`[80]`.
- **`send_email(subject, text)` → bool** — SMTP_SSL/STARTTLS; False если не настроен.
- **`alert(text, chat_id, parse_mode, ticket=False)` → bool** — единая точка отправки АЛЕРТОВ: фиксирует `last_alert` (для /fb), подавляет при активном `/mute` (счётчик `muted_alerts`), маршрутизирует ticket-алерты в ticket-чат `[48]`. Ответы на команды идут через tg_send напрямую и не глушатся.
- **`esc(s)`, `_read_file(p)`, `_n(x)`** — экранирование HTML, безопасное чтение, формат числа.

## 5. bus.py — шина задач

Очередь в `STATE["bus_tasks"]` (≤100, переживает рестарт). Тип `"action"` → `{action, reason}`.
- **`post(kind, payload)`**, **`take(kind)`** (FIFO), **`pending(kind=None)`** (для /bus).

## 6. observer.py — «наблюдатель»

### Пробы
- **`LAST_PROBES`** — имя пробы → `{ok, ms}` (SLI).
- **`_probe(name, fn, problems, fail_text)`** — обёртка с замером латентности.
- **`check_health()`** — 6 проб приложений: `/health` man_reporting, `/login` cntsadmin, pgrep обоих процессов, `pg_isready`.
- **`check_stack()`** — пробы стека мониторинга `[50]`: Grafana `/api/health`, Prometheus `/-/ready`, Tempo `/ready`, otelcol `:13133`. Отдельно от check_health, чтобы сбой наблюдаемости не портил SLI приложений.

### SLI
- **`slo_record_cycle(problems)`** — почасовая агрегация + запись исхода в `slo_cycles` (≤2000, для окон 5/30 мин burn rate).

### Метрики
- **`proc_stats(pattern)`** — count/RSS МБ/потоки по pgrep.
- **`pg_dependency()`** — RED-метрики PostgreSQL `[62]`: latency `SELECT 1` (включает fork psql ~20–50 мс — верхняя оценка), активные запросы, самый длинный запрос, lock-waits, % max_connections.
- **`collect_metrics()`** — снимок: диск + **inode**, память, swap, load, CPU-счётчики, uptime, процессы, `pg_db_mb`/`pg_conns`, `pg_dep`, прикладные метрики `man_reporting_*`, а также «скучные» ресурсы `[134]`: **NTP-sync, DNS-резолв, свежесть бэкапа (ч), объём Tempo/Prometheus (МБ)** и **свежесть генератора тестовых данных** (`data_gen_age_h` / `data_gen_errors` / `data_gen_rows` из `/app/data_gen/state.json`).
- **`metrics_summary()`** — now/min/avg/max за 24 ч (включая inode), CPU busy, динамика прикладных метрик.

### Прочее
- **`read_log_chunk(app)`** — инкрементальное чтение с offsets и переживанием ротации.
- **`smoke_test(app, rounds=5)`** — deploy smoke-пробы `[140]`: rounds запросов на ключевые endpoint'ы → (ok, per-url статистика, p95).
- **`tempo_traces(minutes, limit, min_duration_ms)`** — поиск трейсов в Tempo; None = недоступен.
- **`report_errors_check()`** — новые падения отчётов man_reporting_service: запрос к `man_reporting_settings` (state <> 'Complete' за 2 суток), дедупликация по `(id, last_runtime)` в `report_err_seen` — один алерт на каждое падение, с именем отчёта и причиной. Предметный дублёр generic-алерта из логов сервиса.

## 7. diagnostician.py — «диагностик»

### LLM
- **`mask_secrets(text)`** — пароли/токены/креды в URL + **ПДн `[41]`: email, телефоны РФ, номера карт**.
- **`llm(system, user, max_tokens)`** — chat/completions, `x-opencode-session`; при сбое — счётчик `self_stats.llm_fails`, при успехе — `llm_last_ok`; None = деградация в шаблоны.
- **`SYS_ANALYST`** — системный промпт дежурного SRE-аналитика.

### RAG
- **`similar_incidents(text, limit)`** — пересечение значимых слов с последними 200 инцидентами.
- **`rag_context(text)`** — блок «похожие инциденты» в промпт.

### SLO / burn rate (MWMBR, `[49]`)
- **`_slo_window(hs)`**, **`_burn_hourly(hs)`**, **`_burn_cycles(minutes)`** — источники burn rate (часовые корзины и сырые циклы).
- **`slo_report()`** — доступность 30д, p95, SLI латентности, бюджет, burn 1ч/6ч/**3д**.
- **`slo_burn_check()`** — мультиоконные алерты: fast (1ч **и** 5м ≥ 6, page), slow (6ч **и** 30м ≥ 2, ticket), long (3д ≥ 1, ticket); троттлинг 4 ч; LLM-разбор + RAG.

### Error budget policy (`[25]`)
- **`budget_policy()`** — norm >50% / caution 20–50% / freeze <20% остатка бюджета.
- **`budget_policy_check()`** — реакции на переходы: freeze → **авто-понижение enforce→dryrun** (возврат только вручную), caution → предупреждение, norm → уведомление о восстановлении.

### Аномалии и зависимости
- **`baseline_check(hist)`** — mean+2σ по суточному окну (5 метрик), троттлинг 4 ч.
- **`dependency_check(m)`** — warning-пороги RED PostgreSQL `[62]`: latency >500 мс, запрос >300 с, conns >80%, lock-wait >0; троттлинг 4 ч.

### Прогноз и инфра
- **`forecast_check(hist)`** — линейная регрессия за сутки (диск, RSS обоих приложений): «пересечение порога через ~N ч» при N ≤ 24 `[37]`; троттлинг 12 ч.
- **`infra_check(m)`** — inode ≥85%, NTP не синхронизирован, DNS не резолвит, бэкап старше 30 ч или отсутствует, **генератор данных не запускался >30 ч / ошибки в последнем запуске** `[134]`; троттлинг 6 ч.

### Логи
- **`LOG_RE`**, **`process_log_chunk(app, chunk)`** — многострочные ERROR-блоки.
- **`on_error(app, text)`** — счётчики, инцидент, сигнатура-дедуп 30 мин, LLM-разбор, ticket-алерт.

## 8. executor.py — «исполнитель»

Единственный модуль, меняющий стенд. Белые списки действий.

- **`ACTIONS`** — рестарты сервисов через ctl.sh.
- **`CHAOS_ACTIONS`** — `chaos kill man_reporting|cntsadmin` (pkill) — учебные инциденты, только вручную `[122]`.
- **`_run(action, audit_kind, audit_note, ...)`** — выполнение с аудитом в incidents и alert-отчётом (rc, хвост вывода ≤800).
- **`run_action` / `run_chaos` / `run_action_auto`** — L2 по команде человека / учебный инцидент (`kind=chaos`) / автодействие плейбука (`kind=action-auto`).

### Плейбуки (декларативные, playbooks.json)
- **`load_playbooks()`** — читает `playbooks.json` (types: `rss_growth`, `proc_down`; поля `review_date`, `steps`); при отсутствии/битости — встроенные дефолты `[124]`.
- **`_rss_growth(app_key, min_mb, grow_snapshots, grow_mb)`** — фабрика условий утечки RSS.
- **`_playbook_gate(name, now)`** — общие guardrails: cooldown 1 ч, лимит 3/сутки.
- **`_playbook_fire(name, action, reason, steps)`** — enforce → задача в bus; dryrun → ticket-уведомление. Шаги плейбука прикладываются к алерту `[99]`/`[100]`.
- **`evaluate_playbooks(hist)`** — метриковые плейбуки (из metrics_loop).
- **`evaluate_process_playbooks(down_since)`** — плейбуки падения процессов (из monitor_loop, порог 180 с).
- **`playbook_review_check()`** — ежедневно: `review_date` старше 6 мес → ticket-алерт «требует ревизии» `[124]`.
- **`executor_loop()`** — FIFO-разбор bus, 5 с.

## 9. reporter.py — «докладчик»

- **`_metrics_rows(sm)`** — 11 строк (включая inode).
- **`metrics_message()`** — HTML-таблица now/min/avg/max + динамика прикладных метрик.
- **`slo_message()`** — /slo: доступность, p95, бюджет + **policy-статус**, burn 1ч/6ч/3д.
- **`grafana_render(...)`, `dashboard_images(hours, max_age=300)`, `send_dashboards(...)`** — рендеры дашбордов с кэшем 5 мин.
- **`reliability_stats(days=7)`** — инциденты, MTTR ср/макс, MTTD ≤60 с, два MTTRC `[64]`: **по гипотезе** (`rc_ts` — первая LLM-гипотеза при детекте) и **по подтверждённой причине** (`rc_confirmed_ts` — момент post-mortem'а) + размер выборки.
- **`monthly_slo_summary()`** — календарный итог прошлого месяца `[27]` (в дайджесте 1-го числа).
- **`collect_facts()`** — полный снимок: health, метрики, настройки отчётов из БД, инциденты 24 ч, ошибки, summary, reliability (+MTTRC), slo, **качество алертов (actionable %)**, mute-статус, календарный срез, трейсы.
- **`build_html_report()`** — самодостаточный HTML (таблицы, дашборды base64, инциденты, MTTRC).
- **`daily_report(chat_id, full_llm)`** — LLM-дайджест или fallback-шаблон (+SLO, качество алертов, календарный итог 1-го числа) + таблица метрик + фото дашбордов + HTML-документ (архив `reports/`).
- **`_postmortem_window / build_postmortem / maybe_postmortem / _postmortem_cmd`** — HTML post-mortem с LLM-разбором, архив `postmortems/`. Генерация post-mortem (авто ≥300 с или `/postmortem`) через **`_mark_rc_confirmed()`** проставляет `rc_confirmed_ts` на down-инцидент(ы) того же компонента в окне простоя — это фиксация MTTRC по подтверждённой причине `[64]`; первое подтверждение выигрывает, повторный разбор метку не двигает.

## 10. agent.py — оркестратор

### Циклы
- **`monitor_loop()` (60 с)** — `check_health()` + `check_stack()` (отдельно от SLI); `slo_record_cycle`; `slo_burn_check`; `budget_policy_check`; корреляция новых сбоев в один инцидент (+трейсы Tempo, LLM, RAG, **`rc_ts` для MTTRC**); восстановления с анти-флаппингом (+авто-post-mortem ≥300 с); **re-alert по открытым инцидентам (15 мин, `[81]`)**; **истечение /mute с отчётом о подавленных**; **плейбуки процессов**; **deploy-маркеры**; heartbeat; save_state.
- **`handle_deploy_marks()` / `_smoke_verdict(app)`** — маркер `deploy_marks/<app>` → smoke-прогон → verdict ✅/🔥 (с рекомендацией отката `[144]`), журнал `kind=deploy`.
- **`tail_loop()` (10 с)**, **`metrics_loop()` (300 с: диск, аномалии, зависимости, **прогнозы, инфра**, плейбуки RSS)**, **`report_loop()` (дайджест + **ревизия плейбуков**)**, **`tg_poll()`**.

### Команды Telegram (`handle_command`)

Регистрация: `/start` (основной чат), `/start ticket` (ticket-канал P2/P3) — только для пользователей из белого списка `TG_ALLOWED_USERS` (проверка по `from.id` в `tg_poll` → `tg_allowed_uid`; посторонние сообщения молча отбрасываются со счётчиком `tg_denied`).
Остальные команды — только из зарегистрированного чата.

| Команда | Действие |
|---|---|
| `/status` | пробы приложений + стек мониторинга + каналы tg/LLM + **RED PostgreSQL** + открытые инциденты |
| `/report` | ежедневный отчёт (асинхронно) |
| `/metrics [часы]` | таблица + дашборды Grafana + HTML-отчёт |
| `/slo` | SLO-статус с policy и burn 1ч/6ч/3д |
| `/trace [мин]` | трейсы Tempo (5–120 мин) |
| `/errors` / `/logs [N]` | последние ошибки / хвост лога |
| `/restart <app>` → `/confirm` / `/cancel` | L2-рестарт с подтверждением (5 мин) |
| `/chaos kill <app>` → `/confirm` | учебный инцидент, аудит `chaos` `[94]` |
| `/ack` | глушит re-alert по открытым инцидентам на 4 ч |
| `/mute [мин\|off]` | silence алертов (макс 24 ч, отчёт по истечении) `[143]` |
| `/fb good\|bad` | оценка качества последнего алерта `[51]` |
| `/autonomy [off\|dryrun\|enforce]` | режим плейбуков |
| `/bus` | очередь задач |
| `/postmortem [N]` | post-mortem по N-му инциденту с конца |
| `/ask <вопрос>` | LLM-ответ по фактам стенда |

### Точка входа
Печать метки старта → 5 daemon-потоков (monitor, tail, report, metrics, executor) → `tg_poll()` в основном потоке.

## 11. Деградация и устойчивость

- **Нет LLM** — шаблонные сообщения, счётчики llm_fails; **нет Telegram** — журнал + резервный email; **нет Tempo/Grafana/Prometheus/otelcol** — инциденты через check_stack; **битый playbooks.json** — встроенные дефолты.
- Исключения внутри итераций циклов логируются и не роняют потоки; state.json атомарен (UTF-8, tmp+replace).
- Внешняя страховка: cron-watchdog приложений (перезапуск ~1 мин), `heartbeat_probe.sh` для самого агента, systemd `Restart=always` у tempo/otelcol/prometheus/grafana.
- Ежедневные бэкапы БД: pg_dump в `/app/backups` (03:17, ретенция 7 дней), свежесть под контролем infra_check.

## 12. Файлы на сервере

```
/app/ai_monitor/
├── agent.py / common.py / bus.py / observer.py
├── diagnostician.py / executor.py / reporter.py
├── agent.py.bak      # бэкап монолита до рефакторинга
├── playbooks.json    # декларативные плейбуки + review_date + steps
├── deploy_mark.sh    # маркер деплоя -> smoke-пробы
├── app.env           # секреты и конфигурация
├── ctl.sh / run.sh
├── state.json        # состояние (раздел 3)
├── heartbeat + heartbeat_probe.sh
├── logs/app.log      # журнал агента
├── reports/          # архив HTML-отчётов
├── postmortems/      # архив post-mortem
├── deploy_marks/     # маркеры деплоя (одноразовые)
└── dash_*.png        # кэш рендеров дашбордов (TTL 5 мин)

/app/otelcol/         # OTel Collector 0.114 (tail sampling -> Tempo:14317)
/app/tempo/           # Tempo 3.1.1 (API :3200, ingest :14317/14318, данные data/)
/app/loki/            # Loki 3.5.1 (API 127.0.0.1:3100, grpc :19095, данные data/, ретенция 14 дн)
/app/promtail/        # Promtail 3.5.1 (127.0.0.1:9080, сбор файловых логов -> Loki)
/app/backups/         # ежедневные pg_dump (cron 03:17, ретенция 7 дн)
/app/data_gen/        # генератор тестовых данных (cron 04:09, ~10 тыс. строк/сутки на БД,
                      #   state.json читается агентом: свежесть/ошибки — infra_check)
```
