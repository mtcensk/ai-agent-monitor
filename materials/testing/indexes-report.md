# Отчёт по индексам для тестового стенда CntsAdminCon

**Дата:** 16.11.2026
**Стенд:** VDSina 185.209.31.133, PostgreSQL 18.6, ~1,68 млн записей в 4 рабочих БД
**Приложение:** CntsAdminCon.Refactored (Blazor Server, .NET 8)

## 1. Исходное состояние

В тестовых скриптах `testdb` были созданы только первичные ключи — ни одного вторичного индекса. После генерации массовых данных (300 тыс. документов ФССП, 300 тыс. сообщений Центрос, 200 тыс. сообщений Kafka outbox, 150 тыс. записей шлюза АБС, 100 тыс. записей шлюза ППТЦ) все поисковые запросы админки выполнялись полным сканированием таблиц (Seq Scan).

## 2. Методика

Из исходного кода (`Infrastructure/Persistence/*.cs`) извлечены все SQL-запросы и паттерны доступа. Для 9 типовых запросов выполнен замер `EXPLAIN (ANALYZE)` **до** и **после** создания индексов на одних и тех же значениях параметров. Индексы созданы с `CONCURRENTLY` (без блокировки таблиц), после создания выполнен `ANALYZE`.

## 3. Результаты замеров (до → после)

| # | Запрос (страница) | План до | Время до | План после | Время после | Ускорение |
|---|---|---|---|---|---|---|
| Q1 | Поиск документа по ключу ФССП (`OIp."Id"`) | Seq Scan | 67,6 мс | Index Scan | 0,26 мс | **×260** |
| Q2 | Поиск по № ИП / баркоду (`OIp."IpNo"/"Barcode"`) | Seq Scan | 80,5 мс | Bitmap Or (2 индекса) | 0,75 мс | **×107** |
| Q3 | По RequestMessageId (PK, контроль) | Index Scan (PK) | 0,17 мс | Index Scan (PK) | 0,06 мс | без изменений |
| Q4 | Дочерние сообщения (`RequestMessage."_parentId"`) | Seq Scan | 36,6 мс | Index Scan | 0,09 мс | **×400** |
| Q5 | Платёжные реквизиты (`DetIdPaymentProperties."RequestMessageId"`) | Seq Scan | 31,5 мс | Index Scan | 0,09 мс | **×350** |
| Q6 | Вкладка Kafka: outbox+inbox+tickets по BusinessTransactionId | 3 × Seq Scan | 76,9 мс | Index Scan | 0,32 мс | **×240** |
| Q7 | ФАС Инфо: суммы по `TaskId LIKE` | Seq Scan | 11,1 мс | Index Scan | 0,11 мс | **×100** |
| Q8 | Шлюз АБС: request+response по header_id | 2 × Seq Scan | 97,7 мс | Index Scan | 0,28 мс | **×350** |
| Q9 | Шлюз ППТЦ: request+response+errors по process_id | 3 × Seq Scan | 51,3 мс | Index Scan | 0,33 мс | **×156** |

Все целевые запросы ушли с полного сканирования на индексный доступ; время отклика на уровне SQL — **доли миллисекунды** против 30–100 мс.

## 4. Созданные индексы (42 шт., ~53 MB)

### cnts_decr_oippg_db — 31 индекс, 30 MB

**Поиск документа (страница «Информация»):**

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_oip_id | fssp."OIp" ("Id") | поиск по ключу ФССП (Q1), join'ы связанных документов |
| ix_oip_barcode | fssp."OIp" ("Barcode") | поиск по баркоду (Q2) |
| ix_oip_ipno | fssp."OIp" ("IpNo") | поиск по номеру ИП (Q2) |
| ix_reqmsg_parent | cnts."RequestMessage" ("_parentId") | дочерние сообщения — используется в 10+ подзапросах страницы документа (Q4) |

**Детализация документа:**

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_detpay_rmid | fssp."DetIdPaymentProperties" ("RequestMessageId") | платёжные реквизиты (Q5) |
| ix_detipdocs_rmid | fssp."DetIPDocsInfo" ("RequestMessageId") | связанные документы (отмена/обременение) |
| ix_detipdocs_key | fssp."DetIPDocsInfo" ("InternalKey", "DocDate") | составной — join отмены к исходному постановлению |
| ix_mvvavail_rmid | fssp."MvvDatumAvailabilityAcc" ("RequestMessageId") | счета доступности |
| ix_mvvacc_detmvv | fssp."MvvAccountDatum" ("DetMvvId") | счета задания, CENTROS-3735 |
| ix_reportarrest_rmid | fssp."ReportArrest" ("RequestMessageId") | отчёты об арестах |
| ix_answer3_rmid | cnts."Answer3DetRequestSubject" ("RequestMessageId") | цепочка заданий |
| ix_detmvv_subject | cnts."DetMvvDatum" ("RequestSubjectId") | цепочка заданий |
| ix_detmvvattr_dmd | cnts."DetMvvDatumAttributes" ("DetMvvDatumId") | атрибуты заданий |
| ix_ulreq_rmid / ix_ulresp_req | UlClientSearch* ("RequestMessageId" / "RequestId") | поиск ЮЛ |
| ix_flreq_rmid / ix_flresp_req | FlClientSearch* ("RequestMessageId" / "RequestId") | поиск ФЛ |
| ix_bssoutbox_rmid | cnts."KafkaAdapterBssOutbox" ("RequestMessageId") | уведомления БСС |

**Буфер СОИД (страница «Поиск СОИД») и смежные:**

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_oipexp_ipno | mgr."OIp_exp" ("IpNo") | поиск по номеру ИП |
| ix_oipexp_soidid | mgr."OIp_exp" ("SOIDID") | поиск по SOIDID |
| ix_oipexp_frmid | mgr."OIp_exp" ("FSSP_RequestMessageId") | связь с документом |
| ix_oipaccexp_id | mgr."OIPAcc_exp" ("Id") | join к OIp_exp |
| ix_oipaccexp_acc | mgr."OIPAcc_exp" ("Acc") | поиск по счёту |
| ix_oipaccexp_taskid | mgr."OIPAcc_exp" ("TaskId") | поиск по ID блокировки |
| ix_oipextexp_soidid | mgr."OIPExt_exp" ("SOIDID") | флаг расширения |
| ix_gatetasks_rmid | mgr."GateTasks" ("RequestMessageId") | задания шлюза |

**Прочие страницы:**

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_flmig_mainvid | vtb."FL_migration_in_Blocks_Savings" ("MAIN_V_ID") | страница «ФЛ Блок» — поиск по счёту |
| ix_comments_corr | vtb."Comments" ("CorrelationId") | комментарии к документу |
| ix_ticket_rsmev | vtb."Ticket" ("RSMEVDocID") | квитанции ФССП |
| ix_rmexp_rmid | fssp_export."RequestMessageExports" ("RequestMessageId") | экспорт ВТБ Онлайн |
| ix_exptasks_rmid | fssp_export."ExportTasks" ("RequestMessageId") | стриминг |

### cnts_decr_kafka_db — 6 индексов, 13 MB

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_outbox_bt | KafkaAdapterOutbox ("BusinessTransactionId") | главный поиск вкладки Kafka (Q6) и все проверки отмен блокировок |
| ix_inbox_outbox | KafkaAdapterInbox ("OutboxId") | join ответов (Q6) |
| ix_tickets_outbox | KafkaAdapterTickets ("OutboxId") | join квитанций (Q6) |
| ix_queue_outbox | KafkaAdapterSendingQueue ("OutboxId") | очередь отправки |
| ix_outbox_senton | KafkaAdapterOutbox ("SentOn") | окно ручных отмен PPRS (`SentOn > current_date - 2`) |
| ix_amounts_taskid | KafkaAdapterArrestedAmounts ("TaskId") | ФАС Инфо: `LIKE 'prefix%'` (Q7) |

### abs_gateway_db — 2 индекса, 9 MB

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_request_header | request (header_id) | «Информация с Шлюза» по KafkaCorrelationId (Q8) |
| ix_response_header | response (header_id) | join ответа (Q8) |

### cnts_exec_pptc_gate_db — 3 индекса, 6,5 MB

| Индекс | Таблица (колонки) | Зачем |
|---|---|---|
| ix_pptc_req_proc | pptc_request (process_id) | поиск обмена ППТЦ (Q9) |
| ix_pptc_resp_proc | pptc_response (process_id) | join ответа (Q9) |
| ix_pptc_err_header | error_message (header_id) | join ошибок (Q9) |

## 5. Замечания и ограничения

1. **JSON-фильтры не индексируются.** Проверки отмен блокировок (`KafkaOutboxSql`) фильтруют по `(​"Content"::json #>> '{block,id}')` — под них создан индекс только по "BusinessTransactionId" (сужает выборку до сообщений одного документа, JSON-фильтр дорабатывает). При росте объёма outbox сверх нескольких миллионов строк на документ стоит рассмотреть expression-индекс по `(​"Content"::jsonb #>> '{block,id}')`.
2. **Полнотекстовый фильтр вкладки Kafka** (`"Content" like '%…%'`) — ведущий `%`, btree не поможет; при необходимости — trigram-индекс (pg_trgm). На текущих объёмах фильтр применяется после отбора по BusinessTransactionId, проблемы нет.
3. **LIKE с параметром** (`"TaskId" like @DocumentId`): индекс используется для шаблонов вида `'TASK-…%'` (без ведущего `%`) — это соответствует сценарию страницы «ФАС Инфо».
4. **Накладные расходы:** +53 MB на диске (~12 % к объёму данных, итого БД ~447 MB), замедление вставки — на тестовом стенде несущественно.
5. Индексы созданы с `CONCURRENTLY` — без простоя приложения; скрипт идемпотентен (`IF NOT EXISTS`). Файлы: `build/create_indexes.sql`, замеры — `build/bench.sql` (прогоняется до/после).
6. Если тестовые БД будут пересоздаваться `run.sh`, индексы пропадут — нужно повторно выполнить `create_indexes.sql` (или включить его в testdb).

## 6. Рекомендация для боевого контура

Перенести в прод-миграции как минимум «горячие» индексы: `ix_oip_id`, `ix_oip_barcode`, `ix_oip_ipno`, `ix_reqmsg_parent`, `ix_outbox_bt`, `ix_inbox_outbox`, `ix_tickets_outbox`, `ix_request_header`, `ix_response_header`, `ix_pptc_req_proc`, `ix_pptc_resp_proc`, `ix_pptc_err_header`, `ix_flmig_mainvid` — они обслуживают основные пользовательские сценарии и дают ускорение на 2 порядка. Остальные — по результатам наблюдения за реальной нагрузкой (pg_stat_statements).
