# AI-агент автономной эксплуатации тестового стенда

Автономный SRE-агент для тестового стенда (VPS `185.209.31.133`): следит за доступностью
приложений (CntsAdminCon `:5000`, man_reporting_service `:5080`), ОС, PostgreSQL и стека
наблюдаемости (Prometheus, Grafana, Tempo, Loki, OTel Collector), диагностирует инциденты
через LLM, выполняет remediation по плейбукам (режим `enforce`), шлёт ежедневные
HTML-отчёты со скриншотами Grafana-дашбордов в Telegram и отвечает на команды владельца.

## Структура проекта

```
ai-agent/
├── src/                    # Исходный код агента (Python 3, stdlib only)
│   ├── agent.py            #   оркестратор: главный цикл, Telegram-команды, whitelist
│   ├── bus.py              #   шина событий между модулями
│   ├── common.py           #   конфиг, state.json, HTTP/SSH/Telegram-хелперы, метрики
│   ├── observer.py         #   проверки: health, ОС, БД, отчёты, генератор данных, Prometheus
│   ├── diagnostician.py    #   LLM-диагностика, RCA, post-mortem, MTTRC
│   ├── executor.py         #   remediation по playbooks.json (dry-run/enforce)
│   └── reporter.py         #   ежедневный HTML-отчёт, скриншоты дашбордов, надёжность
├── deploy/                 # Файлы для сервера
│   ├── ctl.sh              #   start/stop/restart агента
│   ├── run.sh              #   точка запуска
│   ├── deploy_mark.sh      #   маркер деплоя (для MTTR)
│   ├── heartbeat_probe.sh  #   внешний heartbeat-пробник
│   ├── playbooks.json      #   плейбуки remediation
│   └── app.env.example     #   шаблон конфигурации (реальный app.env — только на сервере!)
└── materials/              # Документация и материалы
    ├── docs/               #   описание кода, SRE gap-анализ, роадмап, best practices
    ├── testing/            #   программа тестирования, отчёты по тестам и индексам
    └── presentation/       #   презентация (HTML + PPTX)
```

## Сервер

- Код на сервере: `/app/ai_monitor/`, запуск: `./ctl.sh start|stop|restart`.
- Конфигурация: `/app/ai_monitor/app.env` (права 600, в git не коммитить).
- SSH: `ssh -p 22522 root@185.209.31.133`.

## Поток внесения изменений

Автоматический (основной): правка кода в `src/` или `deploy/` → `git push` в `main` →
GitHub Actions (`.github/workflows/deploy.yml`) заливает код на сервер по SSH,
проверяет синтаксис (`py_compile`), перезапускает агента (`ctl.sh restart`) и
контролирует, что процесс поднялся. Статус — вкладка **Actions** в репозитории.
`app.env`, `state.json`, логи и отчёты деплой не затрагивает.

Ручной (резервный): залить файл на сервер → `python3 -m py_compile` →
`cd /app/ai_monitor && ./ctl.sh restart`.

## Документация

Полное описание кода (модули, классы, функции) — `materials/docs/agent-code-docs.md`.
План развития — `materials/docs/roadmap-plan.md`, `materials/docs/sre-gap-analysis.md`.
