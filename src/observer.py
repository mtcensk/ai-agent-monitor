#!/usr/bin/env python3
"""observer — роль «наблюдатель»: только собирает данные, решений не принимает.

Health-пробы приложений и PostgreSQL, снимки метрик ОС/процессов/БД,
чтение логов с offsets, запросы к Tempo, запись SLI-исходов по часам.
"""
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

from common import (STATE, LOGS, MSK, TEMPO_URL, GRAFANA_URL, PROMETHEUS_URL,
                    http_json, http_text, _read_file)

# ---------------------------------------------------------------- health-пробы
LAST_PROBES = {}   # имя пробы -> {"ok": bool, "ms": int} — для SLI

def _probe(name, fn, problems, fail_text):
    """Выполняет пробу, фиксирует латентность и результат в LAST_PROBES."""
    t0 = time.time()
    try:
        fail = fn()
    except Exception as e:
        fail = f"{fail_text}: {e}"
    LAST_PROBES[name] = {"ok": fail is None, "ms": int((time.time() - t0) * 1000)}
    if fail is not None:
        problems.append(fail if fail != fail_text else fail_text)

def check_health():
    """Возвращает список проблем (пусто = всё хорошо)."""
    problems = []

    def man_health():
        st, body = http_text("http://127.0.0.1:5080/health")
        if st != 200 or body.strip() != "Healthy":
            return f"man_reporting /health -> {st} {body.strip()[:60]}"
        return None
    _probe("man_health", man_health, problems, "man_reporting /health недоступен")

    def cnts_login():
        st, _ = http_text("http://127.0.0.1:5000/login")
        if st != 200:
            return f"cntsadmin /login -> {st}"
        return None
    _probe("cnts_login", cnts_login, problems, "cntsadmin /login недоступен")

    for proc in ("man_reporting_service", "CntsAdminCon"):
        _probe(f"proc_{proc}",
               lambda p=proc: (f"процесс {p} не найден"
                               if subprocess.run(["pgrep", "-f", p], capture_output=True).returncode != 0
                               else None),
               problems, f"процесс {proc} не найден")
    _probe("postgres",
           lambda: ("PostgreSQL не отвечает (pg_isready)"
                    if subprocess.run(["pg_isready", "-q", "-h", "127.0.0.1", "-p", "5432"],
                                      capture_output=True).returncode != 0
                    else None),
           problems, "PostgreSQL не отвечает (pg_isready)")
    return problems

def check_stack():
    """Пробы самого стека мониторинга (практика [50]): Grafana, Prometheus, Tempo.

    Отдельно от check_health: сбой стека — инцидент наблюдаемости,
    он не должен портить SLI доступности приложений (slo_record_cycle).
    """
    problems = []

    def grafana():
        st, _ = http_text(f"{GRAFANA_URL}/api/health")
        if st != 200:
            return f"grafana /api/health -> {st}"
        return None
    _probe("grafana", grafana, problems, "grafana недоступна")

    def prometheus():
        st, _ = http_text(f"{PROMETHEUS_URL}/-/ready")
        if st != 200:
            return f"prometheus /-/ready -> {st}"
        return None
    _probe("prometheus", prometheus, problems, "prometheus недоступен")

    def tempo():
        st, _ = http_text(f"{TEMPO_URL}/ready")
        if st != 200:
            return f"tempo /ready -> {st}"
        return None
    _probe("tempo", tempo, problems, "tempo недоступен")

    def otelcol():
        st, _ = http_text("http://127.0.0.1:13133/")
        if st != 200:
            return f"otelcol health -> {st}"
        return None
    _probe("otelcol", otelcol, problems, "otelcol недоступен")
    return problems

# ---------------------------------------------------------------- запись SLI
def slo_record_cycle(problems):
    """Почасовая агрегация исходов health-проб и латентностей. Храним 31 сутки.

    Дополнительно пишет исход цикла в slo_cycles (кольцевой буфер ~2000 записей) —
    для коротких окон burn rate (5/30 мин), недостижимых по часовым корзинам.
    """
    cy = STATE["slo_cycles"]
    cy.append({"ts": int(time.time()), "ok": 0 if problems else 1})
    del cy[:-2000]
    h = datetime.now(MSK).strftime("%Y-%m-%dT%H")
    b = STATE["slo_hourly"].setdefault(h, {"ok": 0, "total": 0, "lat": []})
    b["total"] += 1
    if not problems:
        b["ok"] += 1
    b["lat"].extend(p["ms"] for p in LAST_PROBES.values() if isinstance(p.get("ms"), int))
    b["lat"] = sorted(b["lat"])[-120:]           # до 120 замеров на час
    cut = (datetime.now(MSK) - timedelta(days=31)).strftime("%Y-%m-%dT%H")
    for k in [k for k in STATE["slo_hourly"] if k < cut]:
        del STATE["slo_hourly"][k]

# ---------------------------------------------------------------- снимки метрик
def proc_stats(pattern):
    """Число процессов, суммарный RSS (МБ) и потоки по шаблону pgrep -f."""
    pids = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True).stdout.split()
    rss = thr = 0
    for pid in pids:
        st = _read_file(f"/proc/{pid}/status")
        m = re.search(r"VmRSS:\s+(\d+) kB", st)
        rss += int(m.group(1)) // 1024 if m else 0
        m = re.search(r"Threads:\s+(\d+)", st)
        thr += int(m.group(1)) if m else 0
    return {"count": len(pids), "rss_mb": rss, "threads": thr}

def pg_dependency():
    """RED-метрики ключевой зависимости (PostgreSQL) с точки зрения клиента (практика [62]).

    latency_ms — время `SELECT 1` через psql (включает fork ~20–50 мс:
    грубая верхняя оценка, деградацию видно отчётливо).
    """
    dep = {}
    t0 = time.time()
    subprocess.run(["sudo", "-u", "postgres", "psql", "-X", "-t", "-A", "-c", "SELECT 1"],
                   capture_output=True, timeout=10)
    dep["latency_ms"] = int((time.time() - t0) * 1000)
    out = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-X", "-t", "-A", "-F|", "-c",
         "SELECT (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()),"
         "(SELECT COALESCE(EXTRACT(EPOCH FROM max(now()-query_start))::int,0) FROM pg_stat_activity "
         " WHERE state='active' AND pid<>pg_backend_pid() AND query NOT LIKE '%pg_stat_activity%'),"
         "(SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'),"
         "(SELECT count(*)*100/(SELECT setting::int FROM pg_settings WHERE name='max_connections')"
         " FROM pg_stat_activity)"],
        capture_output=True, text=True, timeout=15).stdout.strip().split("|")
    dep["active"], dep["longest_active_s"], dep["lock_waits"], dep["conns_pct"] = \
        int(out[0]), int(out[1]), int(out[2]), int(out[3])
    return dep

def collect_metrics():
    """Снимок метрик ОС, процессов, PostgreSQL и самих приложений."""
    m = {"ts": datetime.now(MSK).isoformat(timespec="seconds")}
    st = os.statvfs("/")
    m["disk_pct"] = round(100 * (1 - st.f_bavail / st.f_blocks), 1)
    m["disk_free_gb"] = round(st.f_bavail * st.f_frsize / 2 ** 30, 1)
    m["inode_pct"] = round(100 * (1 - st.f_favail / st.f_files), 1) if st.f_files else None
    # «скучные» ресурсы ([134]): NTP, DNS, бэкапы, объём стека мониторинга
    try:
        out = subprocess.run(["timedatectl", "show", "-p", "NTPSynchronized", "--value"],
                             capture_output=True, text=True, timeout=5).stdout.strip()
        m["ntp_synced"] = (out == "yes")
    except Exception:
        m["ntp_synced"] = None
    try:
        import socket
        socket.setdefaulttimeout(5)
        socket.gethostbyname("ya.ru")
        m["dns_ok"] = True
    except Exception:
        m["dns_ok"] = False
    try:
        bdir = Path("/app/backups")
        dumps = list(bdir.glob("*.sql.gz")) if bdir.exists() else []
        m["backup_age_h"] = (round((time.time() - max(f.stat().st_mtime for f in dumps)) / 3600, 1)
                             if dumps else None)
    except Exception:
        m["backup_age_h"] = None
    try:
        import json as _json
        st = _json.loads(Path("/app/data_gen/state.json").read_text(encoding="utf-8"))
        last = datetime.fromisoformat(st["last_run"])
        m["data_gen_age_h"] = round((time.time() - last.timestamp()) / 3600, 1)
        m["data_gen_errors"] = len(st.get("errors", []))
        m["data_gen_rows"] = sum(int(x) for v in st.get("results", {}).values()
                                 for x in str(v).split("|") if x.isdigit())
    except Exception:
        m["data_gen_age_h"] = None
        m["data_gen_errors"] = None
        m["data_gen_rows"] = None
    for key, path in (("tempo_mb", "/app/tempo/data"), ("prom_mb", "/var/lib/prometheus")):
        try:
            m[key] = int(subprocess.run(["du", "-sm", path],
                                        capture_output=True, text=True, timeout=20).stdout.split()[0])
        except Exception:
            pass
    mem = {}
    for line in _read_file("/proc/meminfo").splitlines():
        k, _, v = line.partition(":")
        if v.strip():
            mem[k] = int(v.strip().split()[0])
    m["mem_pct"] = round(100 * (1 - mem["MemAvailable"] / mem["MemTotal"]), 1)
    m["swap_used_mb"] = (mem.get("SwapTotal", 0) - mem.get("SwapFree", 0)) // 1024
    la = _read_file("/proc/loadavg").split()
    m["load1"], m["load5"] = float(la[0]), float(la[1])
    cpu = list(map(int, _read_file("/proc/stat").splitlines()[0].split()[1:]))
    m["cpu_total"], m["cpu_idle"] = sum(cpu), cpu[3] + cpu[4]
    m["uptime_h"] = round(float(_read_file("/proc/uptime").split()[0]) / 3600, 1)
    m["man_rep"] = proc_stats("man_reporting_service")
    m["cntsadmin"] = proc_stats("CntsAdminCon")
    try:
        out = subprocess.run(["sudo", "-u", "postgres", "psql", "-X", "-t", "-A", "-c",
                              "SELECT pg_database_size('man_reporting')/1048576,"
                              "(SELECT count(*) FROM pg_stat_activity)"],
                             capture_output=True, text=True, timeout=15).stdout.strip().split("|")
        m["pg_db_mb"], m["pg_conns"] = int(out[0]), int(out[1])
    except Exception:
        pass
    try:
        m["pg_dep"] = pg_dependency()
    except Exception:
        m["pg_dep"] = None
    try:
        _, txt = http_text("http://127.0.0.1:5080/metrics", timeout=8)
        vals = {}
        for line in txt.splitlines():
            mm = re.match(r"^(man_reporting_\w+)(?:\{[^}]*\})?\s+([\d.eE+-]+)$", line)
            if mm:
                try:
                    vals[mm.group(1)] = float(mm.group(2))
                except ValueError:
                    pass
        m["app"] = vals
    except Exception:
        m["app"] = None
    return m

def _series(day, getter):
    vals = []
    for h in day:
        try:
            v = getter(h)
            if isinstance(v, (int, float)):
                vals.append(v)
        except Exception:
            pass
    return vals

def metrics_summary():
    """now/min/avg/max по ключевым метрикам за 24 ч + CPU busy за период."""
    hist = STATE.get("metrics_history") or []
    if not hist:
        return None
    now = hist[-1]
    cut = (datetime.now(MSK) - timedelta(hours=24)).isoformat(timespec="seconds")
    day = [h for h in hist if h.get("ts", "") >= cut] or hist

    def agg(getter):
        vals = _series(day, getter)
        if not vals:
            return None
        cur = None
        try:
            cur = getter(now)
        except Exception:
            pass
        return {"now": cur, "min": round(min(vals), 1),
                "avg": round(sum(vals) / len(vals), 1), "max": round(max(vals), 1)}

    cpu_busy = None
    if len(day) >= 2:
        a, b = day[0], day[-1]
        dt, di = b.get("cpu_total", 0) - a.get("cpu_total", 0), b.get("cpu_idle", 0) - a.get("cpu_idle", 0)
        if dt > 0:
            cpu_busy = round(100 * (1 - di / dt), 1)
    # динамика метрик приложений (первое vs последнее значение за сутки)
    app_dyn = {}
    for h in day:
        for k, v in (h.get("app") or {}).items():
            app_dyn.setdefault(k, [None, None])
            if app_dyn[k][0] is None:
                app_dyn[k][0] = v
            app_dyn[k][1] = v
    return {
        "disk_pct": agg(lambda h: h.get("disk_pct")),
        "disk_free_gb": agg(lambda h: h.get("disk_free_gb")),
        "inode_pct": agg(lambda h: h.get("inode_pct")),
        "mem_pct": agg(lambda h: h.get("mem_pct")),
        "swap_used_mb": agg(lambda h: h.get("swap_used_mb")),
        "load1": agg(lambda h: h.get("load1")),
        "cpu_busy_pct_за_период": cpu_busy,
        "uptime_h": now.get("uptime_h"),
        "man_rep_rss_mb": agg(lambda h: (h.get("man_rep") or {}).get("rss_mb")),
        "man_rep_threads": agg(lambda h: (h.get("man_rep") or {}).get("threads")),
        "cntsadmin_rss_mb": agg(lambda h: (h.get("cntsadmin") or {}).get("rss_mb")),
        "pg_db_mb": agg(lambda h: h.get("pg_db_mb")),
        "pg_conns": agg(lambda h: h.get("pg_conns")),
        "data_gen_age_h": agg(lambda h: h.get("data_gen_age_h")),
        "data_gen_rows": agg(lambda h: h.get("data_gen_rows")),
        "data_gen_errors": agg(lambda h: h.get("data_gen_errors")),
        "app_metrics_за_сутки_первое_и_последнее": app_dyn,
    }

# ---------------------------------------------------------------- чтение логов
def read_log_chunk(app):
    """Инкрементальное чтение app.log: возвращает новый кусок или None.

    Ведёт offsets в STATE, переживает ротацию файла.
    """
    path = LOGS[app]
    if not path.exists():
        return None
    size = path.stat().st_size
    off = STATE["log_offsets"].get(app)
    if off is None:
        STATE["log_offsets"][app] = size   # при старте читаем только новое
        return None
    if size < off:                          # ротация
        off = 0
    if size == off:
        return None
    with path.open("r", errors="replace") as f:
        f.seek(off)
        chunk = f.read()
        STATE["log_offsets"][app] = f.tell()
    return chunk

# ---------------------------------------------------------------- deploy smoke-пробы
SMOKE_ENDPOINTS = {
    "man_reporting": ["http://127.0.0.1:5080/health", "http://127.0.0.1:5080/metrics"],
    "cntsadmin": ["http://127.0.0.1:5000/login"],
}

def smoke_test(app, rounds=5):
    """Усиленные пробы после деплоя (практика [140]).

    rounds запросов на каждый ключевой endpoint; возвращает
    (ok, per-endpoint-статистика, p95 латентности).
    """
    results = []
    for url in SMOKE_ENDPOINTS.get(app, []):
        for _ in range(rounds):
            t0 = time.time()
            try:
                st, _ = http_text(url, timeout=10)
            except Exception:
                st = 0
            results.append({"url": url, "status": st, "ms": int((time.time() - t0) * 1000)})
            time.sleep(1)
    if not results:
        return False, {}, None
    per_url = {}
    for r in results:
        d = per_url.setdefault(r["url"], {"ok": 0, "total": 0, "lat": []})
        d["total"] += 1
        d["ok"] += 1 if r["status"] == 200 else 0
        d["lat"].append(r["ms"])
    lats = sorted(r["ms"] for r in results)
    ok = all(r["status"] == 200 for r in results)
    return ok, per_url, lats[int(len(lats) * 0.95) - 1] if lats else None

# ---------------------------------------------------------------- трейсы Tempo
def tempo_traces(minutes=15, limit=5, min_duration_ms=None):
    """Краткие описания трейсов из Tempo за окно. None — Tempo недоступен."""
    end = int(time.time())
    try:
        res = http_json(f"{TEMPO_URL}/api/search?start={end - minutes * 60}&end={end}&limit={limit * 3}",
                        timeout=10)
    except Exception as e:
        print(f"[tempo] {e}", flush=True)
        return None
    out = []
    for t in res.get("traces", []):
        dur = t.get("durationMs")
        if min_duration_ms and (dur or 0) < min_duration_ms:
            continue
        out.append(f"{t.get('rootServiceName', '?')} {t.get('rootTraceName', '?')} — {dur} мс")
        if len(out) >= limit:
            break
    return out

# ---------------------------------------------------------------- ошибки отчётов man_reporting
def report_errors_check():
    """Новые падения отчётов man_reporting_service (по report_last_state в БД).

    Дедупликация по (id, last_runtime) в STATE['report_err_seen']:
    один алерт на каждое конкретное падение; перманентно битый отчёт
    алертует при каждом новом запуске (новый runtime). Лог-алерт на ERROR
    в журнале сервиса при этом может дублировать событие — это осознанно:
    здесь алерт предметный, с именем отчёта и причиной."""
    try:
        out = subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-X", "-t", "-A", "-d", "man_reporting", "-c",
             "SELECT id, report_name, report_last_runtime, left(report_last_state, 200)"
             " FROM centros_monitor_db.man_reporting_settings"
             " WHERE report_enabled AND report_last_state IS NOT NULL"
             " AND report_last_state <> 'Complete'"
             " AND report_last_runtime > now() - interval '2 days'"],
            capture_output=True, text=True, timeout=15).stdout.strip()
    except Exception as e:
        print(f"[report errors] {e}", flush=True)
        return []
    if not out:
        return []
    seen = STATE.setdefault("report_err_seen", {})
    if len(seen) > 500:
        seen.clear()
    new = []
    for line in out.splitlines():
        parts = line.split("|", 3)
        if len(parts) < 4:
            continue
        rid, name, runtime, state = parts
        if seen.get(rid) == runtime:
            continue
        seen[rid] = runtime
        new.append(f"• {name} (id={rid}): {state.splitlines()[0][:180]}")
    return new
