# ==========================================================
# backend/core/rrhh_db.py
#
# Base de datos RRHH — solo PostgreSQL
#
# Variables de entorno (ver .env.example):
#   RRHH_DATABASE_URL  → postgres://user:pass@host:port/db  (URL completa, opcional)
#   RRHH_DB_HOST       → host del servidor (o socket Cloud SQL)
#   RRHH_DB_PORT       → 5432
#   RRHH_DB_NAME       → nombre de la base
#   RRHH_DB_USER       → usuario
#   RRHH_DB_PASSWORD   → contraseña
#
# Período: nombrado por el mes del día 16 de inicio.
#   Ej: "2026-05" = Mayo 2026 = 16/05/2026 → 15/06/2026
# ==========================================================

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import psycopg2.pool

ISO_DT    = "%Y-%m-%d %H:%M:%S"
TIPOS_DIA = ["G", "F", "D", "HO"]

# Líderes principales: ven y aprueban TODO el personal
SUPER_LIDERES = {"5473", "5474", "5477", "5478", "5508"}
TIPOS_NUM = ["HV", "HE"]
TIPOS_ALL = TIPOS_DIA + TIPOS_NUM

# Líderes con visibilidad total (ven todos los empleados sin filtro de equipo)
SUPER_LEADERS = {"5474", "5476", "5477", "5478", "5508"}

MESES_ES = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre",
]


# ==========================================================
# Utilidades de período
# ==========================================================

def period_id(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def period_display(pid: str) -> str:
    y, m = int(pid[:4]), int(pid[5:7])
    return f"{MESES_ES[m-1]} {y}"


def period_bounds(pid: str) -> Tuple[date, date]:
    """Retorna (inicio, fin): inicio = día 16 del mes, fin = día 15 del mes siguiente."""
    y, m = int(pid[:4]), int(pid[5:7])
    start = date(y, m, 16)
    end   = date(y + 1, 1, 15) if m == 12 else date(y, m + 1, 15)
    return start, end


def period_dates(pid: str) -> List[date]:
    start, end = period_bounds(pid)
    result, cur = [], start
    while cur <= end:
        result.append(cur)
        cur += timedelta(days=1)
    return result


def current_period_id() -> str:
    today = date.today()
    if today.day >= 16:
        return period_id(today.year, today.month)
    return period_id(today.year - 1, 12) if today.month == 1 else period_id(today.year, today.month - 1)


def recent_periods(n: int = 8) -> List[Dict]:
    """Retorna los últimos n períodos (más reciente primero)."""
    y, m = map(int, current_period_id().split("-"))
    result = []
    for _ in range(n):
        pid = period_id(y, m)
        start, end = period_bounds(pid)
        result.append({
            "id":      pid,
            "display": period_display(pid),
            "start":   start.isoformat(),
            "end":     end.isoformat(),
        })
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return result


def utcnow_str() -> str:
    return datetime.utcnow().strftime(ISO_DT)


# ==========================================================
# Conexión PostgreSQL
# ==========================================================

def _conn_params() -> Dict[str, Any]:
    url = os.getenv("RRHH_DATABASE_URL", "").strip()
    if url:
        return {"dsn": url}
    host     = os.getenv("RRHH_DB_HOST")
    port     = int(os.getenv("RRHH_DB_PORT", "5432") or "5432")
    name     = os.getenv("RRHH_DB_NAME")
    user     = os.getenv("RRHH_DB_USER")
    password = os.getenv("RRHH_DB_PASSWORD")
    if not name or not user:
        raise RuntimeError(
            "RRHH: configurá RRHH_DATABASE_URL o RRHH_DB_NAME + RRHH_DB_USER en .env"
        )
    params: Dict[str, Any] = {"dbname": name, "user": user, "password": password, "port": port}
    if host:
        params["host"] = host
    return params


_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        params = _conn_params()
        if "dsn" in params:
            _pool = psycopg2.pool.ThreadedConnectionPool(1, 5, params["dsn"])
        else:
            _pool = psycopg2.pool.ThreadedConnectionPool(1, 5, **params)
    return _pool


@contextmanager
def get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ==========================================================
# Migraciones (idempotentes — se corren al startup)
# ==========================================================

def migrate() -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rrhh_personal (
                legajo        TEXT PRIMARY KEY,
                cuil          TEXT NOT NULL,
                nombre        TEXT NOT NULL,
                leader_legajo TEXT NOT NULL,
                funcion       TEXT,
                origen        TEXT,
                lugar_trabajo TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rrhh_partes (
                id                 BIGSERIAL PRIMARY KEY,
                legajo             TEXT NOT NULL,
                periodo            TEXT NOT NULL,
                estado             TEXT NOT NULL
                                   CHECK (estado IN ('BORRADOR','ENVIADO','APROBADO','RECHAZADO')),
                submitted_at       TEXT,
                approved_at        TEXT,
                approved_by_legajo TEXT,
                rejection_comment  TEXT,
                UNIQUE (legajo, periodo)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rrhh_items (
                id         BIGSERIAL PRIMARY KEY,
                legajo     TEXT NOT NULL,
                fecha      TEXT NOT NULL,
                tipo       TEXT NOT NULL
                           CHECK (tipo IN ('G','F','D','HO','HV','HE')),
                valor_num  DOUBLE PRECISION,
                comentario TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_items_lf ON rrhh_items(legajo, fecha)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_partes_estado ON rrhh_partes(estado)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_partes_lp ON rrhh_partes(legajo, periodo)")
        cur.close()


# ==========================================================
# Personal
# ==========================================================

def upsert_personal(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    inserted = updated = 0
    with get_conn() as conn:
        cur = _cursor(conn)
        for r in rows:
            leg = str(r["legajo"]).strip()
            cur.execute("SELECT 1 FROM rrhh_personal WHERE legajo=%s", (leg,))
            exists = cur.fetchone() is not None
            cur.execute("""
                INSERT INTO rrhh_personal
                    (legajo, cuil, nombre, leader_legajo, funcion, origen, lugar_trabajo)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (legajo) DO UPDATE SET
                    cuil          = EXCLUDED.cuil,
                    nombre        = EXCLUDED.nombre,
                    leader_legajo = EXCLUDED.leader_legajo,
                    funcion       = EXCLUDED.funcion,
                    origen        = EXCLUDED.origen,
                    lugar_trabajo = EXCLUDED.lugar_trabajo
            """, (
                leg,
                str(r["cuil"]).strip(),
                str(r["nombre"]).strip(),
                str(r["leader_legajo"]).strip(),
                r.get("funcion"),
                r.get("origen"),
                r.get("lugar_trabajo"),
            ))
            if exists:
                updated += 1
            else:
                inserted += 1
        cur.close()
    return inserted, updated


def list_personal() -> List[Dict]:
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM rrhh_personal ORDER BY nombre")
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows


def get_person(legajo: str) -> Optional[Dict]:
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM rrhh_personal WHERE legajo=%s", (str(legajo).strip(),))
        r = cur.fetchone()
        cur.close()
        return dict(r) if r else None


def get_leader_legajos() -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT leader_legajo
            FROM rrhh_personal
            WHERE leader_legajo IS NOT NULL AND leader_legajo <> ''
        """)
        rows = [str(r[0]) for r in cur.fetchall()]
        cur.close()
        return sorted(rows)


def verify_login(legajo: str, cuil_input: str) -> Tuple[bool, Optional[Dict], str]:
    """Verifica login por legajo + CUIL (completo o últimos 4 dígitos)."""
    legajo     = str(legajo).strip()
    cuil_input = str(cuil_input).strip().replace("-", "").replace(" ", "")

    person = get_person(legajo)
    if not person:
        return False, None, "Legajo no encontrado."

    cuil_stored = str(person.get("cuil", "")).replace("-", "").replace(" ", "")
    if cuil_input == cuil_stored or cuil_input == cuil_stored[-4:]:
        leaders       = get_leader_legajos()
        person["role"] = "lider" if legajo in leaders else "empleado"
        return True, person, ""

    return False, None, "CUIL incorrecto."


# ==========================================================
# Partes
# ==========================================================

def get_or_create_parte(legajo: str, periodo: str) -> Dict:
    legajo, periodo = str(legajo).strip(), str(periodo).strip()
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute("""
            INSERT INTO rrhh_partes (legajo, periodo, estado)
            VALUES (%s, %s, 'BORRADOR')
            ON CONFLICT (legajo, periodo) DO NOTHING
        """, (legajo, periodo))
        cur.execute(
            "SELECT * FROM rrhh_partes WHERE legajo=%s AND periodo=%s",
            (legajo, periodo),
        )
        r = dict(cur.fetchone())
        cur.close()
        return r


def get_parte(legajo: str, periodo: str) -> Optional[Dict]:
    legajo, periodo = str(legajo).strip(), str(periodo).strip()
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute(
            "SELECT * FROM rrhh_partes WHERE legajo=%s AND periodo=%s",
            (legajo, periodo),
        )
        r = cur.fetchone()
        cur.close()
        return dict(r) if r else None


def update_parte_estado(
    legajo: str,
    periodo: str,
    nuevo_estado: str,
    submitted_at:       Optional[str] = None,
    approved_at:        Optional[str] = None,
    approved_by_legajo: Optional[str] = None,
    rejection_comment:  Optional[str] = None,
) -> None:
    legajo, periodo = str(legajo).strip(), str(periodo).strip()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE rrhh_partes SET
                estado             = %s,
                submitted_at       = COALESCE(%s, submitted_at),
                approved_at        = COALESCE(%s, approved_at),
                approved_by_legajo = COALESCE(%s, approved_by_legajo),
                rejection_comment  = %s
            WHERE legajo=%s AND periodo=%s
        """, (
            nuevo_estado, submitted_at, approved_at,
            approved_by_legajo, rejection_comment,
            legajo, periodo,
        ))
        cur.close()


def list_bitacora(legajo: str) -> List[Dict]:
    """Historial de partes de un empleado, más reciente primero."""
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute("""
            SELECT p.*, per.nombre AS approved_by_nombre
            FROM rrhh_partes p
            LEFT JOIN rrhh_personal per ON per.legajo = p.approved_by_legajo
            WHERE p.legajo = %s
            ORDER BY p.periodo DESC
        """, (str(legajo).strip(),))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows


def list_pendientes_lider(leader_legajo: str) -> List[Dict]:
    """Partes ENVIADOS de los empleados a cargo del líder."""
    leader_legajo = str(leader_legajo).strip()
    with get_conn() as conn:
        cur = _cursor(conn)
        if leader_legajo in SUPER_LIDERES:
            cur.execute("""
                SELECT p.legajo, per.nombre, p.periodo, p.estado, p.submitted_at
                FROM rrhh_partes p
                JOIN rrhh_personal per ON per.legajo = p.legajo
                WHERE p.estado = 'ENVIADO'
                ORDER BY p.submitted_at DESC NULLS LAST, per.nombre
            """)
        else:
            cur.execute("""
                SELECT p.legajo, per.nombre, p.periodo, p.estado, p.submitted_at
                FROM rrhh_partes p
                JOIN rrhh_personal per ON per.legajo = p.legajo
                WHERE p.estado = 'ENVIADO' AND per.leader_legajo = %s
                ORDER BY p.submitted_at DESC NULLS LAST, per.nombre
            """, (leader_legajo,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows


def list_team_partes(leader_legajo: str, periodo: Optional[str] = None) -> List[Dict]:
    """Todos los partes del equipo del líder, opcionalmente filtrado por período."""
    leader_legajo = str(leader_legajo).strip()
    with get_conn() as conn:
        cur = _cursor(conn)
        if leader_legajo in SUPER_LIDERES:
            if periodo:
                cur.execute("""
                    SELECT p.legajo, per.nombre, per.funcion, p.periodo,
                           p.estado, p.submitted_at, p.approved_at, p.rejection_comment
                    FROM rrhh_partes p
                    JOIN rrhh_personal per ON per.legajo = p.legajo
                    WHERE p.periodo = %s
                    ORDER BY per.nombre
                """, (periodo,))
            else:
                cur.execute("""
                    SELECT p.legajo, per.nombre, per.funcion, p.periodo,
                           p.estado, p.submitted_at, p.approved_at, p.rejection_comment
                    FROM rrhh_partes p
                    JOIN rrhh_personal per ON per.legajo = p.legajo
                    ORDER BY p.periodo DESC, per.nombre
                """)
        else:
            if periodo:
                cur.execute("""
                    SELECT p.legajo, per.nombre, per.funcion, p.periodo,
                           p.estado, p.submitted_at, p.approved_at, p.rejection_comment
                    FROM rrhh_partes p
                    JOIN rrhh_personal per ON per.legajo = p.legajo
                    WHERE per.leader_legajo = %s AND p.periodo = %s
                    ORDER BY per.nombre
                """, (leader_legajo, periodo))
            else:
                cur.execute("""
                    SELECT p.legajo, per.nombre, per.funcion, p.periodo,
                           p.estado, p.submitted_at, p.approved_at, p.rejection_comment
                    FROM rrhh_partes p
                    JOIN rrhh_personal per ON per.legajo = p.legajo
                    WHERE per.leader_legajo = %s
                    ORDER BY p.periodo DESC, per.nombre
                """, (leader_legajo,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows


# ==========================================================
# Items (grilla de días)
# ==========================================================

def list_items(legajo: str, periodo: str) -> List[Dict]:
    legajo, periodo = str(legajo).strip(), str(periodo).strip()
    start, end = period_bounds(periodo)
    with get_conn() as conn:
        cur = _cursor(conn)
        cur.execute("""
            SELECT * FROM rrhh_items
            WHERE legajo=%s AND fecha>=%s AND fecha<=%s
            ORDER BY fecha, tipo
        """, (legajo, start.isoformat(), end.isoformat()))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows


def save_items(legajo: str, periodo: str, items: List[Dict]) -> None:
    """Reemplaza todos los items del período en una sola transacción."""
    legajo, periodo = str(legajo).strip(), str(periodo).strip()
    start, end = period_bounds(periodo)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM rrhh_items WHERE legajo=%s AND fecha>=%s AND fecha<=%s",
            (legajo, start.isoformat(), end.isoformat()),
        )
        if items:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO rrhh_items (legajo, fecha, tipo, valor_num, comentario)
                VALUES (%s, %s, %s, %s, %s)
            """, [
                (legajo, it["fecha"], it["tipo"], it.get("valor_num"), it.get("comentario"))
                for it in items
            ], page_size=500)
        cur.close()


# ==========================================================
# Consolidado
# ==========================================================

def get_consolidado(leader_legajo: str, periodo: str) -> List[Dict]:
    """
    Retorna resumen + detalle por día para cada empleado del equipo.
    Una sola query para empleados + una sola query para todos los items (sin N+1).
    [{legajo, nombre, funcion, estado, G, F, D, HO, HV, HE, dias:[...]}]
    """
    leader_legajo, periodo = str(leader_legajo).strip(), str(periodo).strip()
    start, end = period_bounds(periodo)

    with get_conn() as conn:
        cur = _cursor(conn)

        # 1 query: todos los empleados del equipo
        if leader_legajo in SUPER_LIDERES:
            cur.execute("""
                SELECT per.legajo, per.nombre, per.funcion,
                       p.estado, p.approved_at
                FROM rrhh_personal per
                LEFT JOIN rrhh_partes p
                    ON p.legajo = per.legajo AND p.periodo = %s
                ORDER BY per.nombre
            """, (periodo,))
        else:
            cur.execute("""
                SELECT per.legajo, per.nombre, per.funcion,
                       p.estado, p.approved_at
                FROM rrhh_personal per
                LEFT JOIN rrhh_partes p
                    ON p.legajo = per.legajo AND p.periodo = %s
                WHERE per.leader_legajo = %s
                ORDER BY per.nombre
            """, (periodo, leader_legajo))
        empleados = [dict(r) for r in cur.fetchall()]

        # 1 query: todos los items del período para todo el equipo
        legajos = [e["legajo"] for e in empleados]
        items_all: List[Dict] = []
        if legajos:
            cur.execute("""
                SELECT * FROM rrhh_items
                WHERE legajo = ANY(%s) AND fecha >= %s AND fecha <= %s
                ORDER BY legajo, fecha, tipo
            """, (legajos, start.isoformat(), end.isoformat()))
            items_all = [dict(r) for r in cur.fetchall()]
        cur.close()

    # Agrupar items por legajo en memoria
    items_by_legajo: Dict[str, List[Dict]] = {e["legajo"]: [] for e in empleados}
    for it in items_all:
        leg = str(it["legajo"])
        if leg in items_by_legajo:
            items_by_legajo[leg].append(it)

    result = []
    for emp in empleados:
        items = items_by_legajo.get(emp["legajo"], [])

        by_date: Dict[str, Dict] = {}
        for it in items:
            f = str(it["fecha"])
            if f not in by_date:
                by_date[f] = {"fecha": f, "tipos": [], "HV": 0.0, "HE": 0.0, "comentario": ""}
            t = it["tipo"]
            if t in TIPOS_DIA:
                by_date[f]["tipos"].append(t)
            elif t == "HV":
                by_date[f]["HV"] += float(it.get("valor_num") or 0)
            elif t == "HE":
                by_date[f]["HE"] += float(it.get("valor_num") or 0)
            if it.get("comentario") and not by_date[f]["comentario"]:
                by_date[f]["comentario"] = it["comentario"]

        dias = sorted(by_date.values(), key=lambda x: x["fecha"])

        totales: Dict[str, Any] = {t: 0 for t in TIPOS_DIA}
        totales["HV"] = totales["HE"] = 0.0
        for it in items:
            t = it["tipo"]
            if t in TIPOS_DIA:
                totales[t] += 1
            elif t in ("HV", "HE"):
                totales[t] = round(totales[t] + float(it.get("valor_num") or 0), 2)

        result.append({
            "legajo":      emp["legajo"],
            "nombre":      emp["nombre"],
            "funcion":     emp.get("funcion") or "",
            "estado":      emp.get("estado") or "SIN PARTE",
            "approved_at": emp.get("approved_at"),
            **totales,
            "dias": dias,
        })
    return result
