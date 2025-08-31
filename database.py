# database.py — Postgres (psycopg2) multi-usuario por "owner"
import os
import time
import bcrypt
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, IntegrityError

# ---------------------------------
# Conexión (compatible con Supabase)
# ---------------------------------
def connect_db():
    url = os.getenv("DATABASE_URL")
    if not url:
        try:
            import streamlit as st  # opcional, si se ejecuta en Streamlit
            url = st.secrets["DATABASE_URL"]
        except Exception:
            raise RuntimeError(
                "DATABASE_URL no está configurada en variables de entorno ni en st.secrets."
            )
    url = url.strip()
    if "sslmode=" not in url:
        url = url + ("&sslmode=require" if "?" in url else "?sslmode=require")
    try:
        return psycopg2.connect(url)
    except Exception as e:
        # Ocultar credenciales en el mensaje
        try:
            before_at, after_at = url.split("@", 1)
            safe_url = "postgresql://postgres:***@" + after_at
        except Exception:
            safe_url = "postgresql://postgres:***@<host>:<port>/<db>"
        raise RuntimeError(f"No pude conectar a Postgres con DSN={safe_url}. Detalle: {e}")

# -----------------
# Tablas / Migración
# -----------------
def create_users_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              username TEXT PRIMARY KEY,
              password TEXT NOT NULL,
              created_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_trabajadores_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trabajadores (
              id SERIAL PRIMARY KEY,
              owner TEXT NOT NULL,
              nombre TEXT NOT NULL,
              apellido TEXT NOT NULL,
              created_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trabajadores_owner ON trabajadores(owner);")
        # Único por owner+nombre+apellido para evitar duplicados
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_trabajadores_owner_nombre_apellido
            ON trabajadores(owner, nombre, apellido);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_jornadas_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jornadas (
              id SERIAL PRIMARY KEY,
              owner TEXT NOT NULL,
              trabajador TEXT NOT NULL,
              fecha DATE NOT NULL,
              lote TEXT,
              actividad TEXT,
              dias INTEGER NOT NULL DEFAULT 0,
              horas_normales NUMERIC NOT NULL DEFAULT 0,
              horas_extra NUMERIC NOT NULL DEFAULT 0
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_owner ON jornadas(owner);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_fecha ON jornadas(fecha);")
        conn.commit()
    finally:
        conn.close()


def create_insumos_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS insumos (
              id SERIAL PRIMARY KEY,
              owner TEXT NOT NULL,
              fecha DATE,
              lote TEXT,
              tipo TEXT,
              etapa TEXT,       -- plaga/control o tipo o etapa
              producto TEXT,
              dosis TEXT,
              cantidad NUMERIC,
              precio_unitario NUMERIC,
              costo_total NUMERIC
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_owner ON insumos(owner);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_tipo ON insumos(tipo);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_fecha ON insumos(fecha);")
        conn.commit()
    finally:
        conn.close()


# ---------- Tarifas por usuario ----------
def create_tarifas_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tarifas_user (
              owner TEXT PRIMARY KEY,
              pago_dia NUMERIC NOT NULL,
              pago_hora_extra NUMERIC NOT NULL,
              updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        # Legacy opcional
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tarifas (
              id INTEGER PRIMARY KEY,
              pago_dia NUMERIC NOT NULL,
              pago_hora_extra NUMERIC NOT NULL,
              updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        cur.execute(
            """
            INSERT INTO tarifas (id, pago_dia, pago_hora_extra)
            VALUES (1, 9000, 2000)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_tarifas(owner: str):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("SELECT pago_dia, pago_hora_extra FROM tarifas_user WHERE owner=%s;", (owner,))
        row = cur.fetchone()
        if row:
            return float(row[0]), float(row[1])
        # Copiar de legacy si existe
        cur.execute("SELECT pago_dia, pago_hora_extra FROM tarifas WHERE id=1;")
        legacy = cur.fetchone()
        if legacy:
            cur.execute(
                """
                INSERT INTO tarifas_user (owner, pago_dia, pago_hora_extra, updated_at)
                VALUES (%s,%s,%s, now())
                ON CONFLICT (owner) DO NOTHING;
                """,
                (owner, legacy[0], legacy[1]),
            )
            conn.commit()
            return float(legacy[0]), float(legacy[1])
        # Defaults
        cur.execute(
            """
            INSERT INTO tarifas_user (owner, pago_dia, pago_hora_extra) VALUES (%s,%s,%s)
            ON CONFLICT (owner) DO NOTHING;
            """,
            (owner, 9000, 2000),
        )
        conn.commit()
        return 9000.0, 2000.0
    finally:
        conn.close()


def set_tarifas(owner: str, pago_dia: float, pago_hora_extra: float):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tarifas_user (owner, pago_dia, pago_hora_extra, updated_at)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (owner) DO UPDATE
            SET pago_dia=EXCLUDED.pago_dia,
                pago_hora_extra=EXCLUDED.pago_hora_extra,
                updated_at=now();
            """,
            (owner, pago_dia, pago_hora_extra),
        )
        conn.commit()
    finally:
        conn.close()


# ---------- Cierres mensuales ----------
def create_cierres_tables():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pagos_mes (
              id SERIAL PRIMARY KEY,
              owner TEXT NOT NULL,
              mes_ini DATE NOT NULL,
              mes_fin DATE NOT NULL,
              creado_por TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              tarifa_dia NUMERIC NOT NULL,
              tarifa_hora_extra NUMERIC NOT NULL,
              total_nomina NUMERIC NOT NULL DEFAULT 0,
              total_insumos NUMERIC NOT NULL DEFAULT 0,
              total_general NUMERIC NOT NULL DEFAULT 0
            );
            """
        )
        # Unicidad por owner+rango de mes (índice en vez de UNIQUE por compatibilidad)
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pagos_mes_owner_rango
            ON pagos_mes(owner, mes_ini, mes_fin);
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pagos_mes_owner ON pagos_mes(owner);")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pagos_mes_nomina (
              id SERIAL PRIMARY KEY,
              pago_id INTEGER NOT NULL REFERENCES pagos_mes(id) ON DELETE CASCADE,
              trabajador TEXT NOT NULL,
              dias INTEGER NOT NULL DEFAULT 0,
              horas_extra NUMERIC NOT NULL DEFAULT 0,
              monto_dias NUMERIC NOT NULL DEFAULT 0,
              monto_hex NUMERIC NOT NULL DEFAULT 0,
              total NUMERIC NOT NULL DEFAULT 0
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pagos_mes_insumos (
              id SERIAL PRIMARY KEY,
              pago_id INTEGER NOT NULL REFERENCES pagos_mes(id) ON DELETE CASCADE,
              fecha DATE,
              lote TEXT,
              tipo TEXT,
              producto TEXT,
              etapa TEXT,
              dosis TEXT,
              cantidad NUMERIC,
              precio_unitario NUMERIC,
              costo_total NUMERIC
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def ensure_cierres_schema():
    """Idempotente: asegura columnas/índices esperados en versiones antiguas."""
    conn = connect_db(); cur = conn.cursor()
    try:
        # owner en tablas antiguas + índices
        for t in ("trabajadores", "jornadas", "insumos"):
            cur.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS owner TEXT;")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_owner ON {t}(owner);")
        # índice único para trabajadores
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_trabajadores_owner_nombre_apellido
            ON trabajadores(owner, nombre, apellido);
            """
        )
        # columnas totales en pagos_mes y unicidad por rango
        cur.execute(
            """
            ALTER TABLE pagos_mes
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ADD COLUMN IF NOT EXISTS tarifa_dia NUMERIC NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS tarifa_hora_extra NUMERIC NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_nomina NUMERIC NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_insumos NUMERIC NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_general NUMERIC NOT NULL DEFAULT 0;
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pagos_mes_owner_rango
            ON pagos_mes(owner, mes_ini, mes_fin);
            """
        )
        conn.commit()
    finally:
        conn.close()


# -------------
# Autenticación
# -------------
def add_user(username, raw_password):
    hashed = bcrypt.hashpw(raw_password.encode(), bcrypt.gensalt()).decode()
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users (username, password) VALUES (%s, %s);", (username, hashed))
        conn.commit()
    finally:
        conn.close()


def verify_user(username, raw_password):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("SELECT password FROM users WHERE username=%s;", (username,))
        row = cur.fetchone()
        if not row:
            return False
        return bcrypt.checkpw(raw_password.encode(), row[0].encode())
    finally:
        conn.close()


# -------------
# Trabajadores
# -------------
def add_trabajador(nombre, apellido, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO trabajadores (owner, nombre, apellido) VALUES (%s,%s,%s)
            ON CONFLICT (owner, nombre, apellido) DO NOTHING;
            """,
            (owner, nombre, apellido),
        )
        conn.commit()
        # rowcount==0 => ya existía
        return cur.rowcount > 0
    except IntegrityError:
        conn.rollback()
        return False
    finally:
        conn.close()


def get_all_trabajadores(owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT nombre || ' ' || apellido FROM trabajadores WHERE owner=%s ORDER BY nombre, apellido;",
            (owner,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# --------
# Jornadas
# --------
def add_jornada(trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO jornadas (owner, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (owner, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_jornadas(owner):
    """Devuelve 8 columnas SIN owner para que coincida con la UI:
    (ID, Trabajador, Fecha, Lote, Actividad, Días, Horas Normales, Horas Extra)
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra
            FROM jornadas
            WHERE owner=%s
            ORDER BY fecha DESC, id DESC;
            """,
            (owner,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_last_jornada_by_date(fecha, owner):
    """Devuelve 9 columnas CON owner para edición detallada:
    (id, owner, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra)
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, owner, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra
            FROM jornadas
            WHERE owner=%s AND fecha=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (owner, fecha),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_jornada(id_j, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE jornadas
            SET trabajador=%s, fecha=%s, lote=%s, actividad=%s, dias=%s, horas_normales=%s, horas_extra=%s
            WHERE id=%s AND owner=%s;
            """,
            (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, id_j, owner),
        )
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# Insumos (Abono/Fumi/Cal/Herbi)
# -----------------------------
def add_insumo(fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO insumos (owner, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (owner, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total),
        )
        conn.commit()
    finally:
        conn.close()


# Los "get_last_*_by_date" DEVUELVEN SIEMPRE 10 columnas SIN owner en este orden:
# (id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total)

def get_last_abono_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND tipo='Abono' AND fecha=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (owner, fecha),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_abono(id_i, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s AND tipo='Abono';
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id_i, owner),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_fumigacion_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND tipo='Fumigación' AND fecha=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (owner, fecha),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_fumigacion(id_i, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s AND tipo='Fumigación';
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id_i, owner),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_cal_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND tipo='Cal' AND fecha=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (owner, fecha),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_cal(id_i, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo='Cal', etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s AND tipo='Cal';
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id_i, owner),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_herbicida_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND tipo='Herbicida' AND fecha=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (owner, fecha),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_herbicida(id_i, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo='Herbicida', etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s AND tipo='Herbicida';
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id_i, owner),
        )
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# Consultas por rango (con retry)
# -----------------------------
def get_jornadas_between(fecha_ini, fecha_fin, owner):
    """Devuelve 8 columnas SIN owner para la UI de cierres."""
    def _run():
        conn = connect_db(); cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra
                FROM jornadas
                WHERE owner=%s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC, id DESC;
                """,
                (owner, fecha_ini, fecha_fin),
            )
            return cur.fetchall()
        finally:
            conn.close()
    try:
        return _run()
    except OperationalError:
        time.sleep(0.8)
        return _run()


def get_insumos_between(fecha_ini, fecha_fin, owner):
    def _run():
        conn = connect_db(); cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
                FROM insumos
                WHERE owner=%s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC, id DESC;
                """,
                (owner, fecha_ini, fecha_fin),
            )
            return cur.fetchall()
        finally:
            conn.close()
    try:
        return _run()
    except OperationalError:
        time.sleep(0.8)
        return _run()


# ---------------------------------------------
# Cierres (crear / listar / leer detalle)
# ---------------------------------------------
def crear_cierre_mensual(mes_ini, mes_fin, creado_por, owner, tarifa_dia, tarifa_hora_extra, overwrite=False):
    conn = connect_db(); cur = conn.cursor()
    try:
        # ¿Existe?
        cur.execute(
            "SELECT id FROM pagos_mes WHERE owner=%s AND mes_ini=%s AND mes_fin=%s;",
            (owner, mes_ini, mes_fin),
        )
        row = cur.fetchone()
        if row and not overwrite:
            raise ValueError("Ya existe un cierre para ese mes. Activa 'Sobrescribir' si quieres recrearlo.")
        if row and overwrite:
            cur.execute(
                "DELETE FROM pagos_mes WHERE owner=%s AND mes_ini=%s AND mes_fin=%s;",
                (owner, mes_ini, mes_fin),
            )
            conn.commit()

        # Nómina por trabajador
        cur.execute(
            """
            SELECT trabajador, COALESCE(SUM(dias),0) AS dias, COALESCE(SUM(horas_extra),0) AS horas_extra
            FROM jornadas
            WHERE owner=%s AND fecha BETWEEN %s AND %s
            GROUP BY trabajador
            ORDER BY trabajador;
            """,
            (owner, mes_ini, mes_fin),
        )
        nomina_rows = cur.fetchall()

        total_nomina = 0.0
        detalle_nomina = []
        for trab, dias, hextra in nomina_rows:
            monto_dias = (dias or 0) * float(tarifa_dia)
            monto_hex  = (hextra or 0) * float(tarifa_hora_extra)
            total      = (monto_dias or 0) + (monto_hex or 0)
            total_nomina += float(total)
            detalle_nomina.append(
                (trab, int(dias or 0), float(hextra or 0), float(monto_dias or 0), float(monto_hex or 0), float(total or 0))
            )

        # Insumos del rango
        cur.execute(
            """
            SELECT fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND fecha BETWEEN %s AND %s
            ORDER BY fecha, id;
            """,
            (owner, mes_ini, mes_fin),
        )
        insumos_rows = cur.fetchall()

        total_insumos = sum(float(r[-1] or 0) for r in insumos_rows)
        total_general = float(total_nomina) + float(total_insumos)

        # Cabecera
        cur.execute(
            """
            INSERT INTO pagos_mes
                (owner, mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra, total_nomina, total_insumos, total_general)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
            """,
            (owner, mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra, total_nomina, total_insumos, total_general),
        )
        pago_id = cur.fetchone()[0]

        # Detalle nómina
        if detalle_nomina:
            execute_values(
                cur,
                """
                INSERT INTO pagos_mes_nomina
                  (pago_id, trabajador, dias, horas_extra, monto_dias, monto_hex, total)
                VALUES %s;
                """,
                [(pago_id, *row) for row in detalle_nomina],
            )

        # Detalle insumos
        if insumos_rows:
            execute_values(
                cur,
                """
                INSERT INTO pagos_mes_insumos
                  (pago_id, fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total)
                VALUES %s;
                """,
                [(pago_id, *r) for r in insumos_rows],
            )

        conn.commit()
        return pago_id
    finally:
        conn.close()


def listar_cierres(owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, mes_ini, mes_fin, creado_por, created_at, total_nomina, total_insumos, total_general
            FROM pagos_mes
            WHERE owner=%s
            ORDER BY mes_ini DESC;
            """,
            (owner,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def leer_cierre_detalle(pago_id, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        # Validar pertenencia
        cur.execute("SELECT 1 FROM pagos_mes WHERE id=%s AND owner=%s;", (pago_id, owner))
        if not cur.fetchone():
            return [], []
        cur.execute(
            """
            SELECT trabajador, dias, horas_extra, monto_dias, monto_hex, total
            FROM pagos_mes_nomina
            WHERE pago_id=%s
            ORDER BY trabajador;
            """,
            (pago_id,),
        )
        nomina = cur.fetchall()

        cur.execute(
            """
            SELECT fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total
            FROM pagos_mes_insumos
            WHERE pago_id=%s
            ORDER BY fecha, id;
            """,
            (pago_id,),
        )
        insumos = cur.fetchall()
        return nomina, insumos
    finally:
        conn.close()



















