# database.py — Supabase (Postgres) con psycopg2 + bcrypt — multiusuario (owner)
import os
import time
import bcrypt
import psycopg2
from psycopg2 import OperationalError, IntegrityError
from psycopg2.extras import execute_values

# ==========================
# Conexión
# ==========================
def connect_db():
    """
    Lee DATABASE_URL (env o st.secrets), fuerza sslmode=require si falta,
    y devuelve una conexión psycopg2.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        try:
            import streamlit as st
            url = st.secrets["DATABASE_URL"]
        except Exception:
            raise RuntimeError("DATABASE_URL no está configurada en variables de entorno ni en st.secrets.")

    url = url.strip()
    if "sslmode=" not in url:
        url = url + ("&sslmode=require" if "?" in url else "?sslmode=require")

    try:
        return psycopg2.connect(url)
    except Exception as e:
        # Sanitiza la URL (oculta la contraseña) para logs
        try:
            _, after_at = url.split("@", 1)
            safe_url = "postgresql://postgres:***@" + after_at
        except Exception:
            safe_url = "postgresql://postgres:***@<host>:<port>/<db>"
        raise RuntimeError(f"No pude conectar a Postgres con DSN={safe_url}. Detalle: {e}")

# ==========================
# Creación de tablas base (idempotente)
# ==========================
def create_users_table():
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        conn.commit()
    finally:
        conn.close()

def create_trabajadores_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trabajadores (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                apellido TEXT NOT NULL,
                owner TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (owner, nombre, apellido)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trabajadores_owner ON trabajadores(owner);")
        conn.commit()
    finally:
        conn.close()

def create_jornadas_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jornadas (
                id SERIAL PRIMARY KEY,
                trabajador TEXT NOT NULL,
                fecha DATE NOT NULL,
                lote TEXT,
                actividad TEXT,
                dias INTEGER NOT NULL DEFAULT 0,
                horas_normales NUMERIC NOT NULL DEFAULT 0,
                horas_extra NUMERIC NOT NULL DEFAULT 0,
                owner TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_owner ON jornadas(owner);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_fecha ON jornadas(fecha);")
        conn.commit()
    finally:
        conn.close()

def create_insumos_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insumos (
                id SERIAL PRIMARY KEY,
                fecha DATE NOT NULL,
                lote TEXT,
                tipo TEXT NOT NULL,      -- Abono / Fumigación / Cal / Herbicida
                etapa TEXT,              -- etapa / plaga / tipo cal / tipo herbicida
                producto TEXT,
                dosis TEXT,
                cantidad NUMERIC,
                precio_unitario NUMERIC,
                costo_total NUMERIC,
                owner TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_owner ON insumos(owner);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_tipo ON insumos(tipo);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insumos_fecha ON insumos(fecha);")
        conn.commit()
    finally:
        conn.close()

# ==========================
# Usuarios (login con bcrypt)
# ==========================
def add_user(username, raw_password):
    """
    Crea usuario con password hasheada (bcrypt).
    """
    hashed = bcrypt.hashpw(raw_password.encode(), bcrypt.gensalt()).decode()
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users (username, password) VALUES (%s, %s);", (username, hashed))
        conn.commit()
    finally:
        conn.close()

def verify_user(username, raw_password):
    """
    Verifica usuario y contraseña (bcrypt). Retorna True/False.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("SELECT password FROM users WHERE username = %s;", (username,))
        row = cur.fetchone()
        if not row:
            return False
        hashed = row[0]
        return bcrypt.checkpw(raw_password.encode(), hashed.encode())
    finally:
        conn.close()

# ==========================
# Trabajadores (multiusuario)
# ==========================
def add_trabajador(nombre, apellido, owner):
    """
    Inserta trabajador para un owner. UNIQUE(owner, nombre, apellido).
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO trabajadores (nombre, apellido, owner)
            VALUES (%s, %s, %s)
            ON CONFLICT (owner, nombre, apellido) DO NOTHING;
        """, (nombre, apellido, owner))
        conn.commit()
        return True
    except IntegrityError:
        conn.rollback()
        return None
    finally:
        conn.close()

def get_all_trabajadores(owner):
    """
    Retorna lista de 'Nombre Apellido' del owner, ordenada por nombre.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT nombre || ' ' || apellido
            FROM trabajadores
            WHERE owner=%s
            ORDER BY nombre;
        """, (owner,))
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

# ==========================
# Jornadas (multiusuario)
# ==========================
def add_jornada(trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO jornadas (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
        """, (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner))
        conn.commit()
    finally:
        conn.close()

def get_all_jornadas(owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM jornadas WHERE owner=%s ORDER BY fecha DESC, id DESC;", (owner,))
        return cur.fetchall()
    finally:
        conn.close()

def get_last_jornada_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM jornadas
            WHERE fecha=%s AND owner=%s
            ORDER BY id DESC
            LIMIT 1;
        """, (fecha, owner))
        return cur.fetchone()
    finally:
        conn.close()

def update_jornada(id, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE jornadas
            SET trabajador=%s, fecha=%s, lote=%s, actividad=%s, dias=%s, horas_normales=%s, horas_extra=%s
            WHERE id=%s AND owner=%s;
        """, (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, id, owner))
        conn.commit()
    finally:
        conn.close()

# ==========================
# Insumos (multiusuario)
# ==========================
def add_insumo(fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO insumos (fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total, owner)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total, owner))
        conn.commit()
    finally:
        conn.close()

def get_insumos_by_tipo(tipo, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE tipo=%s AND owner=%s
            ORDER BY fecha DESC, id DESC;
        """, (tipo, owner))
        return cur.fetchall()
    finally:
        conn.close()

# --- helpers para "último por fecha" (multiusuario) ---
def get_last_abono_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM insumos
            WHERE tipo='Abono' AND fecha=%s AND owner=%s
            ORDER BY id DESC LIMIT 1;
        """, (fecha, owner))
        return cur.fetchone()
    finally:
        conn.close()

def update_abono(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s;
        """, (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id, owner))
        conn.commit()
    finally:
        conn.close()

def get_last_fumigacion_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM insumos
            WHERE tipo='Fumigación' AND fecha=%s AND owner=%s
            ORDER BY id DESC LIMIT 1;
        """, (fecha, owner))
        return cur.fetchone()
    finally:
        conn.close()

def update_fumigacion(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s;
        """, (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id, owner))
        conn.commit()
    finally:
        conn.close()

def get_last_cal_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM insumos
            WHERE tipo='Cal' AND fecha=%s AND owner=%s
            ORDER BY id DESC LIMIT 1;
        """, (fecha, owner))
        return cur.fetchone()
    finally:
        conn.close()

def update_cal(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s;
        """, (fecha, lote, "Cal", etapa, producto, dosis, cantidad, precio_unitario, costo_total, id, owner))
        conn.commit()
    finally:
        conn.close()

def get_last_herbicida_by_date(fecha, owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM insumos
            WHERE tipo='Herbicida' AND fecha=%s AND owner=%s
            ORDER BY id DESC LIMIT 1;
        """, (fecha, owner))
        return cur.fetchone()
    finally:
        conn.close()

def update_herbicida(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, owner):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s AND owner=%s;
        """, (fecha, lote, "Herbicida", etapa, producto, dosis, cantidad, precio_unitario, costo_total, id, owner))
        conn.commit()
    finally:
        conn.close()

# ==========================
# TARIFAS por usuario (persisten en Supabase)
# ==========================
def create_tarifas_table():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tarifas (
                owner TEXT PRIMARY KEY,
                pago_dia NUMERIC NOT NULL DEFAULT 9000,
                pago_hora_extra NUMERIC NOT NULL DEFAULT 2000,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        conn.commit()
    finally:
        conn.close()

def get_tarifas(owner):
    """
    Devuelve (pago_dia, pago_hora_extra) como floats para el owner.
    Si no existe, crea defaults y retorna.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("SELECT pago_dia, pago_hora_extra FROM tarifas WHERE owner=%s;", (owner,))
        row = cur.fetchone()
        if row:
            return float(row[0]), float(row[1])
        # crear con defaults
        cur.execute("""
            INSERT INTO tarifas (owner, pago_dia, pago_hora_extra)
            VALUES (%s, %s, %s)
            ON CONFLICT (owner) DO NOTHING;
        """, (owner, 9000, 2000))
        conn.commit()
        return 9000.0, 2000.0
    finally:
        conn.close()

def set_tarifas(owner, pago_dia, pago_hora_extra):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO tarifas (owner, pago_dia, pago_hora_extra, updated_at)
            VALUES (%s,%s,%s, now())
            ON CONFLICT (owner) DO UPDATE
            SET pago_dia=EXCLUDED.pago_dia,
                pago_hora_extra=EXCLUDED.pago_hora_extra,
                updated_at=now();
        """, (owner, pago_dia, pago_hora_extra))
        conn.commit()
    finally:
        conn.close()

# ==========================
# CIERRES MENSUALES (multiusuario)
# ==========================
def create_cierres_tables():
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
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
                total_general NUMERIC NOT NULL DEFAULT 0,
                UNIQUE (owner, mes_ini, mes_fin)
            );
        """)
        cur.execute("""
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
        """)
        cur.execute("""
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
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pagos_mes_owner ON pagos_mes(owner);")
        conn.commit()
    finally:
        conn.close()

# ------- helpers robustos (reintentos) -------
def _run_with_retry(fn, retries=1, delay=0.8):
    try:
        return fn()
    except OperationalError:
        if retries <= 0:
            raise
        time.sleep(delay)
        return fn()

# ------- consultas por rango (filtradas por owner) -------
def get_jornadas_between(fecha_ini, fecha_fin, owner):
    def _do():
        conn = connect_db(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT * FROM jornadas
                WHERE owner=%s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC, id DESC;
            """, (owner, fecha_ini, fecha_fin))
            return cur.fetchall()
        finally:
            conn.close()
    return _run_with_retry(_do)

def get_insumos_between(fecha_ini, fecha_fin, owner):
    def _do():
        conn = connect_db(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, fecha, lote, tipo, etapa, producto, dosis,
                       cantidad, precio_unitario, costo_total
                FROM insumos
                WHERE owner=%s AND fecha BETWEEN %s AND %s
                ORDER BY fecha DESC, id DESC;
            """, (owner, fecha_ini, fecha_fin))
            return cur.fetchall()
        finally:
            conn.close()
    return _run_with_retry(_do)

def crear_cierre_mensual(mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra, owner, overwrite=False):
    """
    Crea snapshot mensual (nómina + insumos) para un owner.
    Retorna pago_id.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        # ¿Existe ya?
        cur.execute("SELECT id FROM pagos_mes WHERE owner=%s AND mes_ini=%s AND mes_fin=%s;",
                    (owner, mes_ini, mes_fin))
        row = cur.fetchone()
        if row and not overwrite:
            raise ValueError("Ya existe un cierre para ese mes. Activa 'Sobrescribir' para recrearlo.")
        if row and overwrite:
            cur.execute("DELETE FROM pagos_mes WHERE owner=%s AND mes_ini=%s AND mes_fin=%s;",
                        (owner, mes_ini, mes_fin))
            conn.commit()

        # Nómina agregada por trabajador
        cur.execute("""
            SELECT trabajador,
                   COALESCE(SUM(dias),0) AS dias,
                   COALESCE(SUM(horas_extra),0) AS horas_extra
            FROM jornadas
            WHERE owner=%s AND fecha BETWEEN %s AND %s
            GROUP BY trabajador
            ORDER BY trabajador;
        """, (owner, mes_ini, mes_fin))
        nomina_rows = cur.fetchall()

        total_nomina = 0.0
        detalle_nomina = []
        for trab, dias, hextra in nomina_rows:
            monto_dias = (dias or 0) * float(tarifa_dia)
            monto_hex  = float(hextra or 0) * float(tarifa_hora_extra)
            total      = float(monto_dias) + float(monto_hex)
            total_nomina += total
            detalle_nomina.append((trab, int(dias or 0), float(hextra or 0),
                                   float(monto_dias), float(monto_hex), float(total)))

        # Insumos del rango
        cur.execute("""
            SELECT fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE owner=%s AND fecha BETWEEN %s AND %s
            ORDER BY fecha, id;
        """, (owner, mes_ini, mes_fin))
        insumos_rows = cur.fetchall()

        total_insumos = sum(float(r[-1] or 0) for r in insumos_rows)
        total_general = float(total_nomina) + float(total_insumos)

        # Cabecera
        cur.execute("""
            INSERT INTO pagos_mes
                (owner, mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra,
                 total_nomina, total_insumos, total_general)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
        """, (owner, mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra,
              total_nomina, total_insumos, total_general))
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
                [(pago_id, *row) for row in detalle_nomina]
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
                [(pago_id, *r) for r in insumos_rows]
            )

        conn.commit()
        return pago_id
    finally:
        conn.close()

def listar_cierres(owner):
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, mes_ini, mes_fin, creado_por, created_at, total_nomina, total_insumos, total_general
            FROM pagos_mes
            WHERE owner=%s
            ORDER BY mes_ini DESC;
        """, (owner,))
        return cur.fetchall()
    finally:
        conn.close()

def leer_cierre_detalle(pago_id, owner):
    """
    Devuelve (nomina_rows, insumo_rows) del pago_id si pertenece al owner.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        # Verifica pertenencia
        cur.execute("SELECT 1 FROM pagos_mes WHERE id=%s AND owner=%s;", (pago_id, owner))
        if not cur.fetchone():
            return [], []

        cur.execute("""
            SELECT trabajador, dias, horas_extra, monto_dias, monto_hex, total
            FROM pagos_mes_nomina
            WHERE pago_id=%s
            ORDER BY trabajador;
        """, (pago_id,))
        nomina = cur.fetchall()

        cur.execute("""
            SELECT fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total
            FROM pagos_mes_insumos
            WHERE pago_id=%s
            ORDER BY fecha, id;
        """, (pago_id,))
        insumos = cur.fetchall()

        return nomina, insumos
    finally:
        conn.close()

# ==========================
# (Opcional) Ajuste de esquema en caliente para columnas nuevas
# ==========================
def ensure_cierres_schema():
    """
    Asegura que pagos_mes tenga columnas clave si migras desde una versión anterior.
    Es idempotente.
    """
    conn = connect_db(); cur = conn.cursor()
    try:
        cur.execute("""
            ALTER TABLE pagos_mes
            ADD COLUMN IF NOT EXISTS owner TEXT,
            ADD COLUMN IF NOT EXISTS created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            ADD COLUMN IF NOT EXISTS tarifa_dia        NUMERIC     NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS tarifa_hora_extra NUMERIC     NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_nomina      NUMERIC     NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_insumos     NUMERIC     NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS total_general     NUMERIC     NOT NULL DEFAULT 0;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pagos_mes_owner ON pagos_mes(owner);")
        conn.commit()
    finally:
        conn.close()















