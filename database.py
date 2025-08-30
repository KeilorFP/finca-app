# database.py — versión Supabase (Postgres) con psycopg2 + bcrypt
from psycopg2 import IntegrityError
import bcrypt
import os
import psycopg2

def connect_db():
    
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
        # Sanitiza la URL para logs (oculta la contraseña)
        try:
            before_at, after_at = url.split("@", 1)
            safe_url = "postgresql://postgres:***@" + after_at
        except Exception:
            safe_url = "postgresql://postgres:***@<host>:<port>/<db>"
        raise RuntimeError(f"No pude conectar a Postgres con DSN={safe_url}. Detalle: {e}")

def create_users_table():
    pass

def create_trabajadores_table():
    pass

def create_jornadas_table():
    pass

def create_insumos_table():
    pass


# ==========================
# Usuarios (login con bcrypt)
# ==========================
def add_user(username, raw_password):
    """
    Crea usuario con password hasheada (bcrypt).
    """
    hashed = bcrypt.hashpw(raw_password.encode(), bcrypt.gensalt()).decode()
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hashed),
        )
        conn.commit()
    finally:
        conn.close()


def verify_user(username, raw_password):
    """
    Verifica usuario y contraseña (bcrypt).
    Retorna True/False.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT password FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            return False
        hashed = row[0]
        return bcrypt.checkpw(raw_password.encode(), hashed.encode())
    finally:
        conn.close()


# ==========================
# Trabajadores
# ==========================
def add_trabajador(nombre, apellido):
    """
    Inserta trabajador; evita duplicados por UNIQUE(nombre, apellido).
    Retorna True si insertó, None si ya existía.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO trabajadores (nombre, apellido) VALUES (%s, %s)",
            (nombre, apellido),
        )
        conn.commit()
        return True
    except IntegrityError:
        # Ya existe
        conn.rollback()
        return None
    finally:
        conn.close()


def get_all_trabajadores():
    """
    Retorna lista de 'Nombre Apellido' ordenada por nombre.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT nombre || ' ' || apellido FROM trabajadores ORDER BY nombre;")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ==========================
# Jornadas
# ==========================
def add_jornada(trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra):
    """
    Inserta jornada. 'fecha' puede ser str 'YYYY-MM-DD' (Postgres DATE lo adapta).
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO jornadas (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_jornadas():
    """
    Retorna todas las jornadas (para reportes), ordenadas por fecha DESC.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM jornadas ORDER BY fecha DESC;")
        return cur.fetchall()
    finally:
        conn.close()


def get_last_jornada_by_date(fecha):
    """
    Retorna la última jornada para una fecha específica.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT * FROM jornadas
            WHERE fecha = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (fecha,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_jornada(id, trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra):
    """
    Actualiza una jornada por id.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE jornadas
            SET trabajador=%s, fecha=%s, lote=%s, actividad=%s, dias=%s, horas_normales=%s, horas_extra=%s
            WHERE id=%s;
            """,
            (trabajador, fecha, lote, actividad, dias, horas_normales, horas_extra, id),
        )
        conn.commit()
    finally:
        conn.close()


# ==========================
# Insumos (Abono / Fumigación / Cal / Herbicida)
# Estructura común en tabla 'insumos':
# (id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total)
# ==========================
def add_insumo(fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario):
    """
    Inserta insumo y calcula costo_total = cantidad * precio_unitario.
    """
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO insumos (fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total),
        )
        conn.commit()
    finally:
        conn.close()


# -------- Abono --------
def get_last_abono_by_date(fecha):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT * FROM insumos
            WHERE tipo = 'Abono' AND fecha = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (fecha,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_abono(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s;
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id),
        )
        conn.commit()
    finally:
        conn.close()


# -------- Fumigación --------
def get_last_fumigacion_by_date(fecha):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT * FROM insumos
            WHERE tipo = 'Fumigación' AND fecha = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (fecha,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_fumigacion(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario):
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s;
            """,
            (fecha, lote, etapa, producto, dosis, cantidad, precio_unitario, costo_total, id),
        )
        conn.commit()
    finally:
        conn.close()


# -------- Cal --------
def get_last_cal_by_date(fecha):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT * FROM insumos
            WHERE tipo = 'Cal' AND fecha = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (fecha,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_cal(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario):
    """
    Nota: forzamos tipo='Cal' y guardamos 'etapa' (tipo de cal) en columna etapa.
    """
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s;
            """,
            (fecha, lote, "Cal", etapa, producto, dosis, cantidad, precio_unitario, costo_total, id),
        )
        conn.commit()
    finally:
        conn.close()


# -------- Herbicida --------
def get_last_herbicida_by_date(fecha):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT * FROM insumos
            WHERE tipo = 'Herbicida' AND fecha = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (fecha,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def update_herbicida(id, fecha, lote, etapa, producto, dosis, cantidad, precio_unitario):
    """
    Nota: forzamos tipo='Herbicida' y guardamos 'etapa' (tipo de herbicida) en columna etapa.
    """
    costo_total = (cantidad or 0) * (precio_unitario or 0)
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE insumos
            SET fecha=%s, lote=%s, tipo=%s, etapa=%s, producto=%s, dosis=%s, cantidad=%s, precio_unitario=%s, costo_total=%s
            WHERE id=%s;
            """,
            (fecha, lote, "Herbicida", etapa, producto, dosis, cantidad, precio_unitario, costo_total, id),
        )
        conn.commit()
    finally:
        conn.close()


# === TARIFAS GLOBALES (persisten en Supabase) ===
def create_tarifas_table():
    """
    Crea la tabla 'tarifas' si no existe e inserta la fila única id=1 con valores por defecto.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tarifas (
                id INTEGER PRIMARY KEY,
                pago_dia NUMERIC NOT NULL,
                pago_hora_extra NUMERIC NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        # Fila única (id=1). Cambia 9000/2000 si quieres defaults distintos.
        cur.execute("""
            INSERT INTO tarifas (id, pago_dia, pago_hora_extra)
            VALUES (1, 9000, 2000)
            ON CONFLICT (id) DO NOTHING;
        """)
        conn.commit()
    finally:
        conn.close()


def get_tarifas():
    """
    Devuelve (pago_dia, pago_hora_extra) como floats.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT pago_dia, pago_hora_extra FROM tarifas WHERE id = 1;")
        row = cur.fetchone()
        if not row:
            return 9000.0, 2000.0
        return float(row[0]), float(row[1])
    finally:
        conn.close()


def set_tarifas(pago_dia, pago_hora_extra):
    """
    Actualiza la fila única con las nuevas tarifas.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO tarifas (id, pago_dia, pago_hora_extra, updated_at)
            VALUES (1, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE
            SET pago_dia = EXCLUDED.pago_dia,
                pago_hora_extra = EXCLUDED.pago_hora_extra,
                updated_at = NOW();
        """, (pago_dia, pago_hora_extra))
        conn.commit()
    finally:
        conn.close()




