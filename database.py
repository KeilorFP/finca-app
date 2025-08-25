# database.py — versión Supabase (Postgres) con psycopg2 + bcrypt

import os
import psycopg2
from psycopg2 import IntegrityError
import bcrypt


# ==========================
# Conexión
# ==========================
def connect_db():
    """
    Conecta a Supabase Postgres usando la variable de entorno DATABASE_URL.
    Ejemplo:
    postgresql://postgres:TU_PASSWORD@db.<project-ref>.supabase.co:5432/postgres
    """
    return psycopg2.connect(os.environ["postgresql://postgres:Pradok.87zeus@db.dvtnonjjdcuovechobau.supabase.co:5432/postgres"], sslmode="require")


# ==========================
# (NO-OP) Creación de tablas
# — Ya creaste las tablas en Supabase con SQL.
# — Estas funciones quedan vacías para no romper tu main.py.
# ==========================
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
