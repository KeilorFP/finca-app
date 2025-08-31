# database.py — versión Supabase (Postgres) con psycopg2 + bcrypt
from psycopg2 import IntegrityError
import bcrypt
import os
import psycopg2
from psycopg2.extras import execute_values

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


# ========== TABLAS DE CIERRES ==========
def create_cierres_tables():
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pagos_mes (
                id SERIAL PRIMARY KEY,
                mes_ini DATE NOT NULL,
                mes_fin DATE NOT NULL,
                creado_por TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                tarifa_dia NUMERIC NOT NULL,
                tarifa_hora_extra NUMERIC NOT NULL,
                total_nomina NUMERIC NOT NULL DEFAULT 0,
                total_insumos NUMERIC NOT NULL DEFAULT 0,
                total_general NUMERIC NOT NULL DEFAULT 0,
                UNIQUE (mes_ini, mes_fin)
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
        conn.commit()
    finally:
        conn.close()

# ========== QUERIES POR RANGO (para previews) ==========
def get_jornadas_between(fecha_ini, fecha_fin):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM jornadas
            WHERE fecha BETWEEN %s AND %s
            ORDER BY fecha DESC, id DESC;
        """, (fecha_ini, fecha_fin))
        return cur.fetchall()
    finally:
        conn.close()

def get_insumos_between(fecha_ini, fecha_fin):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE fecha BETWEEN %s AND %s
            ORDER BY fecha DESC, id DESC;
        """, (fecha_ini, fecha_fin))
        return cur.fetchall()
    finally:
        conn.close()

# ========== CREAR / GUARDAR CIERRE MENSUAL ==========
def crear_cierre_mensual(mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra, overwrite=False):
    """
    Crea un snapshot mensual de nómina + insumos en pagos_mes + pagos_mes_nomina + pagos_mes_insumos.
    - mes_ini/mes_fin: 'YYYY-MM-DD'
    - overwrite=True borra el cierre existente del mismo rango y lo vuelve a crear.
    Retorna el pago_id del cierre creado.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        # ¿Existe ya?
        cur.execute("SELECT id FROM pagos_mes WHERE mes_ini=%s AND mes_fin=%s;", (mes_ini, mes_fin))
        row = cur.fetchone()
        if row and not overwrite:
            raise ValueError("Ya existe un cierre para ese mes. Activa 'Sobrescribir' si quieres recrearlo.")
        if row and overwrite:
            cur.execute("DELETE FROM pagos_mes WHERE mes_ini=%s AND mes_fin=%s;", (mes_ini, mes_fin))
            conn.commit()

        # Nomina agregada por trabajador en el rango
        cur.execute("""
            SELECT trabajador,
                   COALESCE(SUM(dias),0) AS dias,
                   COALESCE(SUM(horas_extra),0) AS horas_extra
            FROM jornadas
            WHERE fecha BETWEEN %s AND %s
            GROUP BY trabajador
            ORDER BY trabajador;
        """, (mes_ini, mes_fin))
        nomina_rows = cur.fetchall()

        # Totales de nómina
        total_nomina = 0
        detalle_nomina = []
        for trab, dias, hextra in nomina_rows:
            monto_dias = (dias or 0) * tarifa_dia
            monto_hex  = (hextra or 0) * tarifa_hora_extra
            total      = (monto_dias or 0) + (monto_hex or 0)
            total_nomina += total
            detalle_nomina.append((trab, dias or 0, float(hextra or 0), float(monto_dias or 0), float(monto_hex or 0), float(total or 0)))

        # Insumos del rango
        cur.execute("""
            SELECT fecha, lote, tipo, producto, etapa, dosis, cantidad, precio_unitario, costo_total
            FROM insumos
            WHERE fecha BETWEEN %s AND %s
            ORDER BY fecha, id;
        """, (mes_ini, mes_fin))
        insumos_rows = cur.fetchall()

        total_insumos = sum(float(r[-1] or 0) for r in insumos_rows)
        total_general = float(total_nomina) + float(total_insumos)

        # Cabecera
        cur.execute("""
            INSERT INTO pagos_mes
                (mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra,
                 total_nomina, total_insumos, total_general)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
        """, (mes_ini, mes_fin, creado_por, tarifa_dia, tarifa_hora_extra,
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

        # Detalle insumos (snapshot)
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

# ========== LISTAR / LEER CIERRES ==========
def listar_cierres():
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, mes_ini, mes_fin, creado_por, created_at, total_nomina, total_insumos, total_general
            FROM pagos_mes
            ORDER BY mes_ini DESC;
        """)
        return cur.fetchall()
    finally:
        conn.close()

def leer_cierre_detalle(pago_id):
    conn = connect_db()
    cur = conn.cursor()
    try:
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










