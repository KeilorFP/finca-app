import time
import pandas as pd
import os
import os, streamlit as st
from streamlit_option_menu import option_menu
import datetime
import datetime as dt

# IMPORTS PARA PDF
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# En Railway usamos variable de entorno; si no existe, intentamos leer de st.secrets (solo en Streamlit Cloud)
if not os.getenv("DATABASE_URL"):
    try:
        os.environ["DATABASE_URL"] = st.secrets["DATABASE_URL"]
    except Exception:
        pass


from database import create_users_table, add_user, verify_user
from database import create_jornadas_table, add_jornada, get_all_jornadas
from database import create_insumos_table, add_insumo
from database import create_trabajadores_table, add_trabajador, get_all_trabajadores
from database import get_last_jornada_by_date, update_jornada
from database import get_last_abono_by_date, update_abono
from database import get_last_fumigacion_by_date, update_fumigacion
from database import get_last_cal_by_date, update_cal
from database import get_last_herbicida_by_date, update_herbicida

#Facilidad para movil

st.markdown("""
<style>
/* —— Mejora móvil general (<= 640px) —— */
@media (max-width: 640px) {
  /* margen/padding general */
  .block-container { padding: 0.6rem !important; }

  /* etiquetas más legibles */
  label, .stSelectbox label, .stNumberInput label, .stDateInput label {
    font-size: 0.95rem !important;
  }

  /* campos más grandes para el dedo + evita zoom iOS (font-size >=16px) */
  input, textarea, select {
    font-size: 16px !important;
    min-height: 44px !important;
  }
  [role="spinbutton"] { min-height: 44px !important; }

  /* botones anchos y cómodos (incluye download_button) */
  div.stButton > button, .stDownloadButton > button {
    width: 100% !important;
    padding: 12px 16px !important;
    font-size: 16px !important;
    border-radius: 10px !important;
    background: linear-gradient(90deg, #10b981, #059669) !important;
    color: #fff !important;
    border: 1px solid #10b981 !important;
  }

  /* métricas alineadas */
  .stMetric { text-align: left !important; }

  /* —— Menú lateral (streamlit-option-menu) coherente con tu tema —— */
  section[data-testid="stSidebar"] .nav-link {
    width: 100% !important;
    padding: 12px 14px !important;
    margin: 8px 0 !important;
    border-radius: 12px !important;
    background: #111827 !important;          /* fondo oscuro */
    border: 1px solid #374151 !important;     /* borde gris */
    color: #e5e7eb !important;                /* texto claro */
  }
  section[data-testid="stSidebar"] .nav-link i {
    color: #10b981 !important;                /* icono verde */
    font-size: 20px !important;
    margin-right: 8px;
  }
  section[data-testid="stSidebar"] .nav-link:hover {
    border-color: #10b981 !important;
    box-shadow: 0 4px 14px rgba(16,185,129,.25) !important;
    transform: translateY(-1px);
  }
  section[data-testid="stSidebar"] .nav-link-selected {
    background: linear-gradient(90deg, #10b981, #059669) !important;
    color: #ffffff !important;
    border: 1px solid #10b981 !important;
    box-shadow: 0 6px 18px rgba(16,185,129,.28) !important;
    font-weight: 700 !important;
  }
  section[data-testid="stSidebar"] .nav-link-selected i {
    color: #ffffff !important;
  }
}
</style>
""", unsafe_allow_html=True)


# Codigos reutilizables
LOTE_LISTA = ["Bijagual Fernando", "Bijagual Brothers", "El Alto", "Quebradaonda", "San Bernardo"]

ACTIVIDADES = ["Herbiciar", "Abonado", "Fumigación", "Poda", "Desije", "Encalado", "Resiembra", "Siembra", "Eliminar sombra", "Otra"]

ETAPAS_ABONO = ["1ra Abonada", "2da Abonada", "3ra Abonada", "4ta Abonada"]

TIPOS_HERBICIDA = ["Selectivo", "No selectivo", "Sistemico", "De contacto", "Otro"]

TIPOS_CAL = [
    "Cal agrícola (CaCO₃)",
    "Cal dolomita (CaCO₃·MgCO₃)",
    "Mezcla con yeso agrícola (CaSO₄)",
    "Cal viva (CaO)",
    "Cal apagada (Ca(OH)₂)"
]



def selectbox_lotes(label="Lote o parcela", valor_actual=None):
    if valor_actual in LOTE_LISTA:
        idx = LOTE_LISTA.index(valor_actual)
    else:
        idx = 0
    return st.selectbox(label, LOTE_LISTA, index=idx)



# Inicializar DB
create_users_table()
create_jornadas_table()
create_insumos_table()
create_trabajadores_table()

import streamlit as st

# ====== CSS para login moderno ======
st.markdown("""
<style>
/* Fondo general del sidebar */
section[data-testid="stSidebar"] {
    background-color: #111827;
}

/* Títulos */
h1, h2, h3 {
    color: #10b981 !important;  /* verde */
    font-weight: 700;
}

/* Inputs de texto */
input {
    border-radius: 10px !important;
    border: 1px solid #374151 !important;
    background-color: #1f2937 !important;
    color: #f9fafb !important;
}

/* Botones */
div.stButton > button {
    background: linear-gradient(90deg, #10b981, #059669) !important;
    color: white !important;
    border: none;
    border-radius: 10px;
    font-weight: 600;
    padding: 0.6rem 1rem;
    transition: all .2s ease-in-out;
}
div.stButton > button:hover {
    background: linear-gradient(90deg, #059669, #10b981) !important;
    box-shadow: 0 4px 12px rgba(16,185,129,.3);
    transform: translateY(-1px);
}
</style>
""", unsafe_allow_html=True)

# ====== Login ======
def login():
    st.title("☕ Finca Cafetalera - Inicio de Sesión")

    # Menú superior sencillo en vez de sidebar
    tabs = st.radio("Menú", ["Iniciar sesión", "Crear cuenta"], horizontal=True)

    if tabs == "Iniciar sesión":
        st.subheader("Ingresar")
        username = st.text_input("👤 Usuario")
        password = st.text_input("🔑 Contraseña", type="password")
        if st.button("Entrar"):
            user = verify_user(username, password)
            if user:
                st.session_state.logged_in = True
                st.session_state.user = username
                st.rerun()
            else:
                st.error("❌ Usuario o contraseña incorrectos")

    elif tabs == "Crear cuenta":
        st.subheader("Crear nuevo usuario")
        new_user = st.text_input("👤 Nuevo usuario")
        new_pass = st.text_input("🔑 Nueva contraseña", type="password")
        if st.button("Registrar"):
            try:
                add_user(new_user, new_pass)
                st.success("✅ Usuario creado exitosamente. Ya puedes iniciar sesión.")
            except:
                st.error("⚠️ Ese usuario ya existe")


# ====== Estado de sesión ======
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user" not in st.session_state:
    st.session_state.user = ""

if not st.session_state.logged_in:
    login()
    st.stop()

# ====== Panel principal ======
st.title("📋 Panel de Control - Finca Cafetalera")
st.write(f"👤 Usuario: **{st.session_state.user}**")


#menu bottons
with st.sidebar:
    st.markdown("## 🧭 Menú Principal")
    menu = option_menu(
        menu_title=None,
        options=[
            "Registrar Jornada",
            "Registrar Abono",
            "Registrar Fumigación",
            "Registrar Cal",
            "Registrar Herbicida",
            "Ver Registros",
            "Añadir Empleado",
            "Reporte Semanal (Dom–Sáb)"
        ],
        icons=[
            "calendar-check",  # Jornada
            "fuel-pump",            # Abono
            "bezier",             # Fumigación
            "gem",          # Cal
            "droplet",         # Herbicida
            "journal-text",    # Ver Registros
            "person-plus",     # Añadir Empleado
            "bar-chart"        # Reporte
        ],
        default_index=0,
        orientation="vertical",
        styles={
            "container": {"padding": "0!important", "background": "rgba(0,0,0,0)"},
            "icon": {"font-size": "18px", "color": "#10b981"},   # ← iconos verdes
            "nav-link": {
                "font-size": "15px",
                "padding": "10px 12px",
                "border-radius": "12px",
                "margin": "6px 0",
                "color": "#e5e7eb",              # texto claro
                "background-color": "#111827",   # gris oscuro
                "border": "1px solid #374151",
            },
            "nav-link-selected": {
                "background": "linear-gradient(90deg, #10b981, #059669)",
                "color": "#ffffff",
                "font-weight": "700",
                "border": "1px solid #10b981",
                "box-shadow": "0 4px 18px rgba(16,185,129,.25)",
            },
        }
    )

# FORMULARIO DE AÑADIR EMPLEADO
if menu == "Añadir Empleado":
    st.subheader("👥 Registrar Nuevo Empleado")
    with st.form("form_empleado"):
        nombre = st.text_input("Nombre del empleado")
        apellido = st.text_input("Apellido del empleado")
        submit_trabajador = st.form_submit_button("Registrar trabajador")
        
        if submit_trabajador:
            if not nombre.strip() or not apellido.strip():
                st.warning("⚠️ Por favor completa todos los campos.")
            else:
                try:
                    add_trabajador(nombre.strip(), apellido.strip())
                    st.success("✅ Empleado registrado exitosamente.")
                except Exception as e:
                    st.error(f"❌ Error al registrar empleado: {str(e)}")


#Formulario jornada
if menu == "Registrar Jornada":
    st.subheader("🧑‍🌾 Registrar Jornada Laboral")
    trabajadores_disponibles = get_all_trabajadores()
    if not trabajadores_disponibles:
        st.warning("⚠️  No hay trabajadores registrados. Por favor agrega uno primero desde el panel correspondiente.")
    else:
        with st.form("form_jornada"):
            trabajador = st.selectbox("Selecciona un trabajador", trabajadores_disponibles)
            fecha = st.date_input("Fecha de trabajo", datetime.date.today())
            lote = st.selectbox("Lote o parcela", LOTE_LISTA)
            actividad = st.selectbox("Tipo de actividad", ACTIVIDADES)
            dias = st.number_input("Días trabajados", min_value=0, max_value=31, step=1)
            horas_extra = st.number_input("Horas extra trabajadas", min_value=0.0, step=0.5)

            horas_normales = dias * 6  # Cálculo automático
            st.info(f"🕒 Horas normales calculadas automáticamente: {horas_normales} horas")

            submitted = st.form_submit_button("Guardar jornada")
            if submitted:
                if trabajador.strip() == "":
                    st.warning("⚠️ Por favor selecciona un trabajador.")
                else:
                    add_jornada(trabajador, str(fecha), lote, actividad, dias, horas_normales, horas_extra)
                    st.success("✅ Jornada registrada exitosamente")

        with st.expander("✏️ Editar último registro de jornada"):
            ultima_jornada = get_last_jornada_by_date(str(fecha))
            if ultima_jornada:
                jornada_id, trabajador_actual, fecha_actual, lote, actividad, dias, horas_normales, horas_extra = ultima_jornada

                # Trabajador
                try:
                    idx_trab = trabajadores_disponibles.index(trabajador_actual)
                except ValueError:
                    idx_trab = 0
                nuevo_trabajador = st.selectbox("Nuevo trabajador", trabajadores_disponibles, index=idx_trab)

                # Fecha
                nueva_fecha = st.date_input("Nueva fecha de trabajo", datetime.datetime.strptime(str(fecha_actual)[:10], "%Y-%m-%d").date(), format="YYYY-MM-DD")


                # Lote (seguro)
                try:
                    idx_lote = LOTE_LISTA.index(lote)
                except ValueError:
                    idx_lote = 0
                nuevo_lote = st.selectbox("Nuevo lote", LOTE_LISTA, index=idx_lote)

                # Actividad (segura)
                try:
                    idx_act = ACTIVIDADES.index(actividad)
                except ValueError:
                    idx_act = 0
                nueva_actividad = st.selectbox("Nueva actividad", ACTIVIDADES, index=idx_act)

                # Números
                nuevos_dias = st.number_input("Nuevos días trabajados", value=int(dias), min_value=0, max_value=31, step=1)
                nuevas_horas_extra = st.number_input("Nuevas horas extra", value=float(horas_extra), min_value=0.0, step=0.5)
                nuevas_horas_normales = nuevos_dias * 6
                st.info(f"🕒 Nuevas horas normales: {nuevas_horas_normales} horas")

                if st.button("Actualizar jornada"):
                    update_jornada(
                        jornada_id,
                        nuevo_trabajador,
                        nueva_fecha.strftime("%Y-%m-%d"),
                        nuevo_lote,
                        nueva_actividad,
                        nuevos_dias,
                        nuevas_horas_normales,
                        nuevas_horas_extra
                    )
                    st.success("✅ Jornada actualizada correctamente.")
                    st.rerun()
            else:
                st.info("No hay registros de jornada para editar.")
# FORMULARIO DE ABONADO
if menu == "Registrar Abono":
    st.subheader("🌿 Registrar Aplicación de Abono")
    with st.form("form_abonado"):
        fecha_abono = st.date_input("Fecha de aplicación de abono", datetime.date.today())
        lote_abono = st.selectbox("Lote o parcela", LOTE_LISTA)
        etapa = st.selectbox("Etapa de abonado", ETAPAS_ABONO)
        producto = st.text_input("Nombre del producto (ej: 18-5-15, Multimag)")
        dosis = st.number_input("Dosis aplicada (en gramos por planta)", min_value=0.0, step=0.1)
        cantidad = st.number_input("Cantidad aplicada (en sacos)", min_value=0.0, step=0.5)
        precio_unitario = st.number_input("Precio por saco (₡)", min_value=0.0, step=100.0)

        if cantidad > 0 and precio_unitario > 0:
            costo_estimado = cantidad * precio_unitario
            st.info(f"💰 Costo total estimado: ₡{costo_estimado:,.2f}")

        guardar_abono = st.form_submit_button("Guardar aplicación de abono")
        if guardar_abono:
            add_insumo(str(fecha_abono), lote_abono, "Abono", etapa, producto, dosis, cantidad, precio_unitario)
            st.success("✅ Aplicación de abono registrada correctamente.")
            st.rerun()  # 🔁 Recargar para limpiar el formulario
    # Editar último abono
    with st.expander("✏️ Editar último registro de abono"):
        ultima_abonado = get_last_abono_by_date(str(fecha_abono))
        if ultima_abonado:
            (abono_id, fecha_actual, lote_actual, tipo, etapa_actual, producto_actual,
             dosis_actual, cantidad_actual, precio_unitario_actual, costo_total_actual) = ultima_abonado

            # Lote (seguro)
            try:
                idx_lote = LOTE_LISTA.index(lote_actual)
            except ValueError:
                idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", LOTE_LISTA, index=idx_lote)

            # Etapa (segura)
            try:
                idx_etapa = ETAPAS_ABONO.index(etapa_actual)
            except ValueError:
                idx_etapa = 0
            nueva_etapa = st.selectbox("Nueva etapa", ETAPAS_ABONO, index=idx_etapa)

            # Fecha
            nueva_fecha = st.date_input("Nueva fecha de abono", datetime.datetime.strptime(fecha_actual, "%Y-%m-%d"))

            # Otros campos
            nuevo_producto = st.text_input("Nuevo producto", value=producto_actual)
            nueva_dosis = st.number_input("Nueva dosis (g/planta)", value=float(dosis_actual), min_value=0.0, step=0.1)
            nueva_cantidad = st.number_input("Nueva cantidad (sacos)", value=float(cantidad_actual), min_value=0.0, step=0.5)
            nuevo_precio_unitario = st.number_input("Nuevo precio por saco (₡)", value=float(precio_unitario_actual), min_value=0.0, step=100.0)

            nuevo_costo_total = nueva_cantidad * nuevo_precio_unitario
            st.info(f"💰 Nuevo costo total estimado: ₡{nuevo_costo_total:,.2f}")

            if st.button("Actualizar abono"):
                update_abono(
                    abono_id,
                    nueva_fecha.strftime("%Y-%m-%d"),
                    nuevo_lote,
                    nueva_etapa,
                    nuevo_producto,
                    nueva_dosis,
                    nueva_cantidad,
                    nuevo_precio_unitario
                )
                st.success("✅ Abono actualizado correctamente.")
                st.rerun()
        else:
            st.info("No hay registros de abono para editar.")


def mostrar_insumos_por_tipo(tipo_nombre, columnas):
    from database import connect_db
    conn = connect_db()
    c = conn.cursor()
    c.execute("SELECT * FROM insumos WHERE tipo = ? ORDER BY fecha DESC;", (tipo_nombre,))
    registros = c.fetchall()
    conn.close()
    if registros:
        df = pd.DataFrame(registros, columns=columnas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info(f"No hay registros de {tipo_nombre.lower()} aún.")

# SECCIÓN DE REGISTROS
if menu == "Ver Registros":
    st.subheader("📊 Registros de Jornadas e Insumos")

    with st.expander("💰 Configurar tarifas por hora"):
        valor_hora_normal = st.number_input("Valor por hora normal (₡)", min_value=0, step=100, value=1500)
        valor_hora_extra = st.number_input("Valor por hora extra (₡)", min_value=0, step=100, value=2000)

    # Mostrar registros de jornadas
    with st.expander("📋 Ver Jornadas Registradas"):
        jornadas = get_all_jornadas()
        if jornadas:
            df_jornadas = pd.DataFrame(jornadas, columns=[
                "ID", "Trabajador", "Fecha", "Lote", "Actividad", "Días", "Horas Normales", "Horas Extra"
            ])
            resumen = df_jornadas.groupby("Trabajador").agg({
                "Días": "sum",
                "Horas Normales": "sum",
                "Horas Extra": "sum"
            }).reset_index()
            resumen["Pago Horas Normales"] = resumen["Horas Normales"] * valor_hora_normal
            resumen["Pago Horas Extra"] = resumen["Horas Extra"] * valor_hora_extra
            resumen["Total Ganado"] = resumen["Pago Horas Normales"] + resumen["Pago Horas Extra"]

            st.markdown("### 👥 Resumen por Trabajador")
            st.dataframe(resumen.style.format({
                "Días": "{:,.0f}",
                "Horas Normales": "{:,.0f}",
                "Horas Extra": "{:,.0f}",
                "Pago Horas Normales": "₡{:,.0f}",
                "Pago Horas Extra": "₡{:,.0f}",
                "Total Ganado": "₡{:,.0f}"
            }), use_container_width=True)
        else:
            st.info("No hay jornadas registradas aún.")

    # Mostrar insumos separados por tipo
    tipos_insumos = {
        "Abono": "🌿 Ver Abonados Aplicados",
        "Fumigación": "🧪 Ver Fumigaciones Aplicadas",
        "Cal": "🧱 Ver Cal Aplicada",
        "Herbicida": "🌾 Ver Herbicidas Aplicados"
    }

    for tipo, titulo in tipos_insumos.items():
        with st.expander(titulo):
            from database import connect_db
            conn = connect_db()
            c = conn.cursor()
            c.execute("SELECT * FROM insumos WHERE tipo = %s ORDER BY fecha DESC;", (tipo,))
            registros = c.fetchall()
            conn.close()
            if registros:
                df_insumos = pd.DataFrame(registros, columns=[
                    "ID", "Fecha", "Lote", "Tipo", "Etapa", "Producto", "Dosis", "Cantidad", "Precio Unitario", "Costo Total"
                ])
                st.dataframe(df_insumos, use_container_width=True)
            else:
                st.info(f"No hay insumos registrados aún para {tipo.lower()}.")


# FORMULARIO DE FUMIGACIÓN
if menu == "Registrar Fumigación":
    st.subheader("🧪 Registrar Fumigación")
    with st.form("form_fumigacion"):
        fecha_fum = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_fum = st.selectbox("Lote o parcela", LOTE_LISTA)
        producto = st.text_input("Nombre del producto utilizado (ej: Fungicida X, Insecticida Y)")
        plaga = st.text_input("Tipo de plaga o control (ej: Roya, Broca, Hongos)")
        dosis = st.text_input("Dosis aplicada por estañon (ej: 50 ml/estañon)")
        litros = st.number_input("Litros aplicados por lote o parcela", min_value=0.0, step=0.5)
        precio_litro = st.number_input("Precio por litro de fumigación (₡)", min_value=0.0, step=100.0)

        if litros > 0 and precio_litro > 0:
            costo_total = litros * precio_litro
            st.info(f"💰 Costo total estimado: ₡{costo_total:,.2f}")

        guardar_fumigacion = st.form_submit_button("Guardar fumigación")
        if guardar_fumigacion:
            add_insumo(str(fecha_fum), lote_fum, "Fumigación", plaga, producto, dosis, litros, precio_litro)
            st.success("✅ Aplicación de fumigación registrada correctamente.")

    with st.expander("✏️ Editar último registro de fumigación"):
        ultima_fumigacion = get_last_fumigacion_by_date(str(fecha_fum))
        if ultima_fumigacion:
            (fum_id, fecha, lote, tipo, plaga, producto, dosis, litros, precio_unitario, costo_total) = ultima_fumigacion

            st.write(f"📅 Fecha: {fecha} | 🧪 Producto: {producto}")

            try:
                idx_lote = LOTE_LISTA.index(lote)
            except ValueError:
                idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", LOTE_LISTA, index=idx_lote)

            nueva_plaga = st.text_input("Nuevo tipo de plaga o control", value=plaga)
            nuevo_producto = st.text_input("Nuevo producto", value=producto)
            nueva_dosis = st.text_input("Nueva dosis aplicada", value=dosis)
            nuevos_litros = st.number_input("Nuevos litros aplicados", value=float(litros), min_value=0.0, step=0.5)
            nuevo_precio_litro = st.number_input("Nuevo precio por litro", value=float(precio_unitario), min_value=0.0, step=100.0)

            if st.button("Actualizar fumigación"):
                update_fumigacion(fum_id, fecha, nuevo_lote, nueva_plaga, nuevo_producto, nueva_dosis, nuevos_litros, nuevo_precio_litro)
                st.success("✅ Registro de fumigación actualizado correctamente.")
                st.rerun()
        else:
            st.info("No hay registros de fumigación para editar.")


# FORMULARIO DE CAL
if menu == "Registrar Cal":
    st.subheader("🧱 Registrar Aplicación de Cal")
    with st.form("form_cal"):
        fecha_cal = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_cal = st.selectbox("Lote o parcela", LOTE_LISTA)
        tipo_cal = st.selectbox("Tipo de cal utilizada", TIPOS_CAL)
        cantidad_sacos = st.number_input("Cantidad aplicada (en sacos de 45 kg)", min_value=0.0, step=0.5)
        precio_saco = st.number_input("Precio por saco (₡)", min_value=0.0, step=100.0)

        if cantidad_sacos > 0 and precio_saco > 0:
            costo_total = cantidad_sacos * precio_saco
            st.info(f"💰 Costo total estimado: ₡{costo_total:,.2f}")

        guardar_cal = st.form_submit_button("Guardar aplicación de cal")
        if guardar_cal:
            add_insumo(
                str(fecha_cal),
                lote_cal,
                "Cal",
                tipo_cal,
                "Saco 45 kg",
                "",
                cantidad_sacos,
                precio_saco
            )
            st.success("✅ Aplicación de cal registrada correctamente.")

    with st.expander("✏️ Editar último registro de cal"):
        fecha_busqueda = st.date_input("Fecha de búsqueda", datetime.date.today())
        ultima_cal = get_last_cal_by_date(str(fecha_busqueda))
        if ultima_cal:
            (cal_id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total) = ultima_cal

            st.write(f"📅 Fecha: {fecha} | 🧱 Tipo: {tipo}")

            try:
                idx_tipo = TIPOS_CAL.index(tipo)
            except ValueError:
                idx_tipo = 0
            try:
                idx_lote = LOTE_LISTA.index(lote)
            except ValueError:
                idx_lote = 0

            nuevo_lote = st.selectbox("Nuevo lote", LOTE_LISTA, index=idx_lote)
            nuevo_tipo = st.selectbox("Nuevo tipo de cal", TIPOS_CAL, index=idx_tipo)
            nueva_cantidad = st.number_input("Nueva cantidad aplicada (sacos)", value=float(cantidad), min_value=0.0, step=0.5)
            nuevo_precio = st.number_input("Nuevo precio por saco (₡)", value=float(precio_unitario), min_value=0.0, step=100.0)

            if st.button("Actualizar cal"):
                update_cal(
                    cal_id,
                    fecha,
                    nuevo_lote,
                    nuevo_tipo,     # etapa
                    "Saco 45 kg",   # producto
                    "",             # dosis
                    nueva_cantidad,
                    nuevo_precio
                )
                st.success("✅ Registro de cal actualizado correctamente.")
                st.rerun()
        else:
            st.info("No hay registros de cal para editar.")


# FORMULARIO DE HERBICIDA
if menu == "Registrar Herbicida":
    st.subheader("🌾 Registrar Aplicación de Herbicida")

    with st.form("form_herbicida"):
        fecha_herb = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_herb = st.selectbox("Lote o parcela", LOTE_LISTA)
        tipo_herbicida = st.selectbox("Tipo de herbicida utilizado", TIPOS_HERBICIDA)
        producto_herb = st.text_input("Nombre del producto (ej: Glifosato 41%, Paraquat 20%)")
        dosis_herb = st.text_input("Dosis aplicada (ej: 80 ml/estañón)")
        litros_herb = st.number_input("Litros aplicados por lote o parcela", min_value=0.0, step=0.5)
        precio_litro_herb = st.number_input("Precio por litro de herbicida (₡)", min_value=0.0, step=100.0)

        if litros_herb > 0 and precio_litro_herb > 0:
            costo_total_herb = litros_herb * precio_litro_herb
            st.info(f"💰 Costo total estimado: ₡{costo_total_herb:,.2f}")

        guardar_herbicida = st.form_submit_button("Guardar aplicación de herbicida")
        if guardar_herbicida:
            add_insumo(
                str(fecha_herb),
                lote_herb,
                "Herbicida",
                tipo_herbicida,
                producto_herb,
                dosis_herb,
                litros_herb,
                precio_litro_herb
            )
            st.success("✅ Aplicación de herbicida registrada correctamente.")

    # Update herbicida
    with st.expander("✏️ Editar último registro de herbicida"):
        ultima_herbicida = get_last_herbicida_by_date(str(fecha_herb))
        if ultima_herbicida:
            (herb_id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total) = ultima_herbicida

            st.write(f"📅 Fecha: {fecha} | 🌿 Tipo: {tipo}")

            # Lote (seguro)
            try:
                idx_lote = LOTE_LISTA.index(lote)
            except ValueError:
                idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", LOTE_LISTA, index=idx_lote)

            # Tipo (seguro)
            try:
                idx_tipo = TIPOS_HERBICIDA.index(tipo)
            except ValueError:
                idx_tipo = 0
            nuevo_tipo = st.selectbox("Nuevo tipo de herbicida", TIPOS_HERBICIDA, index=idx_tipo)

            nuevo_producto = st.text_input("Nuevo producto", value=producto)
            nueva_dosis = st.text_input("Nueva dosis", value=dosis)
            nueva_cantidad = st.number_input("Nueva cantidad aplicada (litros)", value=float(cantidad), min_value=0.0, step=0.5)
            nuevo_precio = st.number_input("Nuevo precio por litro (₡)", value=float(precio_unitario), min_value=0.0, step=100.0)

            if st.button("Actualizar herbicida"):
                update_herbicida(herb_id, fecha, nuevo_lote, nuevo_tipo, nuevo_producto, nueva_dosis, nueva_cantidad, nuevo_precio)
                st.success("✅ Registro de herbicida actualizado correctamente.")
                st.rerun()
        else:
            st.info("No hay registros de herbicida para editar.")

# ============================================================================
# REPORETE SEMANAL (Domingo a Sábado) - Salarios por trabajador
# ============================================================================
if menu == "Reporte Semanal (Dom–Sáb)":
    st.subheader("💵 Reporte Semanal de Salarios (Domingo a Sábado)")

    # 1) Parámetros de la semana y tarifas
    hoy = datetime.date.today()
    fecha_referencia = st.date_input("Selecciona una fecha dentro de la semana", hoy)

    # Helper: domingo a sábado que contienen la fecha_referencia
    def rango_semana_dom_sab(d: datetime.date):
        # weekday(): L=0 ... D=6 → domingo previo = restar (weekday+1)%7
        dias_a_domingo = (d.weekday() + 1) % 7
        domingo = d - datetime.timedelta(days=dias_a_domingo)
        sabado = domingo + datetime.timedelta(days=6)
        return domingo, sabado

    inicio_sem, fin_sem = rango_semana_dom_sab(fecha_referencia)

    st.info(f"📅 Semana seleccionada: **{inicio_sem.strftime('%Y-%m-%d')}** a **{fin_sem.strftime('%Y-%m-%d')}** (Dom–Sáb)")

    col_tar1, col_tar2 = st.columns(2)
    with col_tar1:
        valor_hora_normal = st.number_input("Valor por hora normal (₡)", min_value=0, step=100, value=1500)
    with col_tar2:
        valor_hora_extra = st.number_input("Valor por hora extra (₡)", min_value=0, step=100, value=2000)

    # 2) Traer jornadas y filtrar por semana
    jornadas = get_all_jornadas()
    if not jornadas:
        st.info("No hay jornadas registradas aún.")
    else:
        df = pd.DataFrame(jornadas, columns=[
            "ID", "Trabajador", "Fecha", "Lote", "Actividad", "Días", "Horas Normales", "Horas Extra"
        ])

        # Asegurar tipos correctos
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        df["Horas Normales"] = pd.to_numeric(df["Horas Normales"], errors="coerce").fillna(0.0)
        df["Horas Extra"] = pd.to_numeric(df["Horas Extra"], errors="coerce").fillna(0.0)

        # Filtro por semana (inclusive)
        mask = (df["Fecha"].dt.date >= inicio_sem) & (df["Fecha"].dt.date <= fin_sem)
        df_semana = df.loc[mask].copy()

        if df_semana.empty:
            st.info("No hay jornadas en la semana seleccionada.")
        else:
            # 3) Cálculos por trabajador
            resumen = df_semana.groupby("Trabajador", as_index=False).agg({
                "Días": "sum",
                "Horas Normales": "sum",
                "Horas Extra": "sum"
            })

            resumen["Pago Horas Normales"] = resumen["Horas Normales"] * valor_hora_normal
            resumen["Pago Horas Extra"] = resumen["Horas Extra"] * valor_hora_extra
            resumen["Total a Pagar"] = resumen["Pago Horas Normales"] + resumen["Pago Horas Extra"]

            # 4) Tabla detallada de la semana (opcional)
            st.markdown("### 📋 Jornadas de la semana (detalle)")
            df_semana_orden = df_semana.sort_values(["Trabajador", "Fecha"])
            st.dataframe(
                df_semana_orden.assign(
                    Fecha=df_semana_orden["Fecha"].dt.strftime("%Y-%m-%d")
                ),
                use_container_width=True
            )

            # 5) Resumen por trabajador
            st.markdown("### 👥 Resumen por trabajador")
            st.dataframe(
                resumen.style.format({
                    "Días": "{:,.0f}",
                    "Horas Normales": "{:,.0f}",
                    "Horas Extra": "{:,.1f}",
                    "Pago Horas Normales": "₡{:,.0f}",
                    "Pago Horas Extra": "₡{:,.0f}",
                    "Total a Pagar": "₡{:,.0f}"
                }),
                use_container_width=True
            )

            # 6) Totales generales de la semana
            total_normales = resumen["Pago Horas Normales"].sum()
            total_extras = resumen["Pago Horas Extra"].sum()
            total_semana = resumen["Total a Pagar"].sum()

            st.markdown("### 🧮 Totales de la semana")
            st.write(f"- **Total horas normales (₡):** {total_normales:,.0f}")
            st.write(f"- **Total horas extra (₡):** {total_extras:,.0f}")
            st.write(f"- **Total a pagar (₡):** {total_semana:,.0f}")

            # 7) Descargas CSV
            csv_resumen = resumen.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Descargar resumen semanal (CSV)",
                data=csv_resumen,
                file_name=f"reporte_semanal_{inicio_sem.strftime('%Y-%m-%d')}_a_{fin_sem.strftime('%Y-%m-%d')}.csv",
                mime="text/csv"
            )

            # Un CSV detallado también (opcional)
            csv_detalle = df_semana_orden.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Descargar detalle semanal (CSV)",
                data=csv_detalle,
                file_name=f"reporte_semanal_detalle_{inicio_sem.strftime('%Y-%m-%d')}_a_{fin_sem.strftime('%Y-%m-%d')}.csv",
                mime="text/csv"
            )

            # ===== 7.3) PDF: Resumen por trabajador (Días, Horas Extra, Total a Pagar) =====
            # Tomamos solo las columnas solicitadas
            resumen_min = resumen[["Trabajador", "Días", "Horas Extra", "Total a Pagar"]].copy()

            def generar_pdf_resumen_trabajador(resumen_min, inicio_sem_str, fin_sem_str):
                buffer = BytesIO()
                c = canvas.Canvas(buffer, pagesize=letter)
                width, height = letter

                # Encabezado
                c.setFont("Helvetica-Bold", 14)
                c.drawString(50, height - 50, "Resumen por trabajador")
                c.setFont("Helvetica", 11)
                c.drawString(50, height - 70, f"Semana: {inicio_sem_str} a {fin_sem_str} (Dom–Sáb)")

                # Encabezados de tabla
                y = height - 110
                c.setFont("Helvetica-Bold", 11)
                c.drawString(50,  y, "Trabajador")
                c.drawString(260, y, "Días")
                c.drawString(310, y, "Horas Extra")
                c.drawString(410, y, "Total a pagar (₡)")
                c.line(50, y-3, 560, y-3)

                # Filas
                c.setFont("Helvetica", 10)
                y -= 20
                for _, row in resumen_min.iterrows():
                    trabajador = str(row["Trabajador"])
                    dias = f"{row['Días']:.0f}"
                    hrs_extra = f"{row['Horas Extra']:.1f}"
                    total = f"{row['Total a Pagar']:,.0f}"

                    # Ajuste simple para nombres largos (evita desbordes)
                    nombre = (trabajador[:34] + "…") if len(trabajador) > 35 else trabajador

                    c.drawString(50,  y, nombre)
                    c.drawRightString(295, y, dias)
                    c.drawRightString(375, y, hrs_extra)
                    c.drawRightString(560, y, total)

                    y -= 18
                    if y < 60:  # salto de página
                        c.showPage()
                        # repetir encabezados de tabla en nueva página
                        c.setFont("Helvetica-Bold", 11)
                        y = height - 50
                        c.drawString(50,  y, "Trabajador")
                        c.drawString(260, y, "Días")
                        c.drawString(310, y, "Horas Extra")
                        c.drawString(410, y, "Total a pagar (₡)")
                        c.line(50, y-3, 560, y-3)
                        c.setFont("Helvetica", 10)
                        y -= 20

                c.save()
                pdf_bytes = buffer.getvalue()
                buffer.close()
                return pdf_bytes

            # Generar y descargar PDF
            pdf_resumen_simple = generar_pdf_resumen_trabajador(
                resumen_min,
                inicio_sem.strftime("%Y-%m-%d"),
                fin_sem.strftime("%Y-%m-%d")
            )

            st.download_button(
                "⬇️ Descargar resumen por trabajador (PDF)",
                data=pdf_resumen_simple,
                file_name=f"resumen_trabajador_{inicio_sem.strftime('%Y-%m-%d')}_a_{fin_sem.strftime('%Y-%m-%d')}.pdf",
                mime="application/pdf"
            )


            
    



                 
        
    
        
    


















