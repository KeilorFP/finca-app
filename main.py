import os
import datetime
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from database import (
    connect_db,
    # creación/migraciones
    create_users_table, create_trabajadores_table, create_jornadas_table, create_insumos_table,
    create_tarifas_table, create_cierres_tables, ensure_cierres_schema,
    # fincas
    create_fincas_table, add_finca, get_all_fincas,
    # auth
    add_user, verify_user,
    # trabajadores
    add_trabajador, get_all_trabajadores,
    # jornadas
    add_jornada, get_all_jornadas, get_last_jornada_by_date, update_jornada,
    # insumos
    add_insumo,
    get_last_abono_by_date, update_abono,
    get_last_fumigacion_by_date, update_fumigacion,
    get_last_cal_by_date, update_cal,
    get_last_herbicida_by_date, update_herbicida,
    # tarifas por usuario
    get_tarifas, set_tarifas,
    # cierres
    get_jornadas_between, get_insumos_between,
    crear_cierre_mensual, listar_cierres, leer_cierre_detalle,
)

# =============================
# 🧩 Config DB (Supabase/Postgres)
# =============================
# Fallback: primero env, luego secrets. Requiere sslmode=require para Supabase.
DB_URL = (
    os.getenv("DATABASE_URL")
    or st.secrets.get("DATABASE_URL")
    or st.secrets.get("SUPABASE_DB_URL")
)
if not DB_URL:
    st.error(
        "No encuentro la cadena de conexión. Configura **DATABASE_URL** en *st.secrets* o variable de entorno.\n"
        "Formato típico Supabase: `postgresql://usuario:password@HOST:5432/postgres?sslmode=require`"
    )
    st.stop()
# Normalizamos a env para que connect_db() la use.
os.environ["DATABASE_URL"] = DB_URL

# ===== Estilos =====
st.markdown(
    """
<style>
@media (max-width: 640px) {
  .block-container { padding: 0.6rem !important; }
  label, .stSelectbox label, .stNumberInput label, .stDateInput label { font-size: 0.95rem !important; }
  input, textarea, select { font-size: 16px !important; min-height: 44px !important; }
  [role="spinbutton"] { min-height: 44px !important; }
  div.stButton > button, .stDownloadButton > button {
    width: 100%; padding: 12px 16px; font-size: 16px;
    border-radius: 10px; background: linear-gradient(90deg, #10b981, #059669);
    color: #fff; border: 1px solid #10b981;
  }
  section[data-testid="stSidebar"] .nav-link {
    width: 100%; padding: 12px 14px; margin: 8px 0; border-radius: 12px;
    background: #111827; border: 1px solid #374151; color: #e5e7eb;
  }
  section[data-testid="stSidebar"] .nav-link i { color: #10b981; font-size: 20px; margin-right: 8px; }
  section[data-testid="stSidebar"] .nav-link-selected {
    background: linear-gradient(90deg, #10b981, #059669); color:#fff; border:1px solid #10b981;
    box-shadow: 0 6px 18px rgba(16,185,129,.28); font-weight:700;
  }
}
section[data-testid="stSidebar"] { background:#111827; }
h1,h2,h3{ color:#10b981 !important; font-weight:700; }
input{ border-radius:10px !important; border:1px solid #374151 !important; background:#1f2937 !important; color:#f9fafb !important; }
div.stButton>button{ background:linear-gradient(90deg,#10b981,#059669)!important;color:#fff!important;border:none;border-radius:10px;font-weight:600;padding:.6rem 1rem; }
div.stButton>button:hover{ background:linear-gradient(90deg,#059669,#10b981)!important; box-shadow:0 4px 12px rgba(16,185,129,.3); transform:translateY(-1px);}
</style>
""",
    unsafe_allow_html=True,
)

# ===== Catálogos =====
LOTE_LISTA = ["Bijagual Fernando", "Bijagual Brothers", "El Alto", "Quebradaonda", "San Bernardo"]
ACTIVIDADES = ["Herbiciar","Abonado","Fumigación","Poda","Desije","Encalado","Resiembra","Siembra","Eliminar sombra","Otra"]
ETAPAS_ABONO = ["1ra Abonada","2da Abonada","3ra Abonada","4ta Abonada"]
TIPOS_HERBICIDA = ["Selectivo","No selectivo","Sistemico","De contacto","Otro"]
TIPOS_CAL = ["Cal agrícola (CaCO₃)","Cal dolomita (CaCO₃·MgCO₃)","Mezcla con yeso agrícola (CaSO₄)","Cal viva (CaO)","Cal apagada (Ca(OH)₂)"]

# ===== Init DB/Migraciones (con manejo de errores) =====
try:
    # "Warm-up" rápido para detectar problemas de conexión temprano
    _conn = connect_db(); _conn.close()
except Exception as e:
    st.error(f"No se pudo conectar a la base de datos: {e}")
    st.stop()

try:
    create_users_table()
    create_trabajadores_table()
    create_jornadas_table()
    create_insumos_table()
    create_tarifas_table()
    create_cierres_tables()
    ensure_cierres_schema()
    create_fincas_table()  # ← NUEVO
except Exception as e:
    st.error(f"Error creando/migrando tablas: {e}")
    st.stop()

# ===== Login =====
def login():
    st.title("☕ Finca Cafetalera - Inicio de Sesión")
    tab = st.radio("Menú", ["Iniciar sesión","Crear cuenta"], horizontal=True)
    if tab == "Iniciar sesión":
        st.subheader("Ingresar")
        username = st.text_input("👤 Usuario")
        password = st.text_input("🔑 Contraseña", type="password")
        if st.button("Entrar"):
            try:
                if verify_user(username, password):
                    st.session_state.logged_in = True
                    st.session_state.user = username
                    st.rerun()
                else:
                    st.error("❌ Usuario o contraseña incorrectos")
            except Exception as e:
                st.error(f"Error al verificar usuario: {e}")
    else:
        st.subheader("Crear nuevo usuario")
        new_user = st.text_input("👤 Nuevo usuario")
        new_pass = st.text_input("🔑 Nueva contraseña", type="password")
        if st.button("Registrar"):
            try:
                add_user(new_user, new_pass)
                st.success("✅ Usuario creado. Ya puedes iniciar sesión.")
            except Exception as e:
                st.error(f"⚠️ No se pudo crear: {e}")

if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "user" not in st.session_state: st.session_state.user = ""
if not st.session_state.logged_in:
    login(); st.stop()

OWNER = st.session_state.user  # <- MUY IMPORTANTE

# ===== Fincas del usuario (catálogo) =====

def opciones_fincas():
    try:
        fin = get_all_fincas(OWNER)
    except Exception:
        fin = []
    if fin:
        return fin, False
    return LOTE_LISTA, True  # fallback hasta que el usuario agregue las suyas

FINCAS, FINCAS_FB = opciones_fincas()

# ===== Header =====
st.title("📋 Panel de Control - Finca Cafetalera")
st.write(f"👤 Usuario: **{OWNER}**")

# ===== Sidebar =====
with st.sidebar:
    st.markdown("## 🧭 Menú Principal")
    menu = option_menu(
        None,
        ["Registrar Jornada","Registrar Abono","Registrar Fumigación","Registrar Cal","Registrar Herbicida",
         "Ver Registros","Añadir Finca","Añadir Empleado","Reporte Semanal (Dom–Sáb)","Tarifas","Cierre Mensual"],
        icons=["calendar-check","fuel-pump","bezier","gem","droplet","journal-text","map","person-plus","bar-chart","cash","archive"],
        default_index=0,
        styles={
            "container":{"padding":"0!important","background":"rgba(0,0,0,0)"},
            "icon":{"font-size":"18px","color":"#10b981"},
            "nav-link":{"font-size":"15px","padding":"10px 12px","border-radius":"12px","margin":"6px 0",
                        "color":"#e5e7eb","background-color":"#111827","border":"1px solid #374151"},
            "nav-link-selected":{"background":"linear-gradient(90deg,#10b981,#059669)","color":"#fff",
                                 "font-weight":"700","border":"1px solid #10b981","box-shadow":"0 4px 18px rgba(16,185,129,.25)"},
        },
    )

# ===== Tarifas (por usuario) =====
if menu == "Tarifas":
    st.subheader("⚙️ Tarifas de tu cuenta")
    pago_dia_actual, pago_hex_actual = get_tarifas(OWNER)
    with st.form("form_tarifas"):
        pago_dia = st.number_input("Pago por DÍA (6 horas normales)", min_value=0.0, step=100.0, value=float(pago_dia_actual))
        pago_hora_extra = st.number_input("Pago por HORA EXTRA", min_value=0.0, step=50.0, value=float(pago_hex_actual))
        if st.form_submit_button("Guardar tarifas"):
            set_tarifas(OWNER, pago_dia, pago_hora_extra)
            st.success("✅ Tarifas guardadas para tu usuario.")

# ===== Añadir empleado =====
if menu == "Añadir Empleado":
    st.subheader("👥 Registrar Nuevo Empleado")
    with st.form("form_empleado"):
        nombre = st.text_input("Nombre del empleado")
        apellido = st.text_input("Apellido del empleado")
        if st.form_submit_button("Registrar trabajador"):
            if not nombre.strip() or not apellido.strip():
                st.warning("⚠️ Completa todos los campos.")
            else:
                ok = add_trabajador(nombre.strip(), apellido.strip(), OWNER)
                if ok:
                    st.success("✅ Empleado registrado.")
                else:
                    st.info("Ese empleado ya existe para tu cuenta.")

# ===== Añadir Finca =====
if menu == "Añadir Finca":
    st.subheader("🏞️ Registrar Nueva Finca / Lote")
    with st.form("form_finca"):
        nombre_finca = st.text_input("Nombre de la finca o lote")
        if st.form_submit_button("Registrar finca"):
            if not nombre_finca.strip():
                st.warning("⚠️ Escribe un nombre.")
            else:
                ok = add_finca(nombre_finca.strip(), OWNER)
                if ok:
                    st.success("✅ Finca registrada."); st.rerun()
                else:
                    st.info("Esa finca ya existe para tu cuenta.")
    # Listado simple
    try:
        fincas_list = get_all_fincas(OWNER)
    except Exception:
        fincas_list = []
    if fincas_list:
        st.markdown("### 🌱 Tus fincas/lotes")
        st.dataframe(pd.DataFrame({"Finca/Lote": fincas_list}), use_container_width=True)
    else:
        st.info("Aún no has agregado fincas.")

# ===== Cierre Mensual =====
if menu == "Cierre Mensual":
    st.subheader("🧾 Cierres Mensuales (contabilidad)")
    from calendar import monthrange
    hoy = datetime.date.today()
    c1, c2 = st.columns(2)
    anio = c1.number_input("Año", min_value=2020, max_value=2100, value=hoy.year, step=1)
    mes  = c2.selectbox("Mes", list(range(1,13)), index=hoy.month-1,
                        format_func=lambda m: datetime.date(1900, m, 1).strftime("%B").capitalize())
    mes_ini = datetime.date(int(anio), int(mes), 1)
    mes_fin = datetime.date(int(anio), int(mes), monthrange(int(anio), int(mes))[1])

    pago_dia, pago_hex = get_tarifas(OWNER)
    st.info(f"Rango: {mes_ini} → {mes_fin} | Tarifas: Día ₡{pago_dia:,.0f} • Hora extra ₡{pago_hex:,.0f}")

    jornadas = get_jornadas_between(mes_ini, mes_fin, OWNER)
    insumos  = get_insumos_between(mes_ini, mes_fin, OWNER)

    with st.expander("👷 Nómina del mes (preview)"):
        if jornadas:
            dfj = pd.DataFrame(jornadas, columns=["ID","Trabajador","Fecha","Lote","Actividad","Días","Horas Normales","Horas Extra"])
            dfj["Días"] = pd.to_numeric(dfj["Días"], errors="coerce").fillna(0).astype(int)
            dfj["Horas Extra"] = pd.to_numeric(dfj["Horas Extra"], errors="coerce").fillna(0.0)
            resumen = dfj.groupby("Trabajador", as_index=False)[["Días","Horas Extra"]].sum()
            resumen["Pago por Días"]    = resumen["Días"] * pago_dia
            resumen["Pago Horas Extra"] = resumen["Horas Extra"] * pago_hex
            resumen["Total"]            = resumen["Pago por Días"] + resumen["Pago Horas Extra"]
            st.dataframe(resumen.style.format({
                "Días":"{:,.0f}","Horas Extra":"{:,.1f}",
                "Pago por Días":"₡{:,.0f}","Pago Horas Extra":"₡{:,.0f}","Total":"₡{:,.0f}"
            }), use_container_width=True)
        else:
            st.info("No hay jornadas en ese mes.")

    with st.expander("🧪 Insumos del mes (preview)"):
        if insumos:
            dfi = pd.DataFrame(insumos, columns=["ID","Fecha","Lote","Tipo","Etapa","Producto","Dosis","Cantidad","Precio Unitario","Costo Total"])
            st.dataframe(dfi.style.format({"Precio Unitario":"₡{:,.0f}","Costo Total":"₡{:,.0f}"}), use_container_width=True)
        else:
            st.info("No hay insumos en ese mes.")

    overwrite = st.checkbox("Sobrescribir si ya existe", value=False)
    if st.button("💾 Crear cierre mensual", type="primary"):
        try:
            pid = crear_cierre_mensual(mes_ini, mes_fin, creado_por=OWNER, owner=OWNER,
                                       tarifa_dia=pago_dia, tarifa_hora_extra=pago_hex, overwrite=overwrite)
            st.success(f"Cierre creado (ID {pid})."); st.rerun()
        except Exception as e:
            st.error(str(e))

    st.markdown("### 📚 Cierres guardados")
    cierres = listar_cierres(OWNER)
    if cierres:
        dfc = pd.DataFrame(cierres, columns=["ID","Mes inicio","Mes fin","Creado por","Creado el","Total nómina","Total insumos","Total general"])
        st.dataframe(dfc.style.format({"Total nómina":"₡{:,.0f}","Total insumos":"₡{:,.0f}","Total general":"₡{:,.0f}"}), use_container_width=True)

        sel = st.selectbox("Ver detalle del cierre", [f"{r[0]} — {r[1]} a {r[2]}" for r in cierres])
        if sel:
            pago_id = int(sel.split(" — ",1)[0])
            nomina, insumos_det = leer_cierre_detalle(pago_id, OWNER)

            st.markdown("#### 👷 Nómina (detalle)")
            if nomina:
                dfn = pd.DataFrame(nomina, columns=["Trabajador","Días","Horas Extra","Monto días","Monto HEX","Total"])
                st.dataframe(dfn.style.format({
                    "Días":"{:,.0f}","Horas Extra":"{:,.1f}","Monto días":"₡{:,.0f}","Monto HEX":"₡{:,.0f}","Total":"₡{:,.0f}"
                }), use_container_width=True)
            else:
                st.info("Sin datos de nómina en este cierre.")

            st.markdown("#### 🧪 Insumos (detalle)")
            if insumos_det:
                dfi2 = pd.DataFrame(insumos_det, columns=["Fecha","Lote","Tipo","Producto","Etapa","Dosis","Cantidad","Precio Unitario","Costo Total"])
                st.dataframe(dfi2.style.format({"Precio Unitario":"₡{:,.0f}","Costo Total":"₡{:,.0f}"}), use_container_width=True)
            else:
                st.info("Sin insumos en este cierre.")
    else:
        st.info("Aún no hay cierres guardados.")

# ============================
# Registrar Jornada (con owner)
# ============================
if menu == "Registrar Jornada":
    st.subheader("🧑‍🌾 Registrar Jornada Laboral")

    trabajadores_disponibles = get_all_trabajadores(OWNER)
    if FINCAS_FB:
        st.caption("ℹ️ Usando lista de fincas de ejemplo. Ve a **Añadir Finca** para registrar las tuyas.")
    if not trabajadores_disponibles:
        st.warning("⚠️ No hay trabajadores registrados. Agrega uno primero.")
    else:
        # ---- Formulario de alta ----
        with st.form("form_jornada"):
            trabajador = st.selectbox("Selecciona un trabajador", trabajadores_disponibles)
            fecha = st.date_input("Fecha de trabajo", datetime.date.today())
            lote = st.selectbox("Lote o parcela", FINCAS)
            actividad = st.selectbox("Tipo de actividad", ACTIVIDADES)
            dias = st.number_input("Días trabajados", min_value=0, max_value=31, step=1)
            horas_extra = st.number_input("Horas extra trabajadas", min_value=0.0, step=0.5)

            horas_normales = int(dias) * 6  # 6h por día
            st.info(f"🕒 Horas normales calculadas automáticamente: {horas_normales} horas")

            if st.form_submit_button("Guardar jornada"):
                add_jornada(
                    trabajador=trabajador,
                    fecha=str(fecha),
                    lote=lote,
                    actividad=actividad,
                    dias=int(dias),
                    horas_normales=horas_normales,
                    horas_extra=float(horas_extra),
                    owner=OWNER,  # ← separación por usuario
                )
                st.success("✅ Jornada registrada"); st.rerun()

       # ---- Edición del último registro del mismo día para este usuario ----
        with st.expander("✏️ Editar último registro de jornada"):
            ultima_jornada = get_last_jornada_by_date(fecha=str(fecha), owner=OWNER)

            if ultima_jornada:
                # Soporta 8 u 9 columnas (según si tu tabla ya tiene 'owner')
                if len(ultima_jornada) == 9:
                    (
                        jornada_id, _owner, trabajador_actual, fecha_actual,
                        lote_actual, actividad_actual, dias_actual,
                        horas_normales_actual, horas_extra_actual
                    ) = ultima_jornada
                elif len(ultima_jornada) == 8:
                    (
                        jornada_id, trabajador_actual, fecha_actual,
                        lote_actual, actividad_actual, dias_actual,
                        horas_normales_actual, horas_extra_actual
                    ) = ultima_jornada
                else:
                    st.error(f"Formato inesperado de jornada (campos={len(ultima_jornada)}).")
                    st.stop()

                # Trabajador
                try:
                    idx_trab = trabajadores_disponibles.index(trabajador_actual)
                except ValueError:
                    idx_trab = 0
                nuevo_trabajador = st.selectbox("Nuevo trabajador", trabajadores_disponibles, index=idx_trab)

                # Fecha segura (sin 'format' para compatibilidad)
                try:
                    f_str = str(fecha_actual)[:10]
                    default_date = datetime.datetime.strptime(f_str, "%Y-%m-%d").date()
                except Exception:
                    default_date = datetime.date.today()
                nueva_fecha = st.date_input("Nueva fecha de trabajo", default_date)

                # Lote
                try:
                    idx_lote = FINCAS.index(lote_actual)
                except ValueError:
                    idx_lote = 0
                nuevo_lote = st.selectbox("Nuevo lote", FINCAS, index=idx_lote)

                # Actividad
                try:
                    idx_act = ACTIVIDADES.index(actividad_actual)
                except ValueError:
                    idx_act = 0
                nueva_actividad = st.selectbox("Nueva actividad", ACTIVIDADES, index=idx_act)

                # Conversión segura de numéricos + límites
                try:
                    val_dias = int(float(dias_actual))
                except (TypeError, ValueError):
                    val_dias = 0
                val_dias = max(0, min(val_dias, 31))  # clamp a 0..31

                try:
                    val_hex = float(horas_extra_actual)
                except (TypeError, ValueError):
                    val_hex = 0.0
                val_hex = max(0.0, val_hex)  # no negativas

                nuevos_dias = st.number_input(
                    "Nuevos días trabajados",
                    value=val_dias,
                    min_value=0,
                    max_value=31,
                    step=1,
                )
                nuevas_horas_extra = st.number_input(
                    "Nuevas horas extra",
                    value=val_hex,
                    min_value=0.0,
                    step=0.5,
                )

                nuevas_horas_normales = int(nuevos_dias) * 6
                st.info(f"🕒 Nuevas horas normales: {nuevas_horas_normales} horas")

                if st.button("Actualizar jornada"):
                    update_jornada(
                        jornada_id,                               # id (posicional)
                        nuevo_trabajador,                         # trabajador
                        nueva_fecha.strftime("%Y-%m-%d"),         # fecha
                        nuevo_lote,                               # lote
                        nueva_actividad,                          # actividad
                        int(nuevos_dias),                         # días
                        int(nuevos_dias) * 6,                     # horas_normales
                        float(nuevas_horas_extra),                # horas_extra
                        OWNER,                                    # owner (multi-tenant)
                    )
                    st.success("✅ Jornada actualizada correctamente.")
                    st.rerun()
            else:
                st.info("No hay registros de jornada para editar.")


# ===== Registrar Abono =====
if menu == "Registrar Abono":
    st.subheader("🌿 Registrar Aplicación de Abono")
    if FINCAS_FB:
        st.caption("ℹ️ Usando lista de fincas de ejemplo. Ve a **Añadir Finca** para registrar las tuyas.")
    with st.form("form_abonado"):
        fecha_abono = st.date_input("Fecha de aplicación de abono", datetime.date.today())
        lote_abono = st.selectbox("Lote o parcela", FINCAS)
        etapa = st.selectbox("Etapa de abonado", ETAPAS_ABONO)
        producto = st.text_input("Nombre del producto (ej: 18-5-15, Multimag)")
        dosis = st.number_input("Dosis aplicada (g/planta)", min_value=0.0, step=0.1)
        cantidad = st.number_input("Cantidad aplicada (sacos)", min_value=0.0, step=0.5)
        precio_unitario = st.number_input("Precio por saco (₡)", min_value=0.0, step=100.0)
        if cantidad > 0 and precio_unitario > 0:
            st.info(f"💰 Costo total estimado: ₡{(cantidad*precio_unitario):,.2f}")
        if st.form_submit_button("Guardar aplicación de abono"):
            add_insumo(str(fecha_abono), lote_abono, "Abono", etapa, producto, dosis, cantidad, precio_unitario, OWNER)
            st.success("✅ Abono registrado"); st.rerun()

    with st.expander("✏️ Editar último registro de abono"):
        ultima = get_last_abono_by_date(str(fecha_abono), OWNER)
        if ultima:
            (iid, fec, lote, tipo, etapa_act, prod_act, dosis_act, cant_act, precio_act, costo) = ultima
            try: idx_lote = FINCAS.index(lote)
            except ValueError: idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", FINCAS, index=idx_lote)
            try: idx_et = ETAPAS_ABONO.index(etapa_act)
            except ValueError: idx_et = 0
            nueva_etapa = st.selectbox("Nueva etapa", ETAPAS_ABONO, index=idx_et)
            nueva_fecha = st.date_input("Nueva fecha", datetime.datetime.strptime(str(fec)[:10], "%Y-%m-%d").date())
            nuevo_prod = st.text_input("Nuevo producto", value=prod_act)
            nueva_dosis = st.number_input("Nueva dosis (g/planta)", value=float(dosis_act or 0), min_value=0.0, step=0.1)
            nueva_cant = st.number_input("Nueva cantidad (sacos)", value=float(cant_act or 0), min_value=0.0, step=0.5)
            nuevo_precio = st.number_input("Nuevo precio por saco (₡)", value=float(precio_act or 0), min_value=0.0, step=100.0)
            if st.button("Actualizar abono"):
                update_abono(iid, nueva_fecha.strftime("%Y-%m-%d"), nuevo_lote, nueva_etapa, nuevo_prod, nueva_dosis, nueva_cant, nuevo_precio, OWNER)
                st.success("✅ Abono actualizado"); st.rerun()
        else:
            st.info("No hay registros de abono para editar.")

# ===== Ver Registros =====
if menu == "Ver Registros":
    st.subheader("📊 Registros de Jornadas e Insumos")
    pago_dia, pago_hex = get_tarifas(OWNER)
    st.info(f"Tarifas actuales → Día (6h): ₡{pago_dia:,.0f} | Hora extra: ₡{pago_hex:,.0f}")

    with st.expander("📋 Ver Jornadas Registradas"):
        jornadas = get_all_jornadas(OWNER)
        if jornadas:
            df_j = pd.DataFrame(jornadas, columns=["ID","Trabajador","Fecha","Lote","Actividad","Días","Horas Normales","Horas Extra"])
            try: df_j["Fecha"] = pd.to_datetime(df_j["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
            except Exception: pass
            df_j["Días"] = pd.to_numeric(df_j["Días"], errors="coerce").fillna(0).astype(int)
            df_j["Horas Extra"] = pd.to_numeric(df_j["Horas Extra"], errors="coerce").fillna(0.0)

            resumen = df_j.groupby("Trabajador", as_index=False).agg({"Días":"sum","Horas Extra":"sum"})
            resumen = resumen.rename(columns={"Días":"Días trabajados"})
            resumen["Días a pagar"] = resumen["Días trabajados"]
            resumen["Pago por Días"] = resumen["Días a pagar"] * pago_dia
            resumen["Pago Horas Extra"] = resumen["Horas Extra"] * pago_hex
            resumen["Total Ganado"] = resumen["Pago por Días"] + resumen["Pago Horas Extra"]

            st.markdown("### 👥 Resumen por Trabajador")
            cols = ["Trabajador","Días trabajados","Días a pagar","Horas Extra","Pago por Días","Pago Horas Extra","Total Ganado"]
            st.dataframe(resumen[cols].style.format({
                "Días trabajados":"{:,.0f}","Días a pagar":"{:,.0f}","Horas Extra":"{:,.1f}",
                "Pago por Días":"₡{:,.0f}","Pago Horas Extra":"₡{:,.0f}","Total Ganado":"₡{:,.0f}"
            }), use_container_width=True)

            st.markdown("### 🧾 Detalle de Jornadas")
            st.dataframe(df_j[["Fecha","Trabajador","Lote","Actividad","Días","Horas Extra"]], use_container_width=True)
        else:
            st.info("No hay jornadas registradas aún.")

    # Insumos por tipo
    tipos = {"Abono":"🌿 Ver Abonos","Fumigación":"🧪 Ver Fumigaciones","Cal":"🧱 Ver Cal","Herbicida":"🌾 Ver Herbicidas"}
    for tipo, titulo in tipos.items():
        with st.expander(titulo):
            conn = connect_db(); cur = conn.cursor()
            cur.execute(
                """
                SELECT id, fecha, lote, tipo, etapa, producto, dosis, cantidad, precio_unitario, costo_total
                FROM insumos
                WHERE owner=%s AND tipo=%s
                ORDER BY fecha DESC, id DESC;
                """,
                (OWNER, tipo),
            )
            regs = cur.fetchall(); conn.close()
            if regs:
                df_i = pd.DataFrame(regs, columns=["ID","Fecha","Lote","Tipo","Etapa","Producto","Dosis","Cantidad","Precio Unitario","Costo Total"])
                try: df_i["Fecha"] = pd.to_datetime(df_i["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
                except Exception: pass
                if tipo == "Fumigación":
                    df_i = df_i.rename(columns={"Etapa":"Plaga/Control","Cantidad":"Litros","Precio Unitario":"Precio por litro (₡)"})
                elif tipo == "Herbicida":
                    df_i = df_i.rename(columns={"Etapa":"Tipo de herbicida","Cantidad":"Litros","Precio Unitario":"Precio por litro (₡)"})
                elif tipo == "Cal":
                    df_i = df_i.rename(columns={"Etapa":"Tipo de cal","Producto":"Presentación","Cantidad":"Sacos (45 kg)","Precio Unitario":"Precio por saco (₡)"})
                elif tipo == "Abono":
                    df_i = df_i.rename(columns={"Etapa":"Etapa de abonado","Dosis":"Dosis (g/planta)","Cantidad":"Sacos","Precio Unitario":"Precio por saco (₡)"})
                for col in ["Litros","Sacos (45 kg)","Sacos","Cantidad","Dosis","Dosis (g/planta)","Precio por litro (₡)","Precio por saco (₡)","Precio Unitario","Costo Total"]:
                    if col in df_i.columns: df_i[col] = pd.to_numeric(df_i[col], errors="coerce")
                money = [c for c in ["Precio por litro (₡)","Precio por saco (₡)","Precio Unitario","Costo Total"] if c in df_i.columns]
                qty   = [c for c in ["Litros","Sacos (45 kg)","Sacos","Cantidad"] if c in df_i.columns]
                dose  = [c for c in ["Dosis","Dosis (g/planta)"] if c in df_i.columns]
                fmt = {}; fmt.update({c:"₡{:,.0f}" for c in money}); fmt.update({c:"{:,.1f}" for c in qty}); fmt.update({c:"{:,.0f}" for c in dose})
                st.dataframe(df_i.style.format(fmt), use_container_width=True)
            else:
                st.info(f"No hay insumos de {tipo.lower()}.")

# ===== Registrar Fumigación =====
if menu == "Registrar Fumigación":
    st.subheader("🧪 Registrar Fumigación")
    if FINCAS_FB:
        st.caption("ℹ️ Usando lista de fincas de ejemplo. Ve a **Añadir Finca** para registrar las tuyas.")
    with st.form("form_fumigacion"):
        fecha_fum = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_fum = st.selectbox("Lote o parcela", FINCAS)
        producto = st.text_input("Nombre del producto (ej: Fungicida X, Insecticida Y)")
        plaga = st.text_input("Tipo de plaga o control (ej: Roya, Broca, Hongos)")
        dosis = st.text_input("Dosis aplicada por estañon (ej: 50 ml/estañon)")
        litros = st.number_input("Litros aplicados por lote o parcela", min_value=0.0, step=0.5)
        precio_litro = st.number_input("Precio por litro de fumigación (₡)", min_value=0.0, step=100.0)
        if litros > 0 and precio_litro > 0:
            st.info(f"💰 Costo total estimado: ₡{(litros*precio_litro):,.2f}")
        if st.form_submit_button("Guardar fumigación"):
            add_insumo(str(fecha_fum), lote_fum, "Fumigación", plaga, producto, dosis, litros, precio_litro, OWNER)
            st.success("✅ Fumigación registrada"); st.rerun()

    with st.expander("✏️ Editar último registro de fumigación"):
        ult = get_last_fumigacion_by_date(str(fecha_fum), OWNER)
        if ult:
            (iid, fec, lote, tipo, plaga_act, prod_act, dosis_act, litros_act, precio_u, costo) = ult
            try: idx_lote = FINCAS.index(lote)
            except ValueError: idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", FINCAS, index=idx_lote)
            nueva_plaga = st.text_input("Nuevo plaga/control", value=plaga_act)
            nuevo_prod  = st.text_input("Nuevo producto", value=prod_act)
            nueva_dosis = st.text_input("Nueva dosis", value=dosis_act)
            nuevos_litros = st.number_input("Nuevos litros", value=float(litros_act or 0), min_value=0.0, step=0.5)
            nuevo_precio = st.number_input("Nuevo precio por litro", value=float(precio_u or 0), min_value=0.0, step=100.0)
            # Normalizar fecha a string segura
            try:
                fec_str = datetime.datetime.strptime(str(fec)[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                fec_str = str(datetime.date.today())
            if st.button("Actualizar fumigación"):
                update_fumigacion(iid, fec_str, nuevo_lote, nueva_plaga, nuevo_prod, nueva_dosis, nuevos_litros, nuevo_precio, OWNER)
                st.success("✅ Fumigación actualizada"); st.rerun()
        else:
            st.info("No hay registros de fumigación para editar.")

# ===== Registrar Cal =====
if menu == "Registrar Cal":
    st.subheader("🧱 Registrar Aplicación de Cal")
    if FINCAS_FB:
        st.caption("ℹ️ Usando lista de fincas de ejemplo. Ve a **Añadir Finca** para registrar las tuyas.")
    with st.form("form_cal"):
        fecha_cal = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_cal = st.selectbox("Lote o parcela", FINCAS)
        tipo_cal = st.selectbox("Tipo de cal utilizada", TIPOS_CAL)
        cantidad = st.number_input("Cantidad aplicada (sacos 45 kg)", min_value=0.0, step=0.5)
        precio_saco = st.number_input("Precio por saco (₡)", min_value=0.0, step=100.0)
        if cantidad > 0 and precio_saco > 0:
            st.info(f"💰 Costo total estimado: ₡{(cantidad*precio_saco):,.2f}")
        if st.form_submit_button("Guardar aplicación de cal"):
            add_insumo(str(fecha_cal), lote_cal, "Cal", tipo_cal, "Saco 45 kg", "", cantidad, precio_saco, OWNER)
            st.success("✅ Cal registrada"); st.rerun()

    with st.expander("✏️ Editar último registro de cal"):
        ult = get_last_cal_by_date(str(fecha_cal), OWNER)
        if ult:
            (iid, fec, lote, tipo, etapa, prod, dosis, cant, precio_u, costo) = ult
            try: idx_lote = FINCAS.index(lote)
            except ValueError: idx_lote = 0
            try: idx_tipo = TIPOS_CAL.index(tipo)
            except ValueError: idx_tipo = 0
            nuevo_lote = st.selectbox("Nuevo lote", FINCAS, index=idx_lote)
            nuevo_tipo = st.selectbox("Nuevo tipo de cal", TIPOS_CAL, index=idx_tipo)
            nueva_cant = st.number_input("Nueva cantidad (sacos)", value=float(cant or 0), min_value=0.0, step=0.5)
            nuevo_precio = st.number_input("Nuevo precio por saco (₡)", value=float(precio_u or 0), min_value=0.0, step=100.0)
            try:
                fec_str = datetime.datetime.strptime(str(fec)[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                fec_str = str(datetime.date.today())
            if st.button("Actualizar cal"):
                update_cal(iid, fec_str, nuevo_lote, nuevo_tipo, "Saco 45 kg", "", nueva_cant, nuevo_precio, OWNER)
                st.success("✅ Cal actualizada"); st.rerun()
        else:
            st.info("No hay registros de cal para editar.")

# ===== Registrar Herbicida =====
if menu == "Registrar Herbicida":
    st.subheader("🌾 Registrar Aplicación de Herbicida")
    if FINCAS_FB:
        st.caption("ℹ️ Usando lista de fincas de ejemplo. Ve a **Añadir Finca** para registrar las tuyas.")
    with st.form("form_herbicida"):
        fecha_herb = st.date_input("Fecha de aplicación", datetime.date.today())
        lote_herb = st.selectbox("Lote o parcela", FINCAS)
        tipo_herb = st.selectbox("Tipo de herbicida", TIPOS_HERBICIDA)
        producto  = st.text_input("Nombre del producto (ej: Glifosato 41%, Paraquat 20%)")
        dosis     = st.text_input("Dosis aplicada (ej: 80 ml/estañón)")
        litros    = st.number_input("Litros aplicados", min_value=0.0, step=0.5)
        precio_l  = st.number_input("Precio por litro (₡)", min_value=0.0, step=100.0)
        if litros > 0 and precio_l > 0:
            st.info(f"💰 Costo total estimado: ₡{(litros*precio_l):,.2f}")
        if st.form_submit_button("Guardar aplicación de herbicida"):
            add_insumo(str(fecha_herb), lote_herb, "Herbicida", tipo_herb, producto, dosis, litros, precio_l, OWNER)
            st.success("✅ Herbicida registrado"); st.rerun()

    with st.expander("✏️ Editar último registro de herbicida"):
        ult = get_last_herbicida_by_date(str(fecha_herb), OWNER)
        if ult:
            (iid, fec, lote, tipo, etapa, prod, dosis_act, cant, precio_u, costo) = ult
            try: idx_lote = FINCAS.index(lote)
            except ValueError: idx_lote = 0
            nuevo_lote = st.selectbox("Nuevo lote", FINCAS, index=idx_lote)
            try: idx_tipo = TIPOS_HERBICIDA.index(tipo)
            except ValueError: idx_tipo = 0
            nuevo_tipo = st.selectbox("Nuevo tipo de herbicida", TIPOS_HERBICIDA, index=idx_tipo)
            nuevo_prod = st.text_input("Nuevo producto", value=prod)
            nueva_dos  = st.text_input("Nueva dosis", value=dosis_act)
            nueva_cant = st.number_input("Nueva cantidad (litros)", value=float(cant or 0), min_value=0.0, step=0.5)
            nuevo_pre  = st.number_input("Nuevo precio por litro (₡)", value=float(precio_u or 0), min_value=0.0, step=100.0)
            try:
                fec_str = datetime.datetime.strptime(str(fec)[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                fec_str = str(datetime.date.today())
            if st.button("Actualizar herbicida"):
                update_herbicida(iid, fec_str, nuevo_lote, nuevo_tipo, nuevo_prod, nueva_dos, nueva_cant, nuevo_pre, OWNER)
                st.success("✅ Herbicida actualizado"); st.rerun()
        else:
            st.info("No hay registros de herbicida para editar.")

# ===== Reporte Semanal =====
if menu == "Reporte Semanal (Dom–Sáb)":
    st.subheader("💵 Reporte Semanal de Salarios (Domingo a Sábado)")
    hoy = datetime.date.today()
    fecha_ref = st.date_input("Selecciona una fecha dentro de la semana", hoy)

    def rango_semana_dom_sab(d: datetime.date):
        dias_a_dom = (d.weekday() + 1) % 7
        dom = d - datetime.timedelta(days=dias_a_dom)
        sab = dom + datetime.timedelta(days=6)
        return dom, sab

    inicio_sem, fin_sem = rango_semana_dom_sab(fecha_ref)
    st.info(f"📅 Semana: **{inicio_sem}** a **{fin_sem}** (Dom–Sáb)")

    pago_dia, pago_hex = get_tarifas(OWNER)
    st.info(f"Tarifas → Día (6h): ₡{pago_dia:,.0f} | Hora extra: ₡{pago_hex:,.0f}")

    jornadas = get_all_jornadas(OWNER)
    if not jornadas:
        st.info("No hay jornadas registradas aún.")
    else:
        df = pd.DataFrame(jornadas, columns=["ID","Trabajador","Fecha","Lote","Actividad","Días","Horas Normales","Horas Extra"])
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        df["Horas Extra"] = pd.to_numeric(df["Horas Extra"], errors="coerce").fillna(0.0)
        df["Días"] = pd.to_numeric(df["Días"], errors="coerce").fillna(0).astype(int)

        mask = (df["Fecha"].dt.date >= inicio_sem) & (df["Fecha"].dt.date <= fin_sem)
        df_sem = df.loc[mask].copy()
        if df_sem.empty:
            st.info("No hay jornadas en la semana seleccionada.")
        else:
            resumen = df_sem.groupby("Trabajador", as_index=False).agg({"Días":"sum","Horas Extra":"sum"})
            resumen = resumen.rename(columns={"Días":"Días trabajados"})
            resumen["Días a pagar"] = resumen["Días trabajados"]
            resumen["Pago por Días"] = resumen["Días a pagar"] * pago_dia
            resumen["Pago Horas Extra"] = resumen["Horas Extra"] * pago_hex
            resumen["Total a Pagar"] = resumen["Pago por Días"] + resumen["Pago Horas Extra"]

            st.markdown("### 📋 Jornadas de la semana (detalle)")
            df_sem_orden = df_sem.sort_values(["Trabajador","Fecha"]).copy()
            df_detalle = df_sem_orden[["Fecha","Trabajador","Lote","Actividad","Días","Horas Extra"]].copy()
            df_detalle.rename(columns={"Días":"Días trabajados"}, inplace=True)
            df_detalle["Fecha"] = df_detalle["Fecha"].dt.strftime("%Y-%m-%d")
            df_detalle["Días a pagar"] = df_detalle["Días trabajados"]
            st.dataframe(df_detalle, use_container_width=True)

            st.markdown("### 👥 Resumen por trabajador")
            cols = ["Trabajador","Días trabajados","Días a pagar","Horas Extra","Pago por Días","Pago Horas Extra","Total a Pagar"]
            st.dataframe(resumen[cols].style.format({
                "Días trabajados":"{:,.0f}","Días a pagar":"{:,.0f}","Horas Extra":"{:,.1f}",
                "Pago por Días":"₡{:,.0f}","Pago Horas Extra":"₡{:,.0f}","Total a Pagar":"₡{:,.0f}"
            }), use_container_width=True)

            total_dias = resumen["Pago por Días"].sum()
            total_extras = resumen["Pago Horas Extra"].sum()
            total_semana = resumen["Total a Pagar"].sum()
            st.markdown("### 🧮 Totales de la semana")
            st.write(f"- **Pago por días (₡):** {total_dias:,.0f}")
            st.write(f"- **Pago por horas extra (₡):** {total_extras:,.0f}")
            st.write(f"- **Total a pagar (₡):** {total_semana:,.0f}")

            # Descargas
            csv_res = resumen[cols].to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Descargar resumen semanal (CSV)", data=csv_res,
                               file_name=f"reporte_semanal_{inicio_sem}_a_{fin_sem}.csv", mime="text/csv")

            df_detalle["Pago por Días (₡)"] = (df_detalle["Días a pagar"] * pago_dia).round(2)
            df_detalle["Pago Horas Extra (₡)"] = (df_detalle["Horas Extra"] * pago_hex).round(2)
            df_detalle["Total Fila (₡)"] = df_detalle["Pago por Días (₡)"] + df_detalle["Pago Horas Extra (₡)"]
            csv_det = df_detalle.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Descargar detalle semanal (CSV)", data=csv_det,
                               file_name=f"reporte_semanal_detalle_{inicio_sem}_a_{fin_sem}.csv", mime="text/csv")

            # PDF
            resumen_min = resumen[["Trabajador","Días a pagar","Horas Extra","Total a Pagar"]].copy()
            def pdf_resumen(res_df, ini, fin):
                buffer = BytesIO(); c = canvas.Canvas(buffer, pagesize=letter)
                width, height = letter
                c.setFont("Helvetica-Bold", 14); c.drawString(50, height-50, "Resumen por trabajador")
                c.setFont("Helvetica", 11); c.drawString(50, height-70, f"Semana: {ini} a {fin} (Dom–Sáb)")
                y = height-110; c.setFont("Helvetica-Bold", 11)
                c.drawString(50,y,"Trabajador"); c.drawString(250,y,"Días a pagar"); c.drawString(360,y,"Horas Extra"); c.drawString(460,y,"Total (₡)")
                c.line(50,y-3,560,y-3); y -= 20; c.setFont("Helvetica",10)
                for _, row in res_df.iterrows():
                    nombre = str(row["Trabajador"]); nombre = (nombre[:34]+"…") if len(nombre)>35 else nombre
                    c.drawString(50,y,nombre)
                    c.drawRightString(330,y,f"{row['Días a pagar']:.0f}")
                    c.drawRightString(430,y,f"{row['Horas Extra']:.1f}")
                    c.drawRightString(560,y,f"{row['Total a Pagar']:,.0f}")
                    y -= 18
                    if y < 60:
                        c.showPage(); y = height-50; c.setFont("Helvetica-Bold", 11)
                        c.drawString(50,y,"Trabajador"); c.drawString(250,y,"Días a pagar"); c.drawString(360,y,"Horas Extra"); c.drawString(460,y,"Total (₡)")
                        c.line(50,y-3,560,y-3); y -= 20; c.setFont("Helvetica",10)
                c.save(); pdf = buffer.getvalue(); buffer.close(); return pdf
            pdf_bytes = pdf_resumen(resumen_min, inicio_sem, fin_sem)
            st.download_button("⬇️ Descargar resumen por trabajador (PDF)", data=pdf_bytes,
                               file_name=f"resumen_trabajador_{inicio_sem}_a_{fin_sem}.pdf", mime="application/pdf")





























