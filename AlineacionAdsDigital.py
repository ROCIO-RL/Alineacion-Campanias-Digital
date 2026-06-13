import streamlit as st
import pandas as pd
import snowflake.connector
import re
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="Alineación Digital Ads", layout="wide")
st.title("Alineación Digital - Completar Ads")

SNOWFLAKE_CONFIG = {
    "user": st.secrets["snowflake"]["user"],
    "account": st.secrets["snowflake"]["account"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
    "role": st.secrets["snowflake"]["role"]
}


def get_private_key():
    private_key_data = st.secrets["snowflake"]["private_key"]
    p_key = serialization.load_pem_private_key(private_key_data.encode(), password=None)
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"],
        private_key=get_private_key()
    )


@st.cache_data(ttl=3600)
def obtener_catalogo_productos():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT DISTINCT
                epc.PRONOMBRE AS PRODUCTO_BASE,
                epc.AGPPAUTANOMBRE AS AGRUPACION_PAUTA,
                epc.MRCNOMBRE AS MARCA
            FROM PRD_CNS_MX.DM.DIM_PRODUCTO epc
            WHERE epc.AGPPAUTANOMBRE <> 'FUERA DE LINEA'
            ORDER BY PRODUCTO_BASE
        """, conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=3600)
def obtener_campanas_historicas():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT DISTINCT DES_CAMPANA
            FROM PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
            WHERE DES_CAMPANA IS NOT NULL AND TRIM(DES_CAMPANA) <> ''
            ORDER BY DES_CAMPANA
        """, conn)
    finally:
        conn.close()
    return sorted(df["DES_CAMPANA"].dropna().unique().tolist())


@st.cache_data(ttl=3600)
def obtener_anuncios_historicos():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT DISTINCT
                DES_ANUNCIO_LIMPIO,
                DES_CAMPANA,
                DES_PRODUCTO_BASE,
                DES_AGRUPACION_PAUTA,
                DES_MARCA
            FROM PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
            WHERE DES_ANUNCIO_LIMPIO IS NOT NULL AND TRIM(DES_ANUNCIO_LIMPIO) <> ''
            ORDER BY DES_ANUNCIO_LIMPIO
        """, conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=300)
def obtener_ads_pendientes():
    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                ID_AD, ID_AD_GROUP, COD_PLATAFORMA, DES_AD_NAME_RAW,
                DES_CAMPANA, DES_ANUNCIO_LIMPIO, DES_PRODUCTO_BASE,
                DES_AGRUPACION_PAUTA, DES_MARCA
            FROM PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
            WHERE
                DES_CAMPANA IS NULL OR TRIM(DES_CAMPANA) = ''
                OR DES_ANUNCIO_LIMPIO IS NULL OR TRIM(DES_ANUNCIO_LIMPIO) = ''
                OR DES_PRODUCTO_BASE IS NULL OR TRIM(DES_PRODUCTO_BASE) = ''
            ORDER BY ID_AD
        """, conn)
    finally:
        conn.close()
    return df


def validar_texto(texto):
    return bool(re.match(r'^[A-Za-z0-9 _-]+$', texto))


def validar_registro(r):
    errores = []
    if not r["DES_CAMPANA"]:
        errores.append("Campaña vacía")
    if not r["DES_ANUNCIO_LIMPIO"]:
        errores.append("Anuncio vacío")
    if not r["DES_PRODUCTO_BASE"]:
        errores.append("Producto Base vacío")
    if r["DES_CAMPANA"] and not validar_texto(r["DES_CAMPANA"]):
        errores.append("Campaña contiene caracteres inválidos")
    if r["DES_ANUNCIO_LIMPIO"] and not validar_texto(r["DES_ANUNCIO_LIMPIO"]):
        errores.append("Anuncio contiene caracteres inválidos")
    return errores


def actualizar_registros(df_actualizar, usuario):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        for _, row in df_actualizar.iterrows():
            cursor.execute("""
                UPDATE PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
                SET
                    DES_CAMPANA = %s,
                    DES_ANUNCIO_LIMPIO = %s,
                    DES_PRODUCTO_BASE = %s,
                    DES_AGRUPACION_PAUTA = %s,
                    DES_MARCA = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_USR = %s
                WHERE ID_AD = %s
            """, (
                row["DES_CAMPANA"], row["DES_ANUNCIO_LIMPIO"],
                row["DES_PRODUCTO_BASE"], row["DES_AGRUPACION_PAUTA"],
                row["DES_MARCA"], usuario, row["ID_AD"]
            ))
        conn.commit()
        return True, "Registros actualizados correctamente"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cursor.close()
        conn.close()


# CARGA DE DATOS
with st.spinner("Cargando..."):
    productos_df       = obtener_catalogo_productos()
    campanas_historicas = obtener_campanas_historicas()
    anuncios_df        = obtener_anuncios_historicos()
    ads_df             = obtener_ads_pendientes()

if ads_df.empty:
    st.success("No existen registros pendientes")
    st.stop()

usuario = st.secrets["snowflake"]["user"]
productos_lista = sorted(productos_df["PRODUCTO_BASE"].dropna().unique().tolist())

# SESSION STATE GLOBAL
if "confirmar_guardado" not in st.session_state:
    st.session_state.confirmar_guardado = False
if "registros_datos" not in st.session_state:
    st.session_state.registros_datos = {}


@st.fragment
def render_registro(idx, row, productos_df, campanas_historicas, anuncios_df, productos_lista):

    n = idx + 1
    id_ad = row["ID_AD"]
    aid = id_ad  # sufijo de keys basado en ID real, no posición

    k_prev_ad   = f"_prev_anuncio_{aid}"
    k_ad_sel    = f"anuncio_select_{aid}"
    k_camp_sel  = f"campana_select_{aid}"
    k_nueva     = f"nueva_campana_{aid}"
    k_producto  = f"producto_{aid}"
    k_anuncio   = f"anuncio_texto_{aid}"
    k_confirmar = f"confirmar_ind_{aid}"

    # Inicializar estado
    if k_prev_ad not in st.session_state:
        st.session_state[k_prev_ad] = ""
    if k_confirmar not in st.session_state:
        st.session_state[k_confirmar] = False

    # Pre-cargar valores del row solo la primera vez
    if k_anuncio not in st.session_state:
        val = row.get("DES_ANUNCIO_LIMPIO", "")
        st.session_state[k_anuncio] = val if pd.notnull(val) else ""

    if k_producto not in st.session_state:
        raw_p = row.get("DES_PRODUCTO_BASE", "")
        prod_init = str(raw_p).strip() if pd.notnull(raw_p) else ""
        if prod_init in productos_lista:
            st.session_state[k_producto] = prod_init
        else:
            prod_upper = prod_init.upper()
            match = next((p for p in productos_lista if p.upper() == prod_upper), None)
            st.session_state[k_producto] = match if match else ""

    if k_camp_sel not in st.session_state:
        camp = row.get("DES_CAMPANA", "")
        st.session_state[k_camp_sel] = camp if (pd.notnull(camp) and camp in campanas_historicas) else ""

    # Auto-rellenar cuando cambia la selección de anuncio existente
    current_ad = st.session_state.get(k_ad_sel, "")
    if current_ad and current_ad != st.session_state[k_prev_ad]:
        ad_rows = anuncios_df[anuncios_df["DES_ANUNCIO_LIMPIO"] == current_ad]
        if not ad_rows.empty:
            ad_info = ad_rows.iloc[0]
            campana_ad = ad_info.get("DES_CAMPANA", "")
            if pd.notnull(campana_ad) and campana_ad in campanas_historicas:
                st.session_state[k_camp_sel] = campana_ad
            st.session_state[k_anuncio] = current_ad
            raw_prod = ad_info.get("DES_PRODUCTO_BASE", "")
            prod_ad = str(raw_prod).strip() if pd.notnull(raw_prod) else ""
            if prod_ad:
                # búsqueda exacta primero, luego insensible a mayúsculas
                if prod_ad in productos_lista:
                    st.session_state[k_producto] = prod_ad
                else:
                    prod_upper = prod_ad.upper()
                    match = next((p for p in productos_lista if p.upper() == prod_upper), None)
                    if match:
                        st.session_state[k_producto] = match
    st.session_state[k_prev_ad] = current_ad

    # --- ENCABEZADO con botón guardar superior derecho ---
    hc1, _, hc3 = st.columns([5, 1, 0.5])
    with hc1:
        st.markdown(f"### Registro {n}")
    with hc3:
        save_top = st.button("💾", key=f"save_top_{aid}", help="Guardar este registro")

    col1, col2 = st.columns([1, 2])

    with col1:
        st.write(f"**Id Anuncio:** {id_ad}")
        st.write(f"**Anuncio:** {row['DES_AD_NAME_RAW']}")
        st.write(f"**Plataforma:** {row['COD_PLATAFORMA']}")

    with col2:
        # --- Selección de campaña ---
        cc1, cc2 = st.columns(2)
        with cc1:
            opciones_campana = ["", "➕ Nueva campaña"] + campanas_historicas
            campana_sel = st.selectbox(
                f"Campaña_{aid}",
                options=opciones_campana,
                key=k_camp_sel
            )
        with cc2:
            if campana_sel == "➕ Nueva campaña":
                des_campana = st.text_input(f"Nueva campaña_{aid}", key=k_nueva)
            else:
                des_campana = campana_sel

        # --- Selección de anuncio existente (filtrado por campaña si aplica) ---
        campana_filtro = des_campana if campana_sel not in ["", "➕ Nueva campaña"] else None
        if campana_filtro:
            ads_filtrados = anuncios_df[anuncios_df["DES_CAMPANA"] == campana_filtro]
        else:
            ads_filtrados = anuncios_df

        opciones_ad = [""] + ads_filtrados["DES_ANUNCIO_LIMPIO"].dropna().unique().tolist()
        st.selectbox(
            f"Anuncio existente_{aid}",
            options=opciones_ad,
            key=k_ad_sel,
            help="Selecciona un anuncio guardado para autocompletar"
        )

        # --- Anuncio limpio (editable) ---
        des_anuncio = st.text_input(f"Anuncio limpio_{aid}", key=k_anuncio)

        # --- Producto Base ---
        producto_sel = st.selectbox(
            f"Producto Base_{aid}",
            options=[""] + productos_lista,
            key=k_producto
        )

        agrupacion = marca = ""
        if producto_sel:
            info = productos_df[productos_df["PRODUCTO_BASE"] == producto_sel]
            if not info.empty:
                agrupacion = info.iloc[0]["AGRUPACION_PAUTA"]
                marca = info.iloc[0]["MARCA"]

        st.text_input(f"Agrupación Pauta_{aid}", value=agrupacion, disabled=True)
        st.text_input(f"Marca_{aid}", value=marca, disabled=True)

        reg = {
            "ID_AD": id_ad,
            "DES_CAMPANA": (des_campana or "").strip(),
            "DES_ANUNCIO_LIMPIO": (des_anuncio or "").strip(),
            "DES_PRODUCTO_BASE": producto_sel or "",
            "DES_AGRUPACION_PAUTA": agrupacion,
            "DES_MARCA": marca
        }
        # Guardar en estado global para el botón "Guardar todos"
        st.session_state.registros_datos[id_ad] = reg

        # --- Guardar individual (botón superior) ---
        if save_top:
            errores = validar_registro(reg)
            if errores:
                for e in errores:
                    st.error(e)
            else:
                st.session_state[k_confirmar] = True

        if st.session_state.get(k_confirmar):
            st.warning("¿Guardar este registro?")
            ca, cb = st.columns(2)
            with ca:
                if st.button("Sí, guardar", key=f"conf_si_{aid}"):
                    ok, msg = actualizar_registros(pd.DataFrame([reg]), usuario)
                    if ok:
                        st.success(msg)
                        st.session_state[k_confirmar] = False
                        st.cache_data.clear()
                        st.rerun(scope="app")
                    else:
                        st.error(msg)
            with cb:
                if st.button("Cancelar", key=f"conf_no_{aid}"):
                    st.session_state[k_confirmar] = False


# RENDERIZAR REGISTROS
for idx, row in ads_df.iterrows():
    st.divider()
    render_registro(idx, row, productos_df, campanas_historicas, anuncios_df, productos_lista)


# BOTÓN GUARDAR TODOS (inferior)
st.divider()

if st.button("💾 Guardar todos los registros completados"):
    errores = []
    registros_actualizar = []

    for id_ad, r in st.session_state.registros_datos.items():
        fila_con_datos = any([r["DES_CAMPANA"], r["DES_ANUNCIO_LIMPIO"], r["DES_PRODUCTO_BASE"]])
        if not fila_con_datos:
            continue
        errs = validar_registro(r)
        if errs:
            for e in errs:
                errores.append(f"ID_AD {id_ad}: {e}")
        else:
            registros_actualizar.append(r)

    if not registros_actualizar:
        st.warning("No hay registros para actualizar")
    elif errores:
        st.error("Existen errores en algunos registros:")
        for e in errores:
            st.warning(e)
    else:
        st.session_state.df_update = pd.DataFrame(registros_actualizar)
        st.session_state.confirmar_guardado = True


# CONFIRMACIÓN GUARDAR TODOS
if st.session_state.confirmar_guardado:
    df_preview = st.session_state.df_update
    st.warning(f"¿Seguro que deseas actualizar {len(df_preview)} registros?")
    st.dataframe(df_preview, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sí, actualizar"):
            with st.spinner("Actualizando registros..."):
                ok, mensaje = actualizar_registros(st.session_state.df_update, usuario)
            if ok:
                st.success(mensaje)
                st.cache_data.clear()
                st.session_state.confirmar_guardado = False
                if "df_update" in st.session_state:
                    del st.session_state.df_update
                st.rerun()
            else:
                st.error(mensaje)
    with col2:
        if st.button("Cancelar"):
            st.session_state.confirmar_guardado = False
            if "df_update" in st.session_state:
                del st.session_state.df_update
            st.info("Actualización cancelada")
