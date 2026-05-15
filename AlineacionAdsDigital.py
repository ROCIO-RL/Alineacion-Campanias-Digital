import streamlit as st
import pandas as pd
import snowflake.connector
import re
from cryptography.hazmat.primitives import serialization

# CONFIG STREAMLIT
st.set_page_config(
    page_title="Alineación Digital Ads",
    layout="wide"
)

st.title("Alineación Digital - Completar Ads")

# CONFIG SNOWFLAKE


SNOWFLAKE_CONFIG = {
    "user": st.secrets["snowflake"]["user"],
    "account": st.secrets["snowflake"]["account"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
    "role": st.secrets["snowflake"]["role"]
}
# RSA PRIVATE KEY

def get_private_key():

    private_key_data = st.secrets["snowflake"]["private_key"]

    p_key = serialization.load_pem_private_key(
        private_key_data.encode(),
        password=None
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return pkb

# CONEXIÓN

def get_connection(role=None):

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"],
        private_key=get_private_key()
    )

    return conn

# FUNCIONES

@st.cache_data(ttl=3600)
def obtener_catalogo_productos():

    conn = get_connection()

    query = """
    SELECT DISTINCT
         epc.PRONOMBRE AS PRODUCTO_BASE,
         epc.AGPPAUTANOMBRE AS AGRUPACION_PAUTA,
         epc.MRCNOMBRE AS MARCA
    FROM PRD_CNS_MX.DM.DIM_PRODUCTO epc
    ORDER BY PRODUCTO_BASE
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return df

@st.cache_data(ttl=3600)
def obtener_campanas_historicas():

    conn = get_connection()

    query = """
    SELECT DISTINCT DES_CAMPANA
    FROM PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
    WHERE DES_CAMPANA IS NOT NULL
    AND TRIM(DES_CAMPANA) <> ''
    ORDER BY DES_CAMPANA
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return sorted(df["DES_CAMPANA"].dropna().unique().tolist())


def obtener_ads_pendientes():

    conn = get_connection()

    query = """
    SELECT
         ID_AD
        ,ID_AD_GROUP
        ,COD_PLATAFORMA
        ,DES_AD_NAME_RAW
        ,DES_CAMPANA
        ,DES_ANUNCIO_LIMPIO
        ,DES_PRODUCTO_BASE
        ,DES_AGRUPACION_PAUTA
        ,DES_MARCA
    FROM PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
    WHERE
        DES_CAMPANA IS NULL
        OR TRIM(DES_CAMPANA) = ''
        OR DES_ANUNCIO_LIMPIO IS NULL
        OR TRIM(DES_ANUNCIO_LIMPIO) = ''
        OR DES_PRODUCTO_BASE IS NULL
        OR TRIM(DES_PRODUCTO_BASE) = ''
    ORDER BY ID_AD
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return df


def validar_texto(texto):

    patron = r'^[A-Za-z0-9 _-]+$'

    return bool(re.match(patron, texto))


def actualizar_registros(df_actualizar, usuario):

    conn = get_connection()

    cursor = conn.cursor()

    try:

        for _, row in df_actualizar.iterrows():

            query_update = """
            UPDATE PRD_STG.GNM_MEX.MKT_DIM_ADS_DIGITAL_FLE
            SET
                 DES_CAMPANA = %s
                ,DES_ANUNCIO_LIMPIO = %s
                ,DES_PRODUCTO_BASE = %s
                ,DES_AGRUPACION_PAUTA = %s
                ,DES_MARCA = %s
                ,UPDATED_AT = CURRENT_TIMESTAMP()
                ,UPDATED_USR = %s
            WHERE ID_AD = %s
            """

            valores = (
                row["DES_CAMPANA"],
                row["DES_ANUNCIO_LIMPIO"],
                row["DES_PRODUCTO_BASE"],
                row["DES_AGRUPACION_PAUTA"],
                row["DES_MARCA"],
                usuario,
                row["ID_AD"]
            )

            cursor.execute(query_update, valores)

        conn.commit()

        return True, "Registros actualizados correctamente"

    except Exception as e:

        conn.rollback()

        return False, str(e)

    finally:

        cursor.close()
        conn.close()

# DATA

productos_df = obtener_catalogo_productos()
campanas_historicas = obtener_campanas_historicas()
ads_df = obtener_ads_pendientes()

# SIN REGISTROS

if ads_df.empty:
    st.success("No existen registros pendientes")
    st.stop()

usuario = st.secrets["snowflake"]["user"]

# UI

resultado = []

productos_lista = sorted(
    productos_df["PRODUCTO_BASE"].dropna().unique().tolist()
)

for idx, row in ads_df.iterrows():

    st.divider()

    col1, col2 = st.columns([1, 2])

    with col1:

        st.markdown(f"### Registro {idx + 1}")

        st.write(f"**Id Anuncio:** {row['ID_AD']}")
        st.write(f"**Anuncio:** {row['DES_AD_NAME_RAW']}")
        st.write(f"**Plataforma:** {row['COD_PLATAFORMA']}")

    with col2:

        #des_campana = st.text_input(
        #    f"Campaña_{idx+1}",
        #    value=row["DES_CAMPANA"] if pd.notnull(row["DES_CAMPANA"]) else ""
        #)

        col_camp1, col_camp2 = st.columns([2, 2])

        with col_camp1:

            opciones_campana = ["", "➕ Nueva campaña"] + campanas_historicas

            campana_seleccionada = st.selectbox(
                f"Campaña_{idx+1}",
                options=opciones_campana,
                key=f"campana_select_{idx+1}"
            )

        with col_camp2:

            if campana_seleccionada == "➕ Nueva campaña":

                des_campana = st.text_input(
                    f"Nueva campaña_{idx+1}",
                    key=f"nueva_campana_{idx+1}"
                )

            else:

                des_campana = campana_seleccionada

        des_anuncio = st.text_input(
            f"Anuncio_{idx+1}",
            value=row["DES_ANUNCIO_LIMPIO"] if pd.notnull(row["DES_ANUNCIO_LIMPIO"]) else ""
        )

        producto_sel = st.selectbox(
            f"Producto Base_{idx+1}",
            options=[""] + productos_lista,
            index=0,
            key=f"producto_{idx+1}"
        )

        agrupacion = ""
        marca = ""

        if producto_sel != "":

            producto_info = productos_df[
                productos_df["PRODUCTO_BASE"] == producto_sel
            ]

            if not producto_info.empty:

                agrupacion = producto_info.iloc[0]["AGRUPACION_PAUTA"]
                marca = producto_info.iloc[0]["MARCA"]

        st.text_input(
            f"Agrupación Pauta_{idx+1}",
            value=agrupacion,
            disabled=True
        )

        st.text_input(
            f"Marca_{idx+1}",
            value=marca,
            disabled=True
        )

        resultado.append({
            "ID_AD": row["ID_AD"],
            "DES_CAMPANA": des_campana.strip(),
            "DES_ANUNCIO_LIMPIO": des_anuncio.strip(),
            "DES_PRODUCTO_BASE": producto_sel,
            "DES_AGRUPACION_PAUTA": agrupacion,
            "DES_MARCA": marca
        })


# SESSION STATE

if "confirmar_guardado" not in st.session_state:
    st.session_state.confirmar_guardado = False


# BOTÓN GUARDAR

if st.button("Guardar"):

    errores = []
    registros_actualizar = []

    for i, r in enumerate(resultado):

        # DETECTAR SI EL USUARIO LLENÓ ALGO

        fila_con_datos = any([
            r["DES_CAMPANA"] != "",
            r["DES_ANUNCIO_LIMPIO"] != "",
            r["DES_PRODUCTO_BASE"] != ""
        ])

        # Si no llenó nada, ignorar fila
        if not fila_con_datos:
            continue

        # VALIDACIONES SOLO PARA FILAS LLENAS

        if r["DES_CAMPANA"] == "":
            errores.append(f"Registro {i+1}: Campaña vacía")

        if r["DES_ANUNCIO_LIMPIO"] == "":
            errores.append(f"Registro {i+1}: Anuncio vacío")

        if r["DES_PRODUCTO_BASE"] =="":
            errores.append(f"Registro {i+1}: Producto Base vacío")

        if r["DES_CAMPANA"] != "" and not validar_texto(r["DES_CAMPANA"]):
            errores.append(
                f"Registro {i+1}: Campaña contiene caracteres inválidos"
            )

        if r["DES_ANUNCIO_LIMPIO"] != "" and not validar_texto(r["DES_ANUNCIO_LIMPIO"]):
            errores.append(
                f"Registro {i+1}: Anuncio contiene caracteres inválidos"
            )

        # SI PASA VALIDACIONES SE AGREGA

        registros_actualizar.append(r)

    # VALIDAR SI NO HAY REGISTROS

    if len(registros_actualizar) == 0:

        st.warning("No hay registros para actualizar")


    # MOSTRAR ERRORES

    elif errores:

        st.error("Existen errores")

        for e in errores:
            st.warning(e)

    # CONFIRMAR

    else:

        st.session_state.df_update = pd.DataFrame(registros_actualizar)
        st.session_state.confirmar_guardado = True


# CONFIRMACIÓN

if st.session_state.confirmar_guardado:

    df_preview = st.session_state.df_update

    st.warning(
        f"¿Seguro que deseas actualizar {len(df_preview)} registros?"
    )

    st.dataframe(
        df_preview,
        use_container_width=True,
        hide_index=True
    )

    col1, col2 = st.columns(2)


    # CONFIRMAR UPDATE

    with col1:

        if st.button("Sí, actualizar"):

            with st.spinner("Actualizando registros..."):

                ok, mensaje = actualizar_registros(
                    st.session_state.df_update,
                    usuario
                )

            if ok:

                st.success(mensaje)

                # LIMPIAR CACHE

                st.cache_data.clear()

                # LIMPIAR SESSION

                st.session_state.confirmar_guardado = False

                if "df_update" in st.session_state:
                    del st.session_state.df_update

                # RECARGAR

                st.rerun()

            else:

                st.error(mensaje)


    # CANCELAR

    with col2:

        if st.button("Cancelar"):

            st.session_state.confirmar_guardado = False

            if "df_update" in st.session_state:
                del st.session_state.df_update

            st.info("Actualización cancelada")
