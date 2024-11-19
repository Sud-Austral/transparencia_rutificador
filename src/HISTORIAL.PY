import pandas as pd
from datetime import datetime
from src.DATABASE import ConnectionClass

#from ConnectionClass import ConnectionClass  # Asegúrate de que esta clase esté correctamente configurada
import traceback  # Para el manejo y formateo de excepciones

# Configuración de conexión
conn_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'UnaCasaEnUnArbol2024',
    'host': '186.67.61.251'
}

# Crear instancia de conexión
connection = ConnectionClass(conn_params)

# Cargar datos de organismo
organismo = connection.fetch_table("SELECT * FROM organismo360")

diccionarioOrganismoMuni = {}
for i,j in organismo[["organismo","municipal"]].iterrows():
    diccionarioOrganismoMuni[j["organismo"]] = j["municipal"]

# Función para convertir el mes en número y retornar una fecha
def get_fecha2(fila):
    meses = {
        "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4,
        "Mayo": 5, "Junio": 6, "Julio": 7, "Agosto": 8,
        "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
    }
    try:
        mes_numero = meses[fila["Mes"]]
        fecha_string = f'{int(fila["anyo"])}-{mes_numero}'
        fecha_dt = datetime.strptime(fecha_string, '%Y-%m')
    except KeyError:
        # Maneja meses no válidos
        print(f"Mes inválido encontrado: {fila['Mes']}")
        fecha_dt = datetime.strptime("2099-01", '%Y-%m')
    except Exception as e:
        # Maneja otros errores
        print(f"Error procesando la fila {fila}: {e}")
        fecha_dt = datetime.strptime("2099-01", '%Y-%m')
    
    return fecha_dt

# Función principal para identificar pagos múltiples
def pagos_multiples(df):
    """
    Procesa el DataFrame para identificar pagos múltiples y generar un reporte.

    Args:
        df (pd.DataFrame): DataFrame con datos de pagos.

    Returns:
        pd.DataFrame: DataFrame con pagos múltiples identificados.
    """
    df = df.query("anyo > 2000 and anyo < 2030").copy()   

    organismo_nombre = df.iloc[0].organismo_nombre
    # Generar DataFrame único con fechas
    df2 = df[['organismo_nombre', 'anyo', 'Mes', 'rut']].drop_duplicates()
    df2["Fecha"] = df2.apply(get_fecha2, axis=1)    
    
    fecha_actual = pd.Timestamp.today()
    df2 = df2[df2['Fecha'] < fecha_actual]
    # Filtrar por fecha de referencia
    fecha_referencia = datetime.strptime("2021-6", '%Y-%m')
    if diccionarioOrganismoMuni.get(organismo_nombre) == 0:
        fecha_referencia = datetime.strptime("2022-3", '%Y-%m')
    df3 = df2.groupby(["organismo_nombre", "rut"]).agg(
        fecha_max=('Fecha', 'max'),
        fecha_min=('Fecha', 'min')
    ).reset_index()
    df4 = df3[df3["fecha_min"] >= fecha_referencia]
    # Identificar pagos múltiples
    multiples = df4[["organismo_nombre", "rut"]]
    multiples2 = multiples.merge(df, on=["organismo_nombre", "rut"])
    multiples3 = multiples2.groupby(
        ["organismo_nombre", "rut", "anyo", "Mes", "base"]
    ).size().reset_index(name="counts")
    # Crear tabla pivote
    multiples4 = multiples3.pivot_table(
        index=['organismo_nombre', 'rut', 'anyo', 'Mes'], 
        columns='base', 
        values='counts', 
        fill_value=0
    ).reset_index()
    # Asegurar que las columnas esperadas existan
    for col in ['Contratohonorarios', 'Contrata', 'Planta', 'Codigotrabajo']:
        if col not in multiples4.columns:
            multiples4[col] = 0
    # Calcular total de pagos
    multiples4["Total"] = multiples4[['Contratohonorarios', 'Contrata', 'Planta', 'Codigotrabajo']].sum(axis=1)
    multiples5 = multiples4[multiples4["Total"] > 1]
    # Agregar información de organismo
    multiples6 = multiples5.merge(
        organismo[["organismo", "municipal"]], 
        left_on="organismo_nombre", 
        right_on="organismo"
    )
    del multiples6["organismo_nombre"]
    # Agregar datos adicionales de pagos
    pagos = df[[
        "rut", "anyo", "Mes", "NombreEncontrado", "Homologado",
        "remuneracionbruta_mensual", "remuliquida_mensual"
    ]]
    multiples7 = multiples6.merge(pagos, on=["rut", "anyo", "Mes"])
    # Agregar numeración
    multiples7['numeracion'] = multiples7.groupby(['rut', 'anyo', 'Mes']).cumcount() + 1
    # Unir con base de datos
    base = df[['rut', 'anyo', 'Mes', 'remuneracionbruta_mensual', 'remuliquida_mensual', "base"]].drop_duplicates()
    multiples8 = multiples7.merge(base, on=['rut', 'anyo', 'Mes', 'remuneracionbruta_mensual', 'remuliquida_mensual'])
    # Renombrar columnas a minúsculas
    multiples8 = multiples8.rename(columns={"NombreEncontrado":"nombre_completo_x"})
    multiples8 = multiples8.rename(columns=str.lower)
    connection.save_dataframe(multiples8, "pagos_multiple2")
    return multiples8
