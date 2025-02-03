import pandas as pd
from datetime import datetime
from src.DATABASE import ConnectionClass

#from concurrent.futures import ThreadPoolExecutor
import gc
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
# Extraer la relación entre organismos y si son municipales desde la base de datos
organismo = connection.fetch_table("SELECT * FROM organismo360")
# Crear un diccionario con 'organismo' como clave y 'municipal' como valor
diccionarioOrganismoMuni = dict(zip(organismo["organismo"], organismo["municipal"]))
diccionarioOrganismoMuni["Subsecretaria de Evaluación Social"] = 0
diccionarioOrganismoMuni["CFT de la Región de Coquimbo"] = 0


organismo2 = connection.fetch_table("SELECT DISTINCT organismo_nombre FROM public.tabla_auxiliar_historial")

def get_fecha(fila):
    meses = {
        "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4,
        "Mayo": 5, "Junio": 6, "Julio": 7, "Agosto": 8,
        "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
    }
    try:
        mes_numero = meses[fila["mes"]]
        fecha_string = f'{int(fila["anyo"])}-{mes_numero}'
        fecha_dt = datetime.strptime(fecha_string, '%Y-%m')
    except Exception as e:
        fecha_dt = datetime.strptime("2099-01", '%Y-%m')
    
    return fecha_dt

def buscar_datos(anio):
    engine = connection.get_engine() 
    print(anio,datetime.now())
    query = "SELECT rut,organismo_nombre, anyo, mes, base,remuneracionbruta_mensual, remuliquida_mensual,homologado FROM personal2 WHERE anyo ="
    df = pd.read_sql(f"{query} {anio}", engine)
    df = df.drop_duplicates()
    engine.dispose()
    print("cerrar",anio,datetime.now())
    return df


def get_base():   
    engine = connection.get_engine() 
    query = "SELECT rut,organismo_nombre, anyo, mes, base,remuneracionbruta_mensual, remuliquida_mensual,homologado FROM personal2 WHERE anyo ="
    
    print(datetime.now())
    df2019 = pd.read_sql(f"{query} 2019", engine)
    #df2019 = df2019.drop_duplicates()
    print(datetime.now())
    df2020 = pd.read_sql(f"{query} 2020", engine)
    #df2020 = df2020.drop_duplicates()
    print(datetime.now())
    df2021 = pd.read_sql(f"{query} 2021", engine)
    #df2021 = df2021.drop_duplicates()
    print(datetime.now())
    df2022 = pd.read_sql(f"{query} 2022", engine)
    #df2022 = df2022.drop_duplicates()
    print(datetime.now())
    df2023 = pd.read_sql(f"{query} 2023", engine)
    #df2023 = df2023.drop_duplicates()
    print(datetime.now())
    df2024 = pd.read_sql(f"{query} 2024", engine)
    #df2024 = df2024.drop_duplicates()
    engine.dispose()
    print(datetime.now())
    df2 =  pd.concat([df2019,df2020,df2021,df2022,df2023,df2024])
    del df2019
    del df2020
    del df2021
    del df2022
    del df2023
    del df2024    
    gc.collect()
    df3 = df2.drop_duplicates()
    del df2
    fecha = df3[["anyo", "mes"]].drop_duplicates()
    fecha["Fecha"] = fecha.apply(get_fecha, axis=1)
    df3 = df3.merge(fecha)
    gc.collect()
    return df3

def marca_trabajo_anterior(fila):
    if fila["organismo_nombre_y"] == "Sin organismo anterior":
        return "Sin organismo anterior"
    if fila["fecha_min"] > fila["fecha_anexo_max"]:
        try:
            antes = diccionarioOrganismoMuni[fila["organismo_nombre_y"]]
        except:
            antes = 1
        try:
            actual = diccionarioOrganismoMuni[fila["organismo_nombre_x"]]
        except:
            actual = 1
        antes_string =  "Municpalidad" if antes == 1 else "Organismo"        
        actual_string =  "Municpalidad" if actual == 1 else "Organismo"
        return f"{antes_string} a {actual_string}"
    return "Intersecto"

def get_historial_personal(personal,df3):
    """
    Obtiene el historial personal de empleados comparando entre diferentes organismos.
    
    Args:
        personal (pd.DataFrame): DataFrame con información del personal actual
        df3 (pd.DataFrame): DataFrame con historial completo
        organismo (pd.DataFrame): DataFrame con información de organismos
    
    Returns:
        pd.DataFrame: Historial procesado del personal
    """
    # 2. Constantes
    DEFAULT_DATE = pd.Timestamp('1900-01-01')
    DEFAULT_ORG = "Sin organismo anterior"

    rut_unicos  = frozenset(personal["rut"].unique())
    #aux = aux[aux["rut"].apply(lambda x: x in lista_rut)]
    #aux = aux[aux["rut"].isin(lista_rut)]
    org_nombre = personal.iloc[0].organismo_nombre
    aux = df3.loc[
            (df3["organismo_nombre"] != org_nombre) &
            (df3["rut"].isin(rut_unicos))
        ]
    personal2 = personal.merge(aux, on="rut", how="left", sort=False)
    #personal2['Fecha'] = personal2['Fecha'].fillna(pd.Timestamp('1900-01-01'))
    #personal2["organismo_nombre_y"] = personal2["organismo_nombre_y"].fillna("Sin organismo anterior") 
    personal2.fillna({'Fecha': DEFAULT_DATE, "organismo_nombre_y": DEFAULT_ORG}, inplace=True)

    personal3 = personal2.groupby(['organismo_nombre_x', 'rut', 'fecha_max', 'fecha_min',
           'organismo_nombre_y']).agg(
                fecha_anexo_max = ('Fecha', 'max'),
                fecha_anexo_min = ('Fecha', 'min')
        ).reset_index()

    personal3 = personal3[personal3["fecha_min"] >= personal3["fecha_anexo_min"]]

    personal3 = personal3.merge(
        organismo[["organismo","municipal"]], 
        left_on="organismo_nombre_y", 
        right_on="organismo", 
        how="left")
    #del personal3["organismo"]
    personal3.drop(columns=["organismo"], inplace=True)
    personal3["municipal"] = personal3["municipal"].fillna(2)    
    personal3["trabajo_anterior"] = personal3.apply(marca_trabajo_anterior, axis=1)

    #personal3 = personal3.rename(columns={"organismo_nombre_x":"organismo_nombre_actual","organismo_nombre_y":"organismo_nombre_anterior"})
    return personal3.rename(columns={"organismo_nombre_x": "organismo_nombre_actual", "organismo_nombre_y": "organismo_nombre_anterior"}).rename(columns=str.lower) 

def get_detalle_historial(salida,df5):
    aux = salida.merge(df5,left_on=['organismo_nombre_actual', 'rut', 'fecha_max'], right_on=['organismo_nombre','rut','Fecha'],suffixes=('', '_agregado1'), how='left') \
        .merge(df5,left_on=['organismo_nombre_actual', 'rut', 'fecha_min'], right_on=['organismo_nombre','rut','Fecha'],suffixes=('', '_agregado2'), how='left')  \
        .merge(df5,left_on=['organismo_nombre_anterior', 'rut', 'fecha_anexo_max'], right_on=['organismo_nombre','rut','Fecha'],suffixes=('', '_agregado3'), how='left') \
        .merge(df5,left_on=['organismo_nombre_anterior', 'rut', 'fecha_anexo_min'], right_on=['organismo_nombre','rut','Fecha'],suffixes=('', '_agregado4'), how='left')
    eliminar_columnas = ['organismo_nombre_agregado2',"Fecha","Fecha_agregado2","Fecha_agregado3","Fecha_agregado4","organismo_nombre",
                     "organismo_nombre_agregado3","organismo_nombre_agregado4"]
    for i in eliminar_columnas:
        del aux[i]
    aux2 = aux.rename(columns={"base":"base_max",
                            "remuneracionbruta_mensual":"bruta_max",
                        "remuliquida_mensual":"liquida_max",
                        "homologado":"homologado_max",
                            "base_agregado2":"base_min",
                            "remuneracionbruta_mensual_agregado2":"bruta_min",
                            "remuliquida_mensual_agregado2":"liquida_min",
                            "homologado_agregado2":"homologado_min",
                            "base_agregado3":"base_anexo_max",
                            "remuneracionbruta_mensual_agregado3":"bruta_anexo_max",
                            "remuliquida_mensual_agregado3":"liquida_anexo_max",
                            "homologado_agregado3":"homologado_anexo_max",
                            "base_agregado4":"base_anexo_min",
                            "remuneracionbruta_mensual_agregado4":"bruta_anexo_min",
                            "remuliquida_mensual_agregado4":"liquida_anexo_min",
                            "homologado_agregado4":"homologado_anexo_min",
                        })
    return aux2

    




def recorrer_organismo(DB_RUT):
    ref2 = get_base()
    DB_RUT2 = DB_RUT[["NombreCompleto","rut"]].drop_duplicates(subset=["rut"])    
    ref2 = get_base()
    ref3 = ref2.sort_values(['remuneracionbruta_mensual', 'remuliquida_mensual']).drop_duplicates(subset=['rut', 'organismo_nombre', 'Fecha'])
    ref3 = ref3[['rut', 'organismo_nombre',  'base', 'remuneracionbruta_mensual', 'remuliquida_mensual', 'homologado','Fecha']]
    for organismo_for in organismo2["organismo_nombre"]:
        print(organismo_for)
        resumen = connection.fetch_table(f"SELECT * FROM tabla_auxiliar_historial WHERE organismo_nombre = '{organismo_for}'")
        resumen = get_historial_personal(resumen,ref2)
        resumen = get_detalle_historial(resumen,ref3)
        resumen = resumen.merge(DB_RUT2)  
        #connection.save_dataframe(df2.rename(columns=lambda x: x.lower()), "tabla_pruebas2")
        del resumen
        gc.collect()
    print("Cierre")

