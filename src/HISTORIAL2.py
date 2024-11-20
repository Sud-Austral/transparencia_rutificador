import pandas as pd
from datetime import datetime
from src.DATABASE import ConnectionClass
from concurrent.futures import ThreadPoolExecutor

#from ConnectionClass import ConnectionClass  # Asegúrate de que esta clase esté correctamente configurada
import traceback  # Para el manejo y formateo de excepciones
import gc
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


organismo2 = connection.fetch_table("SELECT DISTINCT organismo FROM public.pagos_multiple2")

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
    print(df3)
    personal = personal.copy()
    aux = df3[df3["organismo_nombre"] != personal.iloc[0].organismo]
    personal2 = personal.merge(aux, on="rut", how="left")
    personal2['Fecha'] = personal2['Fecha'].fillna(pd.Timestamp('1900-01-01'))
    personal2["organismo_nombre_y"] = personal2["organismo_nombre_y"].fillna("Sin organismo anterior") 
    personal3 = personal2.groupby(['organismo_nombre_x', 'rut', 'fecha_max', 'fecha_min',
           'organismo_nombre_y']).agg(
                fecha_anexo_max = ('Fecha', 'max'),
                fecha_anexo_min = ('Fecha', 'min')
        ).reset_index()
    personal4 = personal3.merge(organismo[["organismo","municipal"]], left_on="organismo_nombre_y", right_on="organismo", how="left")
    del personal4["organismo"]
    personal4["municipal"] = personal4["municipal"].fillna(2)
    personal5 = personal4[personal4["fecha_min"] >= personal4["fecha_anexo_min"]]
    personal5["trabajo_anterior"] = personal5.apply(marca_trabajo_anterior, axis=1)
    personal6 = personal5.rename(columns={"organismo_nombre_x":"organismo_nombre_actual","organismo_nombre_y":"organismo_nombre_anterior"})
    return personal6.rename(columns=lambda x: x .lower()) 


def recorrer_organismo():
    #df2 = get_base()
    #print(df2.columns)
    #print(df2.head())
    nada = connection.fetch_table("SELECT rut,organismo_nombre, anyo, mes, base,remuneracionbruta_mensual, remuliquida_mensual,homologado FROM personal2 LIMIT 1")
    #fecha = df3[["anyo", "mes"]].drop_duplicates()
    nada["Fecha"] = nada.apply(get_fecha, axis=1)
    for organismo in organismo2["organismo"]:
        print(organismo)
        resumen = connection.fetch_table(f"SELECT * FROM pagos_multiple2 WHERE organismo = '{organismo}'")
        df2 = get_historial_personal(resumen,nada)
        break
    print("Cierre")

