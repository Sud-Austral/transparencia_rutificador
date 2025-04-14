import numpy as np
import pandas as pd
from datetime import datetime, timedelta

popt_filtrado = np.load("popt_filtrado2.npy")
fecha_base = datetime(1900, 1, 1)

def exponential_1(x, a, b, c, d):
    x = x/10000000
    #x = np.log(x)
    acumulador = 0
    n_idx = 0
    for param in [a, b, c, d]:
        acumulador += param * x ** n_idx
        n_idx += 1
    return acumulador



def dias_a_fecha(dias):
    """
    Convierte un número de días desde el 1-1-1900 en una fecha.
    
    Parámetros:
        dias (int): Número de días desde el 1 de enero de 1900.
    
    Retorna:
        str: Fecha en formato YYYY-MM-DD.
    """
    fecha_resultado = fecha_base + timedelta(days=dias)
    if fecha_resultado < datetime.now():
        return fecha_resultado.strftime("%Y-%m-%d")
    return None
    
def diferencia_anios(fecha1, fecha2):
    """
    Calcula la diferencia en años entre dos fechas.
    
    Parámetros:
        fecha1 (str): Primera fecha en formato YYYY-MM-DD.
        fecha2 (str): Segunda fecha en formato YYYY-MM-DD.
    
    Retorna:
        int: Número de años entre las dos fechas.
    """
    if not fecha2:
        return None
    fecha1 = datetime.strptime(fecha1, "%Y-%m-%d")
    fecha2 = datetime.strptime(fecha2, "%Y-%m-%d")
    
    if fecha1 < fecha2:
        return None
    return fecha1.year - fecha2.year

def get_age(fila):
    try:
        fecha1 = fila["fecha"]
        dias = fila["dias_desde_1900"]
        fecha2 = dias_a_fecha(dias)
        return diferencia_anios(fecha1,fecha2)
    except Exception as e:
        #print(f"Error en get_age: {e}")
        return None 
    
def getRUT(rut):
    if "-" not in rut:
        return None
    try:
        return int(rut.split("-")[0])
    except:
        return None
    
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
    
    return fecha_dt.strftime("%Y-%m-%d")

def rut_age(df: pd.DataFrame) -> pd.DataFrame:
    df["rut2"] = df["rut"].apply(getRUT)  
    df["fecha"] = df.apply(get_fecha, axis = 1) 
    df["dias_desde_1900"] = exponential_1(df["rut2"],*popt_filtrado)
    del df["rut2"]
    df["age_personal"] = df.apply(get_age, axis=1)
    df['age_personal'] = df['age_personal'].fillna(0)
    df['intervalo'] = pd.cut(df['age_personal'], bins=[18,30,50,65,85], right=False)
    df["age_label"] = df['intervalo'].apply(lambda x: f"{x.left} a {x.right}")
    del df["intervalo"]
    del df["fecha"]
    return df
