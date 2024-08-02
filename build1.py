import pandas as pd
from datetime import datetime
import re
import os
import time
import tarfile
import requests
from collections import Counter
from itertools import permutations
from thefuzz import fuzz
from thefuzz import process
import pickle
import warnings


base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']


TA_PersonalPlanta                       = f"{base}TA_PersonalPlanta.csv"
TA_PersonalContrata                     = f"{base}TA_PersonalContrata.csv"
TA_PersonalCodigotrabajo                = f"{base}TA_PersonalCodigotrabajo.csv"
TA_PersonalContratohonorarios           = f"{base}TA_PersonalContratohonorarios.csv"


PersonalPlantaDICT                = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContrataDICT              = deseadas+["remuliquida_mensual",'Tipo cargo','remuneracionbruta_mensual'] 
PersonalCodigotrabajoDICT         = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContratohonorariosDICT    = deseadas+['remuliquida_mensual','tipo_pago','num_cuotas','remuneracionbruta']

def eliminar_espacios_adicionales(cadena):
    if(type(cadena) == float):
        return "NO"
    return re.sub(r'\s+', ' ', cadena).strip()

def getFullName(fila):
    name = fila["Nombres2"]
    apellidoP = fila['Paterno']
    apellidoM = fila['Materno']
    return f"{name} {apellidoP} {apellidoM}"

def transformar_string(texto):
    # Convertir a mayúsculas
    texto = texto.upper()
    # Reemplazar tildes por letras sin tilde
    texto = texto.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
    # Reemplazar la letra "ñ" por "n"
    texto = texto.replace('Ñ', 'N')
    return texto

def getFloat(numero):
    try:
        return float(numero[:-2])
    except:
        return 0

def descarga():
    PersonalPlanta             = getDF(TA_PersonalPlanta              ,PersonalPlantaDICT)
    PersonalContrata           = getDF(TA_PersonalContrata            ,PersonalContrataDICT)
    PersonalCodigotrabajo      = getDF(TA_PersonalCodigotrabajo       ,PersonalCodigotrabajoDICT)
    PersonalContratohonorarios = getDF(TA_PersonalContratohonorarios  ,PersonalContratohonorariosDICT)
    personal = pd.concat([PersonalContrata,PersonalPlanta,PersonalCodigotrabajo,PersonalContratohonorarios])
    return personal

def getFecha(fecha):
    try:
        return datetime.strptime(fecha, date_format)
    except:
        return None

def consolidar():
    personal = descarga()
    personal["remuneracionbruta_mensual"] = personal["remuneracionbruta_mensual"].apply(getFloat)
    personal["remuliquida_mensual"] = personal["remuliquida_mensual"].apply(getFloat)
    personal["Nombres2"] = personal["Nombres"].apply(eliminar_espacios_adicionales)
    personal["NOMBRECOMPLETO"] = personal.apply(getFullName,axis=1)
    personal["Nombres2"] = personal["Nombres2"].apply(transformar_string) 
    personal["NOMBRECOMPLETO2"] = personal["NOMBRECOMPLETO"].apply(transformar_string) 
    for i in personal["organismo_nombre"].unique():
        organismo = personal[personal["organismo_nombre"] == i]
        organismo.to_excel(f"organismo/{i}.xlsx", index=False)

def addColumns(personal):
    personal["remuneracionbruta_mensual"] = personal["remuneracionbruta_mensual"].apply(getFloat)
    personal["remuliquida_mensual"] = personal["remuliquida_mensual"].apply(getFloat)
    personal["Nombres2"] = personal["Nombres"].apply(eliminar_espacios_adicionales)
    personal["NOMBRECOMPLETO"] = personal.apply(getFullName,axis=1)
    personal["Nombres2"] = personal["Nombres2"].apply(transformar_string) 
    personal["NOMBRECOMPLETO2"] = personal["NOMBRECOMPLETO"].apply(transformar_string) 
    return personal

url = 'https://www.cplt.cl/transparencia_activa/datoabierto/archivos/TA_PersonalPlanta.csv'
output_file = 'TA_PersonalPlanta.csv'
chunk_size = 1048576  # 1 MB

def download_file(url, output_file, chunk_size):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(output_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
                print(f"Downloaded {file.tell()} bytes")
    
    print(f"Download complete. File saved as {output_file}")

if __name__ == '__main__':
    print("Hola")
