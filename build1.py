import pandas as pd
import urllib.parse
import pandas as pd
import re
from thefuzz import fuzz
from thefuzz import process
import pickle
import numpy as np
from functools import lru_cache
from datetime import datetime
import os
import time
import tarfile
import requests
from collections import Counter
from itertools import permutations

def string_to_url(s):
    return urllib.parse.quote(s)

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

comunas = ['Corporación Municipal de Providencia',
 'Municipalidad de Antofagasta',
 'Municipalidad de Chillán',
 'Municipalidad de Concepción',
 'Municipalidad de Coquimbo',
 'Municipalidad de Curicó',
 'Municipalidad de El Bosque',
 'Municipalidad de Florida',
 'Municipalidad de Iquique',
 'Municipalidad de La Pintana',
 'Municipalidad de La Serena',
 'Municipalidad de Los Ángeles',
 'Municipalidad de Maipú',
 'Municipalidad de Peñalolén',
 'Municipalidad de Pudahuel',
 'Municipalidad de Puente Alto',
 'Municipalidad de Puerto Montt',
 'Municipalidad de Quilicura',
 'Municipalidad de Rancagua',
 'Municipalidad de San Bernardo',
 'Municipalidad de Santiago',
 'Municipalidad de Talca',
 'Municipalidad de Talcahuano',
 'Municipalidad de Valdivia',
 'Municipalidad de Valparaíso',
 'Municipalidad de Viña del Mar',
 'Municipalidad de Ñuñoa']

def get_nombre_completo(df):
    df["Nombres2"] = df["Nombres"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Paterno2"] = df["Paterno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Materno2"] = df["Materno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["NombreCompleto"] = df[['Paterno2', 'Materno2','Nombres2',]].apply(
            lambda row: ' '.join(row.dropna().astype(str)).strip(), axis=1
        )
    return df



if __name__ == '__main__':
    #https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/Corporaci%C3%B3n%20Municipal%20de%20Providencia.csv
    print("Hola")
    for i in comunas:
        base = string_to_url(i)
        url = f"https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/{base}.csv"
        df = pd.read_csv(url, compression='xz', sep='\t')
        df2 = get_nombre_completo(df)
        print(url)
        print(df2)
