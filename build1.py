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



base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']

pattern_maximo = r'^90.{6}$'
DB_RUT = pd.read_csv("ENCONTRADOS_10.csv", compression='xz', sep='\t')
ref = pd.read_excel(r"calificacion_key3 (7).xlsx", sheet_name="MATRIZ", skiprows=2)
ref2 = ref[["key","Secuencia"]]
ref3 = ref2.sort_values(by="Secuencia")
listaCalificacion = list(ref3["key"])
refFinal = ref[["key","Homologado"]]

hologado2 = pd.read_excel(r"homologa2 (1).xlsx", sheet_name="homologa2", skiprows=2)
hologado22 = hologado2[["Homologado","key","Secuencia"]].sort_values("Secuencia")
hologado2_x = hologado2[["key","Homologado","Homologado 2"]]

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

def string_to_url(s):
    return urllib.parse.quote(s)

def fixRemuneracion(valor):
    try:
        return int(valor[:-2])
    except:
        None

def eliminar_espacios_adicionales(cadena):
    if(type(cadena) == float):
        return "NO"
    return re.sub(r'\s+', ' ', cadena).strip()

def transformar_string(texto):
    # Convertir a mayúsculas
    texto = texto.upper()
    # Reemplazar tildes por letras sin tilde
    texto = texto.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
    # Reemplazar la letra "ñ" por "n"
    texto = texto.replace('Ñ', 'N')
    return texto

def limpiar_texto(texto):
    # Eliminar caracteres no deseados y convertir a mayúsculas
    texto_limpio = re.sub(r'[^A-Z ]', '', texto.upper())
    return texto_limpio

def rutificador(df):
    merge = df.merge(DB_RUT, left_on="NombreCompleto",right_on="NombreCompleto",how="left")
    return merge

def get_nombre_completo(df):
    df["Nombres2"] = df["Nombres"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Paterno2"] = df["Paterno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Materno2"] = df["Materno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["NombreCompleto"] = df[['Paterno2', 'Materno2','Nombres2',]].apply(
            lambda row: ' '.join(row.dropna().astype(str)).strip(), axis=1
        )
    return df

def getPagos(df):    
    """
    Process payments data from an Excel file and save the results to a new file.
    
    Parameters:
    URL (str): The file path of the Excel file to process.
    input_dir (str): The directory name in the input file path to replace (default is "organismoSalida2").
    output_dir (str): The directory name in the output file path to replace (default is "organismoSalida3").
    """
    result = df[['anyo', 'Mes','rut']].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
    df["año-mes-rut"] = result
    resultados = pd.DataFrame(result.value_counts()).reset_index().rename(columns={'count':'Cantidad de pagos en un mes',0: 'Cantidad de pagos en un mes',"index":"año-mes-rut"})
    df2 = df.merge(resultados)
    result = df2[["año-mes-rut",'base']].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
    df2["año-mes-rut-base"] = result
    resultados = pd.DataFrame(result.value_counts()).reset_index().rename(columns={'count':'Detalle de base en pagos en un mes',0: 'Detalle de base en pagos en un mes',"index":"año-mes-rut-base"})
    df2 = df2.merge(resultados)
    distinct_counts = df2[["año-mes-rut","año-mes-rut-base"]].groupby("año-mes-rut")["año-mes-rut-base"].nunique().reset_index()
    distinct_counts.columns = ["año-mes-rut", "Tipo de contrato distintos"]
    df2 = df2.merge(distinct_counts)    
    df2['remuliquida_mensual'] = df2['remuliquida_mensual'].map(fixRemuneracion)
    df2['remuneracionbruta_mensual'] = df2['remuneracionbruta_mensual'].map(fixRemuneracion)
    return df2

def calificacion_nivel_1(df):
    df["clean"] = df["tipo_calificacionp"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    calificacion2 = df[["clean"]].drop_duplicates()
    calificacion2 = calificacion2[calificacion2["clean"].notnull()]
    resto = calificacion2.copy()
    acumulador = {}
    for i in listaCalificacion:
        aux = resto[resto["clean"].apply(lambda x: all(word in x for word in i.split()))]
        resto = resto[resto["clean"].apply(lambda x: not all(word in x for word in i.split()))]
        aux["key"] = i
        acumulador[i] = aux.copy()
        if(len(resto) == 0):
            break
    calificacionClave = pd.concat([acumulador[x] for x in acumulador.keys()])
    merge = calificacionClave.merge(df, on="clean", how="right")
    #dfMerge = df.merge(merge, on="tipo_calificacionp",how="left")
    #dfMergeFinal = dfMerge.merge(refFinal, on="key" , how="left")
    dfMergeFinal = merge.merge(refFinal, on="key" , how="left")
    print(df.shape,dfMergeFinal.shape)
    return dfMergeFinal

def calificacion_nivel_2(df):
    lista_homologado_original = list(hologado22["Homologado"].unique())
    acumuladoDF_homologado = df[df["Homologado"].apply(lambda x: x in lista_homologado_original)]
    acumuladoDF_no_homologado = df[df["Homologado"].apply(lambda x: x not in lista_homologado_original)]
    acumulador = []
    acumulador_resto = []
    for i in hologado22["Homologado"].unique():
        aux = acumuladoDF_homologado[acumuladoDF_homologado["Homologado"] == i]
        auxaux = hologado22[hologado22["Homologado"] == i]
        lista_homologado2 = list(auxaux["key"].unique())
        resto = aux.copy()
        if(aux.shape[0] > 0):
            for j in lista_homologado2:
                aux = resto[resto["clean"].apply(lambda x: all(word in x for word in j.split()))]
                resto = resto[resto["clean"].apply(lambda x: not all(word in x for word in j.split()))]
                aux["key2"] = j
                acumulador.append(aux.copy())
                if(resto.shape[0] == 0):
                    break
        acumulador_resto.append(resto.copy())
    df_resto = pd.concat(acumulador_resto)
    acumuladoDF = pd.concat(acumulador)
    df_final = pd.concat([acumuladoDF,df_resto])
    hologado2_x.columns = ['key2', 'Homologado', 'Homologado 2']
    hologado2_x2 = hologado2_x.drop_duplicates()
    df_final_merge = df_final.merge(hologado2_x2, how="left")
    final = pd.concat([df_final_merge,acumuladoDF_no_homologado])
    final["Homologado"] = final["Homologado"].fillna("Sin Clasificar")
    final["Homologado 2"] = final["Homologado 2"].fillna("Sin Clasificar")
    return final

def process_comuna(comuna):
    base = string_to_url(comuna)
    url = f"https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/{base}.csv"
    
    try:
        # Leer el archivo CSV
        df = pd.read_csv(url, compression='xz', sep='\t')
        
        # Procesar el DataFrame a través de las funciones específicas
        df = get_nombre_completo(df)
        df = rutificador(df)
        df = getPagos(df)
        df = calificacion_nivel_1(df)
        df = calificacion_nivel_2(df)
        
        # Guardar el DataFrame procesado en un archivo Excel
        df.to_excel(f"test/{comuna}.xlsx", index=False)
    except Exception as e:
        print(f"Error al procesar {comuna}: {e}")

if __name__ == '__main__':
    #https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/Corporaci%C3%B3n%20Municipal%20de%20Providencia.csv
    for comuna in comunas:
        process_comuna(comuna)
        #print(url)
        #print(df2)
