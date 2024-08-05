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
import gc



base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']

pattern_maximo = r'^90.{6}$'
DB_RUT = pd.read_csv("ENCONTRADOS_10.csv", compression='xz', sep='\t')
DB_SERVEL = pd.concat([pd.read_csv("RUT1.csv", compression='xz', sep='\t'),pd.read_csv("RUT2.csv", compression='xz', sep='\t')])
ref = pd.read_excel(r"calificacion_key3 (7).xlsx", sheet_name="MATRIZ", skiprows=2)
ref2 = ref[["key","Secuencia"]]
ref3 = ref2.sort_values(by="Secuencia")
listaCalificacion = list(ref3["key"])
refFinal = ref[["key","Homologado"]]

hologado2 = pd.read_excel(r"homologa2 (1).xlsx", sheet_name="homologa2", skiprows=2)
hologado22 = hologado2[["Homologado","key","Secuencia"]].sort_values("Secuencia")
hologado2_x = hologado2[["key","Homologado","Homologado 2"]]
lista_homologado_original = list(hologado22["Homologado"].unique())
# Preparar el DataFrame de homologaciones para el merge
hologado2_x.columns = ['key2', 'Homologado', 'Homologado 2']
hologado2_x2 = hologado2_x.drop_duplicates()

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


exclusion_list = ["NO", "DEL", "DE", "SAN","A","LA","HONORARIOS","LAS","TRABAJO","CODIGO","ASISTENTE","EDUCACION","C","SIN","DOCENTE","APELLIDO", "Y",'M',"E"]

def getSimilitud(name1,name2):
    return fuzz.ratio(name1, name2)

def string_to_url(s):
    return urllib.parse.quote(s)

def fixRemuneracion(valor):
    try:
        return int(valor[:-2])
    except:
        None

def eliminar_espacios_adicionales(cadena):
    if isinstance(cadena, float):
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



def get_nombre_completo(df):
    df["Nombres2"] = df["Nombres"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Paterno2"] = df["Paterno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["Materno2"] = df["Materno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    df["NombreCompleto"] = df[['Paterno2', 'Materno2','Nombres2',]].apply(
            lambda row: ' '.join(row.dropna().astype(str)).strip(), axis=1
        )
    return df

def buscar_rut(df):
    merge2 = df[df["rut"].isnull()]
    problematico = merge2[["NombreCompleto"]].drop_duplicates()
    problematico_x = problematico.merge(DB_SERVEL,left_on="NombreCompleto",right_on="Nombre_merge",how="left")
    df_si = problematico_x[problematico_x["Nombre_merge"].notnull()]
    problematico_x2 = problematico_x[problematico_x["Nombre_merge"].isnull()]
    problematico_x3 = problematico_x2[["NombreCompleto"]].sort_values("NombreCompleto")
    diccionarioAcumulador = {}
    resto = problematico_x3.copy()
    while resto.shape[0] > 0:
        all_text = " ".join(resto["NombreCompleto"].dropna().to_list())
        words = all_text.split()
        filtered_words = [word for word in words if word not in exclusion_list]
        word_counts = Counter(filtered_words)
        most_common_words = word_counts.most_common(1)
        #unique_word
        i = [x[0] for x in most_common_words][0]
        result = resto["NombreCompleto"].str.contains(i)
        diccionarioAcumulador[i] = resto[result]
        resto = resto[~result]
    pattern = '|'.join(list(diccionarioAcumulador.keys()))
    datos6 = DB_SERVEL[DB_SERVEL['Nombre_merge'].str.contains(pattern, case=True, na=False)]  
    lista_palabras = list(diccionarioAcumulador.keys())

    def encontrar_nombre_similar(row,lista):
        print(row, end="\r")
        try:
            nombre = row
            #similaridades = [(getSimilitud(nombre, y), y) for y in lista]
            similaridades = [(fuzz.token_sort_ratio(nombre, y), y) for y in lista] +  [(getSimilitud(nombre, y), y) for y in lista] +[(fuzz.ratio(nombre, y), y) for y in lista] + [(fuzz.partial_ratio(nombre, y), y) for y in lista] + [(fuzz.UQRatio(nombre, y), y) for y in lista]+ [(fuzz.QRatio(nombre, y), y) for y in lista]
            similaridad_maxima, nombre_mas_similar = max(similaridades, key=lambda x: x[0])
            return pd.Series([similaridad_maxima, nombre_mas_similar])
        except:
            print(f"Error en {nombre}")
            return pd.Series([None, None])

    acumulador = []
    #for i in unique_word:
    for i in lista_palabras:
        print(f"\n{i}")
        ref = datos6[datos6['Nombre_merge'].str.contains(i, case=True, na=False)]["Nombre_merge"]
        aux = diccionarioAcumulador[i]
        aux[["probabilidad","nombre"]] = aux["NombreCompleto"].apply(lambda x: encontrar_nombre_similar(x,ref))
        acumulador.append(aux.copy())
    salida = pd.concat(acumulador)
    valor = 85
    #result = (salida.probabilidad>=valor)|(salida.p1>=valor)|(salida.p2>=valor)|(salida.p3>=valor)|(salida.p4>=valor)|(salida.p5>=valor)
    result = (salida.probabilidad >= valor)
    salida[result]
    return None



def rutificador(df):
    merge = df.merge(DB_RUT, on="NombreCompleto",how="left")
    return merge

def getPagos(df):    
    """
    Process payments data from an Excel file and save the results to a new file.    
    Parameters:
    URL (str): The file path of the Excel file to process.
    input_dir (str): The directory name in the input file path to replace (default is "organismoSalida2").
    output_dir (str): The directory name in the output file path to replace (default is "organismoSalida3").
    
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
    """
    df["año-mes-rut"] = df[['anyo', 'Mes', 'rut']].astype(str).agg('-'.join, axis=1)
    count_pagos_mes = df["año-mes-rut"].value_counts().rename('Cantidad de pagos en un mes')
    df = df.join(count_pagos_mes, on="año-mes-rut")

    df["año-mes-rut-base"] = df[['año-mes-rut', 'base']].astype(str).agg('-'.join, axis=1)
    detalle_base_pagos_mes = df["año-mes-rut-base"].value_counts().rename('Detalle de base en pagos en un mes')
    df = df.join(detalle_base_pagos_mes, on="año-mes-rut-base")

    tipo_contrato_distintos = df.groupby("año-mes-rut")["año-mes-rut-base"].nunique().rename("Tipo de contrato distintos")
    df = df.join(tipo_contrato_distintos, on="año-mes-rut")

    df['remuliquida_mensual'] = df['remuliquida_mensual'].map(fixRemuneracion)
    df['remuneracionbruta_mensual'] = df['remuneracionbruta_mensual'].map(fixRemuneracion)
    
    return df

def calificacion_nivel_1(df):
    df["clean"] = df["tipo_calificacionp"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    calificacion2 = df[["clean"]].drop_duplicates().dropna()
    #calificacion2 = calificacion2[calificacion2["clean"].notnull()]
    resto = calificacion2.copy()
    acumulador = {}
    for i in listaCalificacion:
        #aux = resto[resto["clean"].apply(lambda x: all(word in x for word in i.split()))]
        #resto = resto[resto["clean"].apply(lambda x: not all(word in x for word in i.split()))]
        mask = resto["clean"].apply(lambda x: all(word in x for word in i.split()))
        aux = resto[mask]
        resto = resto[~mask]

        #aux = aux.to_frame()
        aux["key"] = i
        acumulador[i] = aux.copy()
        if resto.empty:
            break
    #calificacionClave = pd.concat([acumulador[x] for x in acumulador.keys()])
    calificacionClave = pd.concat(acumulador.values())
    merge = calificacionClave.merge(df, on="clean", how="right")
    #dfMerge = df.merge(merge, on="tipo_calificacionp",how="left")
    #dfMergeFinal = dfMerge.merge(refFinal, on="key" , how="left")
    dfMergeFinal = merge.merge(refFinal, on="key" , how="left")
    
    return dfMergeFinal

def calificacion_nivel_2(df):    
    #acumuladoDF_homologado = df[df["Homologado"].apply(lambda x: x in lista_homologado_original)]
    #acumuladoDF_no_homologado = df[df["Homologado"].apply(lambda x: x not in lista_homologado_original)]
     # Dividir el DataFrame en homologado y no homologado de una vez
    is_homologado = df["Homologado"].isin(lista_homologado_original)
    acumuladoDF_homologado = df[is_homologado]
    acumuladoDF_no_homologado = df[~is_homologado]   

    acumulador = []
    acumulador_resto = []
    #for i in hologado22["Homologado"].unique():

    for i in lista_homologado_original:
        aux = acumuladoDF_homologado[acumuladoDF_homologado["Homologado"] == i]
        auxaux = hologado22[hologado22["Homologado"] == i]
        lista_homologado2 = list(auxaux["key"].unique())
        #resto = aux.copy()

        if(aux.shape[0] > 0):
            for j in lista_homologado2:
                #aux = resto[resto["clean"].apply(lambda x: all(word in x for word in j.split()))]
                #resto = resto[resto["clean"].apply(lambda x: not all(word in x for word in j.split()))]
                #mask = resto["clean"].apply(lambda x: all(word in x for word in j.split()))
                mask = aux["clean"].apply(lambda x: all(word in x for word in j.split()))
                aux_subset = aux[mask]
                aux_subset["key2"] = j
                acumulador.append(aux_subset)
                aux = aux[~mask]
                #aux = resto[mask]
                #resto = resto[~mask]
                #aux["key2"] = j
                #aux_subset["key2"] = j
                #acumulador.append(aux)
                if aux.empty:
                    break

        acumulador_resto.append(resto.copy())

    df_resto = pd.concat(acumulador_resto, ignore_index=True)
    acumuladoDF = pd.concat(acumulador, ignore_index=True)
    df_final = pd.concat([acumuladoDF,df_resto], ignore_index=True)    

    df_final_merge = df_final.merge(hologado2_x2, how="left")
    final = pd.concat([df_final_merge,acumuladoDF_no_homologado], ignore_index=True)
    
    final["Homologado"] = final["Homologado"].fillna("Sin Clasificar")
    final["Homologado 2"] = final["Homologado 2"].fillna("Sin Clasificar")
    return final

def process_comuna(comuna):
    base = string_to_url(comuna)
    url = f"https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/{base}.csv"
    print(url)
    try:
        # Leer el archivo CSV
        df = pd.read_csv(url, compression='xz', sep='\t')
        print(df.shape)
        # Procesar el DataFrame a través de las funciones específicas
        df = get_nombre_completo(df)
        df = rutificador(df)[['organismo_nombre', 'anyo', 'Mes', 
       'tipo_calificacionp', 'Tipo cargo', 'remuneracionbruta_mensual',
       'remuliquida_mensual', 'base', 'tipo_pago', 'num_cuotas','NombreCompleto', 'rut', 'Nombre_merge']]
        
        df = getPagos(df)
        
        df = calificacion_nivel_1(df)[['organismo_nombre', 'anyo', 'Mes', 'tipo_calificacionp',
       'Tipo cargo', 'remuneracionbruta_mensual', 'remuliquida_mensual',
       'base', 'tipo_pago', 'num_cuotas', 'NombreCompleto', 'rut',
       'Nombre_merge', 'Cantidad de pagos en un mes',
        'Detalle de base en pagos en un mes',
       'Tipo de contrato distintos', 'Homologado','key']]
        
        df = calificacion_nivel_2(df)[['organismo_nombre', 'anyo', 'Mes', 'tipo_calificacionp',
       'Tipo cargo', 'remuneracionbruta_mensual', 'remuliquida_mensual',
       'base', 'tipo_pago', 'num_cuotas', 'NombreCompleto', 'rut',
       'Nombre_merge', 'Cantidad de pagos en un mes',
        'Detalle de base en pagos en un mes',
       'Tipo de contrato distintos', 'Homologado',  'Homologado 2','key']]
        df = df.rename(columns={'NombreCompleto': 'NombreCompleto_x', 'Nombre_merge': 'NombreEncontrado'})
        #Guardar el DataFrame procesado en un archivo Excel
        df.to_excel(f"test/{comuna}.xlsx", index=False)
        
        print(df.shape)
    except Exception as e:
        print(f"Error al procesar {comuna}: {e}")

if __name__ == '__main__':
    #https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/Corporaci%C3%B3n%20Municipal%20de%20Providencia.csv
    for comuna in comunas[20:21]:
        print(comuna)
        result = process_comuna(comuna)
        # Eliminar la variable que contiene los datos grandes para liberar memoria
        del result
        gc.collect()
        #print(url)
        #print(df2)
