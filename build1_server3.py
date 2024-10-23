# Importación de las bibliotecas necesarias
import pandas as pd  # Para la manipulación y análisis de datos
#import urllib.parse  # Para el análisis y manipulación de URLs
#import re  # Para trabajar con expresiones regulares
#from thefuzz import fuzz, process  # Para la coincidencia difusa de cadenas (fuzzy matching)
#import pickle  # Para la serialización y deserialización de objetos
#import numpy as np  # Para operaciones numéricas
#from functools import lru_cache  # Para el almacenamiento en caché de resultados de funciones repetitivas
#from datetime import datetime  # Para trabajar con fechas y horas
import os  # Para interactuar con el sistema operativo (e.g., rutas de archivos)
#import time  # Para funciones relacionadas con el tiempo (e.g., sleep, seguimiento de tiempos)
#import tarfile  # Para trabajar con archivos tar
#import requests  # Para realizar peticiones HTTP
#from collections import Counter  # Para contar elementos hashables
#from itertools import permutations  # Para generar permutaciones de una secuencia
#import gc  # Para la recolección manual de basura (liberación de memoria)
import traceback  # Para el manejo y formateo de excepciones
#from functools import lru_cache
#import psycopg2
#from psycopg2 import sql
#from sqlalchemy import create_engine


def get_folder():
    ruta = "respaldo/"
    # Lista todas las carpetas que comienzan con '20'
    carpetas = [nombre for nombre in os.listdir(ruta) if os.path.isdir(os.path.join(ruta, nombre)) and nombre.startswith('20')]
    return carpetas

def get_files(ruta):    
    archivos = [nombre for nombre in os.listdir(ruta) if os.path.isfile(os.path.join(ruta, nombre))]
    return archivos

def save_archivo():
    folder = get_folder()
    for ruta in folder:
        files = get_files(f"respaldo/{ruta}")
        for file in files:
            url = f"respaldo/{ruta}/{file}"
            print(url)
            try:
                pd.read_csv(url,sep=";",encoding="latin").to_csv(url, index=False,compression='xz', sep='\t')
            except Exception as e:
                print(f"Error al procesar {url}: {e}")
                error_traceback = traceback.format_exc()
                print("Traceback detallado:")
                print(error_traceback)

            


if __name__ == '__main__':
    print("hola")




