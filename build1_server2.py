# Importación de las bibliotecas necesarias
import pandas as pd  # Para la manipulación y análisis de datos
import urllib.parse  # Para el análisis y manipulación de URLs
import re  # Para trabajar con expresiones regulares
from thefuzz import fuzz, process  # Para la coincidencia difusa de cadenas (fuzzy matching)
import pickle  # Para la serialización y deserialización de objetos
import numpy as np  # Para operaciones numéricas
from functools import lru_cache  # Para el almacenamiento en caché de resultados de funciones repetitivas
from datetime import datetime  # Para trabajar con fechas y horas
import os  # Para interactuar con el sistema operativo (e.g., rutas de archivos)
import time  # Para funciones relacionadas con el tiempo (e.g., sleep, seguimiento de tiempos)
import tarfile  # Para trabajar con archivos tar
import requests  # Para realizar peticiones HTTP
from collections import Counter  # Para contar elementos hashables
from itertools import permutations  # Para generar permutaciones de una secuencia
import gc  # Para la recolección manual de basura (liberación de memoria)
import traceback  # Para el manejo y formateo de excepciones
#from functools import lru_cache
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from src.PERSONAL import get_historial_persona
import src.HISTORIAL
import src.HISTORIAL2 as H2

def truncate_table_personal(db_config):
    """
    Esta función ejecuta el comando TRUNCATE en la tabla 'personal'.
    
    Parámetros:
        db_config (dict): Un diccionario con la configuración de la base de datos.
                          Debe contener las claves 'dbname', 'user', 'password', 'host', y 'port'.
    """
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Comando SQL para truncar la tabla
        truncate_query = sql.SQL("TRUNCATE TABLE personal2")

        # Ejecutar la consulta
        cursor.execute(truncate_query)

        # Confirmar la transacción
        conn.commit()

        print("La tabla 'personal' ha sido truncada exitosamente.")

    except Exception as e:
        print(f"Error al truncar la tabla: {e}")
    
    finally:
        # Cerrar la conexión y el cursor
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def truncate_table_personal_general(db_config,tabla):
    """
    Esta función ejecuta el comando TRUNCATE en la tabla 'personal'.
    
    Parámetros:
        db_config (dict): Un diccionario con la configuración de la base de datos.
                          Debe contener las claves 'dbname', 'user', 'password', 'host', y 'port'.
    """
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Comando SQL para truncar la tabla
        truncate_query = sql.SQL(f"TRUNCATE TABLE {tabla}")

        # Ejecutar la consulta
        cursor.execute(truncate_query)

        # Confirmar la transacción
        conn.commit()

        print(f"La tabla {tabla} ha sido truncada exitosamente.")

    except Exception as e:
        print(f"Error al truncar la tabla: {e}")
    
    finally:
        # Cerrar la conexión y el cursor
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def truncate_update_personal2(db_config):
    """
    Esta función ejecuta el comando TRUNCATE en la tabla 'personal'.
    
    Parámetros:
        db_config (dict): Un diccionario con la configuración de la base de datos.
                          Debe contener las claves 'dbname', 'user', 'password', 'host', y 'port'.
    """
    query2 = """
        INSERT INTO personal2 (
            id, 
            anyo, 
            remuneracionbruta_mensual, 
            remuliquida_mensual, 
            num_cuotas, 
            cantidad_de_pago, 
            detalle_de_base, 
            tipo_de_contrato, 
            base, 
            tipo_pago, 
            homologado, 
            nombrecompleto_x, 
            rut, 
            nombreencontrado, 
            organismo_nombre, 
            homologado_2, 
            mes, 
            tipo_calificacionp, 
            tipo_cargo, 
            key, 
            metodo
        ) 
        SELECT 
            id, 
            anyo, 
            remuneracionbruta_mensual, 
            remuliquida_mensual, 
            num_cuotas, 
            cantidad_de_pago, 
            detalle_de_base, 
            tipo_de_contrato, 
            base, 
            tipo_pago, 
            homologado, 
            nombrecompleto_x, 
            rut, 
            nombreencontrado, 
            organismo_nombre, 
            homologado_2, 
            mes, 
            tipo_calificacionp, 
            tipo_cargo, 
            key, 
            metodo 
        FROM personal2_base;
    """

    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Comando SQL para truncar la tabla
        truncate_query = sql.SQL(f"TRUNCATE TABLE personal2")


        # Ejecutar la consulta
        cursor.execute(truncate_query)

        cursor.execute(query2)

        # Confirmar la transacción
        conn.commit()

        #print(f"La tabla {tabla} ha sido truncada exitosamente.")

    except Exception as e:
        print(f"Error al truncar la tabla: {e}")
    
    finally:
        # Cerrar la conexión y el cursor
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_dataframe_to_postgres(df, conn_params):
    table_name = "personal2_base"
    """
    try:
        conn = psycopg2.connect(**conn_params)
        print("Conexión exitosa")
        
        # Crea un cursor
        cursor = conn.cursor()

        # Realiza una consulta (opcional)
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"Versión de la base de datos: {db_version[0]}")

        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()
    """
    conn_string = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params.get('port', 5432)}/{conn_params['dbname']}"

    try:
        # Crear un motor de SQLAlchemy
        engine = create_engine(conn_string)

        # Guardar el DataFrame en la tabla
        df.columns = ['organismo_nombre',
                        'anyo',
                        'mes',
                        'tipo_calificacionp',
                        'tipo_cargo',
                        'remuneracionbruta_mensual',
                        'remuliquida_mensual',
                        'base',
                        'tipo_pago',
                        'num_cuotas',
                        'nombrecompleto_x',
                        'rut',
                        'nombreencontrado',
                        'cantidad_de_pago',
                        'detalle_de_base',
                        'tipo_de_contrato',
                        'homologado',
                        'homologado_2',
                        'key',
                        'metodo']
        df.to_sql(table_name, engine, if_exists='append', index=False)
        #print(f"Datos guardados en la tabla personal con éxito.")
    except Exception as e:
        print(f"Ocurrió un error al guardar los datos: {e}")
        error_traceback = traceback.format_exc()
        print("Traceback detallado en SQL:")
        print(error_traceback)

# Ejemplo de uso
conn_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'UnaCasaEnUnArbol2024',
    'host': 'localhost'
    #'host': '186.67.61.251'
}



dtype_dict = {'organismo_nombre': 'object',
                'anyo': 'float64',
                'Mes': 'object',
                'Nombres': 'object',
                'Paterno': 'object',
                'Materno': 'object',
                'tipo_calificacionp': 'object',
                'Tipo cargo': 'object',
                'remuneracionbruta_mensual': 'object',
                'remuliquida_mensual': 'object',
                'base': 'object',
                'tipo_pago': 'object',
                'num_cuotas': 'float64'}

# URL base para descargar archivos
base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
# Lista de columnas deseadas que se van a filtrar
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']

# Definimos un patrón de expresión regular para buscar cadenas que comiencen con '90' seguidas de cualquier 6 caracteres.
pattern_maximo = r'^90.{6}$'

# Cargamos dos archivos CSV comprimidos con 'xz' y los concatenamos en dos DataFrames.
# Utilizamos el separador de tabulación (\t) para leer los datos.
aux_1 = pd.read_csv("ENCONTRADOS_11_1.csv", compression='xz', sep='\t')
aux_2 = pd.read_csv("ENCONTRADOS_11_2.csv", compression='xz', sep='\t')

# Concatenamos los DataFrames cargados para crear un DataFrame que contiene todos los datos históricos.
DB_RUT_HISTORICO = pd.concat([aux_1, aux_2])

# Seleccionamos solo las columnas relevantes y creamos un nuevo DataFrame con esas columnas.
DB_RUT = pd.concat([aux_1, aux_2])[['NombreCompleto', 'rut', 'Nombre_merge', 'Fecha']]

# Rellenamos los valores nulos en las columnas 'NombreCompleto' y 'Nombre_merge' con cadenas vacías.
DB_RUT["NombreCompleto"] = DB_RUT["NombreCompleto"].fillna("")
DB_RUT["Nombre_merge"]   = DB_RUT["Nombre_merge"].fillna("")

# Cargamos dos archivos CSV comprimidos con 'xz' y los concatenamos en un solo DataFrame.
# Utilizamos el separador de tabulación (\t) para leer los datos.
DB_SERVEL = pd.concat([
    pd.read_csv("RUT1.csv", compression='xz', sep='\t'),
    pd.read_csv("RUT2.csv", compression='xz', sep='\t')
])

# Cargamos un archivo Excel y leemos la hoja "MATRIZ", saltándonos las dos primeras filas.
ref = pd.read_excel(r"calificacion_key3 (7).xlsx", sheet_name="MATRIZ", skiprows=2)

# Seleccionamos solo las columnas 'key' y 'Secuencia' del DataFrame cargado.
ref2 = ref[["key", "Secuencia"]]

# Ordenamos el DataFrame 'ref2' por la columna 'Secuencia' de forma ascendente.
ref3 = ref2.sort_values(by="Secuencia")

# Convertimos la columna 'key' del DataFrame 'ref3' en una lista.
listaCalificacion = list(ref3["key"])

# Seleccionamos solo las columnas 'key' y 'Homologado' del DataFrame original para crear 'refFinal'.
refFinal = ref[["key", "Homologado"]]


# Cargamos un archivo Excel y leemos la hoja llamada "homologa2", omitiendo las dos primeras filas.
hologado2 = pd.read_excel(r"homologa2 (1).xlsx", sheet_name="homologa2", skiprows=2)

# Seleccionamos las columnas 'Homologado', 'key' y 'Secuencia', y ordenamos el DataFrame por la columna 'Secuencia'.
hologado22 = hologado2[["Homologado", "key", "Secuencia"]].sort_values("Secuencia")

# Seleccionamos las columnas 'key', 'Homologado' y 'Homologado 2' del DataFrame.
# Cambiamos el nombre de las columnas para que sean más descriptivas en el siguiente paso.
hologado2_x = hologado2[["key", "Homologado", "Homologado 2"]]

# Creamos una lista con los valores únicos de la columna 'Homologado' del DataFrame 'hologado22'.
lista_homologado_original = list(hologado22["Homologado"].unique())

# Preparar el DataFrame para el merge: 
# Renombramos las columnas de 'hologado2_x' para que 'key' se convierta en 'key2' y mantener las columnas 'Homologado' y 'Homologado 2'.
hologado2_x.columns = ['key2', 'Homologado', 'Homologado 2']

# Eliminamos las filas duplicadas en el DataFrame 'hologado2_x' para tener valores únicos.
hologado2_x2 = hologado2_x.drop_duplicates()

# Definimos las rutas de los archivos CSV utilizando una base común para el nombre de archivo.
# 'base' es una variable que contiene el directorio donde están ubicados los archivos CSV.
TA_PersonalPlanta                       = f"{base}TA_PersonalPlanta.csv"
TA_PersonalContrata                     = f"{base}TA_PersonalContrata.csv"
TA_PersonalCodigotrabajo                = f"{base}TA_PersonalCodigotrabajo.csv"
TA_PersonalContratohonorarios           = f"{base}TA_PersonalContratohonorarios.csv"

# Creamos diccionarios de columnas deseadas para cada DataFrame.
# 'deseadas' es una lista de columnas que queremos incluir en cada DataFrame.
PersonalPlantaDICT                = deseadas + ["remuliquida_mensual", 'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContrataDICT              = deseadas + ["remuliquida_mensual", 'Tipo cargo', 'remuneracionbruta_mensual']
PersonalCodigotrabajoDICT         = deseadas + ["remuliquida_mensual", 'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContratohonorariosDICT    = deseadas + ['remuliquida_mensual', 'tipo_pago', 'num_cuotas', 'remuneracionbruta']

# Lista de comunas municipales en Chile
comunas = [
    'Corporación Municipal de Providencia',
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
    'Municipalidad de Ñuñoa'
]


# Lista de términos a excluir en el procesamiento de datos
exclusion_list = [
    "NO", "DEL", "DE", "SAN", "A", "LA", "HONORARIOS", "LAS", "TRABAJO",
    "CODIGO", "ASISTENTE", "EDUCACION", "C", "SIN", "DOCENTE", "APELLIDO",
    "Y", "M", "E"
]

####   FUNCIONES #####

def getSimilitud(name1, name2):
    """
    Calcula la similitud entre dos cadenas utilizando el ratio de coincidencia de fuzzywuzzy.
    
    Args:
        name1 (str): La primera cadena.
        name2 (str): La segunda cadena.
        
    Returns:
        int: El porcentaje de similitud entre las dos cadenas.
    """
    return fuzz.ratio(name1, name2)

def string_to_url(s):
    """
    Convierte una cadena en una URL codificada.
    
    Args:
        s (str): La cadena a codificar.
        
    Returns:
        str: La cadena codificada en formato URL.
    """
    return urllib.parse.quote(s)

def fixRemuneracion(valor):
    """
    Intenta convertir un valor a un entero, eliminando los últimos dos caracteres.
    
    Args:
        valor (str): La cadena que representa un valor de remuneración.
        
    Returns:
        int or None: El valor entero sin los últimos dos caracteres, o None si no se puede convertir.
    """
    try:
        return int(valor[:-2])
    except:
        return None  # Corregido para devolver None explícitamente en caso de error

def eliminar_espacios_adicionales(cadena):
    """
    Elimina espacios adicionales en una cadena de texto y maneja el caso de valores flotantes.
    
    Args:
        cadena (str or float): La cadena de texto a limpiar o un valor flotante.
        
    Returns:
        str: La cadena limpia de espacios adicionales o "NO" si el valor es flotante.
    """
    if isinstance(cadena, float):
        return "NO"  # Maneja el caso de valores flotantes devolviendo "NO"
    return re.sub(r'\s+', ' ', cadena).strip()  # Reemplaza múltiples espacios por uno solo y elimina espacios en los extremos

def transformar_string(texto):
    """
    Transforma una cadena de texto para que esté en mayúsculas y sin acentos.
    
    Args:
        texto (str): La cadena de texto a transformar.
        
    Returns:
        str: La cadena transformada en mayúsculas y sin acentos.
    """
    # Convertir a mayúsculas
    texto = texto.upper()
    # Reemplazar tildes por letras sin tilde
    texto = texto.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
    # Reemplazar la letra "ñ" por "n"
    texto = texto.replace('Ñ', 'N')
    return texto

def limpiar_texto(texto):
    """
    Elimina caracteres no deseados de una cadena de texto y la convierte a mayúsculas.
    
    Args:
        texto (str): La cadena de texto a limpiar.
        
    Returns:
        str: La cadena de texto limpia, solo con letras mayúsculas y espacios.
    """
    # Eliminar caracteres no deseados y convertir a mayúsculas
    texto_limpio = re.sub(r'[^A-Z ]', '', texto.upper())
    return texto_limpio

def get_nombre_completo(df):
    """
    Procesa las columnas 'Nombres', 'Paterno' y 'Materno' de un DataFrame para crear una nueva columna 'NombreCompleto'.
    La función limpia y transforma los textos antes de combinarlos en una sola columna.
    
    Args:
        df (pd.DataFrame): El DataFrame que contiene las columnas 'Nombres', 'Paterno' y 'Materno'.
        
    Returns:
        pd.DataFrame: El DataFrame original con una nueva columna 'NombreCompleto' añadida.
    """
    # Aplicar funciones de limpieza y transformación a la columna 'Nombres'
    df["Nombres2"] = df["Nombres"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    
    # Aplicar funciones de limpieza y transformación a la columna 'Paterno'
    df["Paterno2"] = df["Paterno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    
    # Aplicar funciones de limpieza y transformación a la columna 'Materno'
    df["Materno2"] = df["Materno"].apply(eliminar_espacios_adicionales).apply(transformar_string).apply(limpiar_texto)
    
    # Crear la columna 'NombreCompleto' combinando las columnas procesadas 'Paterno2', 'Materno2' y 'Nombres2'
    df["NombreCompleto"] = df[['Paterno2', 'Materno2', 'Nombres2']].apply(
        lambda row: ' '.join(row.dropna().astype(str)).strip(), axis=1
    )    
    return df


def rutificar_problematico(df):
    """
    Realiza una combinación (merge) entre el DataFrame proporcionado y el DataFrame DB_SERVEL
    para agregar información sobre el RUT a los registros problemáticos.

    Args:
        df (pd.DataFrame): El DataFrame que contiene los registros problemáticos, con una columna 'nombre'.
        
    Returns:
        pd.DataFrame: Un DataFrame con la información del RUT añadida, incluyendo 'NombreCompleto', 'RUN', 'DV', y 'Nombre_merge'.
    """
    # Realizar una combinación (merge) con el DataFrame DB_SERVEL en base a la columna 'nombre' del DataFrame actual
    # y la columna 'Nombre_merge' de DB_SERVEL. La combinación se hace de tipo 'left', por lo que se mantendrán todos los registros del DataFrame actual.
    resultado = df.merge(DB_SERVEL, left_on="nombre", right_on="Nombre_merge", how="left")
    
    # Seleccionar las columnas relevantes para el resultado final
    resultado = resultado[['NombreCompleto', 'RUN', 'DV', 'Nombre_merge']]
    
    return resultado

def consolidar_rutificado(salida):
    """
    Combina información del DataFrame de salida con datos de RUT y crea una nueva columna 'rut' formateada.
    
    Args:
        salida (pd.DataFrame): El DataFrame de entrada que contiene información para el RUT.
        
    Returns:
        pd.DataFrame: Un DataFrame con las columnas 'NombreCompleto', 'Nombre_merge' y 'rut' consolidadas.
    """
    # Llamar a la función rutificar_problematico para obtener un DataFrame con información del RUT
    df_salida = rutificar_problematico(salida)
    
    # Crear la columna 'rut' combinando las columnas 'RUN' y 'DV', formateando el resultado
    df_salida["rut"] = df_salida[['RUN', 'DV']].apply(
        lambda row: '-'.join(row.dropna().astype(str)).strip().replace(".0",""), axis=1
    )
    
    # Seleccionar y devolver las columnas relevantes
    return df_salida[["NombreCompleto", "Nombre_merge", "rut"]]


def get_rut_maximo():
    """
    Calcula el valor máximo del RUT en el DataFrame DB_RUT, excluyendo aquellos que contienen un guion ('-').

    Returns:
        int: El valor máximo del RUT como un entero.
    """
    
    # Filtrar los registros del DataFrame DB_RUT donde la columna 'rut' no contiene un guion ('-')
    filtrado = DB_RUT[DB_RUT["rut"].apply(lambda x: "-" not in x)]
    
    # Obtener el valor máximo de la columna 'rut' después del filtrado y convertirlo a entero
    max_rut = int(filtrado["rut"].max())
    
    return max_rut

def rutificar_no_encontrado(df):
    """
    Añade una columna 'rut' al DataFrame de entrada con valores numéricos secuenciales, comenzando desde el RUT máximo actual.
    La nueva columna 'rut' se crea para los registros que no tienen un RUT asignado previamente.
    
    Args:
        df (pd.DataFrame): El DataFrame que contiene los registros sin RUT asignado, con una columna 'NombreCompleto'.
        
    Returns:
        pd.DataFrame: El DataFrame original con una nueva columna 'rut' añadida.
    """
    # Seleccionar la columna 'NombreCompleto' y hacer una copia para evitar el SettingWithCopyWarning
    df = df[["NombreCompleto"]].copy()
    
    # Obtener el valor máximo del RUT actual
    rut_maximo = get_rut_maximo()

    #df["rut"] = [str(x) for x in range(rut_maximo+1,rut_maximo + 1 + len(df))]
    # Crear una nueva columna 'rut' con valores secuenciales basados en el valor máximo actual del RUT
    df.loc[:, "rut"] = [str(x) for x in range(rut_maximo + 1, rut_maximo + 1 + len(df))]
    
    return df

def actualizar_DB_RUT():
    global DB_RUT  # Declara que usarás la variable global DB_RUT
    trabajar = DB_RUT[DB_RUT["Nombre_merge"].isnull()]
    trabajar["Nombre_merge"] = trabajar["NombreCompleto"]
    nada = DB_RUT_HISTORICO[DB_RUT_HISTORICO["Nombre_merge"].notnull()]
    DB_RUT = pd.concat([trabajar,nada]).reset_index()
    del DB_RUT["index"]
    DB_RUT[:int(len(DB_RUT)/2)].to_csv("ENCONTRADOS_11_1.csv", index=False, compression='xz', sep='\t')
    DB_RUT[int(len(DB_RUT)/2):].to_csv("ENCONTRADOS_11_2.csv", index=False, compression='xz', sep='\t')
    return None


def save_new_rut(encontrado, no_encontrados):
    """
    Combina y guarda la información de RUT encontrada y no encontrada, actualizando los DataFrames globales
    y guardando los resultados en archivos CSV comprimidos.

    Args:
        encontrado (pd.DataFrame): DataFrame con los registros encontrados que incluyen RUT.
        no_encontrados (pd.DataFrame): DataFrame con los registros que no tienen un RUT asignado.

    Returns:
        pd.DataFrame: El DataFrame combinado y actualizado con la nueva información de RUT.
    """
    global DB_RUT
    global DB_RUT_HISTORICO
    
    # Consolidar y rutificar los registros encontrados y los no encontrados
    final = pd.concat([consolidar_rutificado(encontrado), rutificar_no_encontrado(no_encontrados)])
    
    # Añadir la fecha actual a cada registro
    final["Fecha"] = pd.to_datetime('today').normalize()
    final["Fecha"] = pd.to_datetime(final["Fecha"]).dt.date
    
    # Combinar los datos históricos con los nuevos datos
    concat = pd.concat([DB_RUT_HISTORICO, final])
    
    # Actualizar el DataFrame global DB_RUT_HISTORICO
    DB_RUT_HISTORICO = concat
    
    # Actualizar el DataFrame global DB_RUT con columnas específicas
    DB_RUT = concat[['NombreCompleto', 'rut', 'Nombre_merge', 'Fecha']]
    
    # Guardar los datos en dos archivos CSV comprimidos, dividiendo el DataFrame en dos partes
    #concat[:int(len(concat)/2)].to_csv("ENCONTRADOS_intento_1.csv", index=False, compression='xz', sep='\t')
    #concat[int(len(concat)/2):].to_csv("ENCONTRADOS_intento_2.csv", index=False, compression='xz', sep='\t')
    
    return final


def buscar_rut(df):    
    """
    Busca y asigna RUTs a los registros en el DataFrame que tienen RUTs faltantes,
    utilizando similitud de nombres para encontrar coincidencias en un DataFrame de referencia.

    Args:
        df (pd.DataFrame): DataFrame que contiene registros con una columna 'NombreCompleto' y posiblemente faltantes en la columna 'rut'.

    Returns:
        pd.DataFrame: DataFrame con los registros actualizados con RUTs asignados.
    """
    
    # Filtrar registros sin RUT y obtener una lista única de nombres completos
    merge2 = df[df["rut"].isnull()]
    problematico = merge2[["NombreCompleto"]].drop_duplicates().sort_values("NombreCompleto")
    
    diccionarioAcumulador = {}
    resto = problematico.copy()
    
    while resto.shape[0] > 0:
        # Concatenar todos los nombres y contar la frecuencia de palabras
        all_text = " ".join(resto["NombreCompleto"].dropna().to_list())
        words = all_text.split()
        filtered_words = [word for word in words if word not in exclusion_list]
        word_counts = Counter(filtered_words)
        most_common_words = word_counts.most_common(1)
        if not most_common_words:
            break
        # Obtener la palabra más común
        i = [x[0] for x in most_common_words][0]
        
        # Filtrar registros que contienen la palabra más común
        result = resto["NombreCompleto"].str.contains(i)
        diccionarioAcumulador[i] = resto[result]
        resto = resto[~result]
    
    # Crear un patrón de búsqueda basado en las palabras más comunes
    pattern = '|'.join(list(diccionarioAcumulador.keys()))
    
    # Buscar registros en DB_SERVEL que coincidan con el patrón
    datos6 = DB_SERVEL[DB_SERVEL['Nombre_merge'].str.contains(pattern, case=True, na=False)]  
    
    lista_palabras = list(diccionarioAcumulador.keys())
    
    def encontrar_nombre_similar(row, lista):
        """
        Encuentra el nombre más similar en la lista de referencia usando varias métricas de similitud.
        
        Args:
            row (str): Nombre completo del registro a comparar.
            lista (list): Lista de nombres de referencia para comparar.
        
        Returns:
            pd.Series: Serie con la similitud máxima y el nombre más similar encontrado.
        """
        try:
            nombre = row
            # Calcular similitudes utilizando diferentes métricas de similitud
            similaridades = [(fuzz.token_sort_ratio(nombre, y), y) for y in lista] + \
                             [(fuzz.ratio(nombre, y), y) for y in lista] + \
                             [(fuzz.partial_ratio(nombre, y), y) for y in lista] + \
                             [(fuzz.UQRatio(nombre, y), y) for y in lista] + \
                             [(fuzz.QRatio(nombre, y), y) for y in lista]
                             
            similaridad_maxima, nombre_mas_similar = max(similaridades, key=lambda x: x[0])
            return pd.Series([similaridad_maxima, nombre_mas_similar])
        except:
            print(f"Error en {nombre}")
            return pd.Series([None, None])

    acumulador = []
    
    for i in lista_palabras:
        print(f"\n{i}")
        ref = datos6[datos6['Nombre_merge'].str.contains(i, case=True, na=False)]["Nombre_merge"]
        aux = diccionarioAcumulador[i]
        
        # Aplicar la función de similitud para encontrar el nombre más parecido
        aux[["probabilidad", "nombre"]] = aux["NombreCompleto"].apply(lambda x: encontrar_nombre_similar(x, ref))
        acumulador.append(aux.copy())
    if not acumulador:
        return None
    
    # Concatenar todos los resultados acumulados
    salida = pd.concat(acumulador)
    
    valor = 85
    
    # Filtrar registros válidos y por rutificar en función de la probabilidad de coincidencia
    result = (salida.probabilidad >= valor)
    valido = salida[result]
    por_rutificar = salida[~result]
    
    # Guardar los registros válidos y los que necesitan más revisión
    salida = save_new_rut(valido, por_rutificar)
    
    return salida



def rutificador(df):
    merge = df.merge(DB_RUT, on="NombreCompleto",how="left")
    #merge.loc[merge["NombreCompleto"].isnull(), "rut"] = 90001171
    merge2 = merge[merge["rut"].isnull()]
    if not merge2.empty:
        buscar_rut(merge)
        merge = df.merge(DB_RUT, on="NombreCompleto",how="left")
        merge.loc[merge["NombreCompleto"] == "", "rut"] = "90001171"
    return merge

def getPagos(df):   
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
    is_homologado = df["Homologado"].isin(lista_homologado_original)
    acumuladoDF_homologado = df[is_homologado]
    acumuladoDF_no_homologado = df[~is_homologado]
    if acumuladoDF_homologado.shape[0] == 0:
        df["Homologado 2"] = "Sin Clasificar"
        df["Homologado"] = df["Homologado"].fillna("Sin Clasificar")
        return df
    


    acumulador = []
    acumulador_resto = []

    for i in lista_homologado_original:
        aux = acumuladoDF_homologado[acumuladoDF_homologado["Homologado"] == i]
        auxaux = hologado22[hologado22["Homologado"] == i]
        lista_homologado2 = auxaux["key"].unique()
        
        if not aux.empty:
            for j in lista_homologado2:
                mask = aux["clean"].str.contains(' '.join(j.split()))
                aux_subset = aux[mask].copy()
                aux_subset["key2"] = j
                acumulador.append(aux_subset)
                aux = aux[~mask]
                if aux.empty:
                    break

        acumulador_resto.append(aux)

    # Verificar si hay objetos no vacíos en acumulador_resto antes de concatenar
    if any(not x.empty for x in acumulador_resto):
        df_resto = pd.concat((x for x in acumulador_resto if not x.empty), ignore_index=True)
    else:
        df_resto = pd.DataFrame()  # Crear un DataFrame vacío en caso de que no haya nada que concatenar

    # Verificar si hay objetos no vacíos en acumulador antes de concatenar
    if any(not x.empty for x in acumulador):
        acumuladoDF = pd.concat((x for x in acumulador if not x.empty), ignore_index=True)
    else:
        acumuladoDF = pd.DataFrame()  # Crear un DataFrame vacío si no hay nada que concatenar

    # Si hay datos en df_resto, los concatenamos con acumuladoDF
    if not df_resto.empty:
        df_final = pd.concat([acumuladoDF, df_resto], ignore_index=True)
    else:
        df_final = acumuladoDF  # Si df_resto está vacío, usamos solo acumuladoDF

    # Merge y concatenación final
    df_final_merge = df_final.merge(hologado2_x2, how="left")
    final = pd.concat([df_final_merge, acumuladoDF_no_homologado], ignore_index=True)

    # Rellenar valores nulos
    final["Homologado"] = final["Homologado"].fillna("Sin Clasificar")
    final["Homologado 2"] = final["Homologado 2"].fillna("Sin Clasificar")

    return final

def save_dataframe_to_postgres2(df, conn_params):
    conn_string = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params.get('port', 5432)}/{conn_params['dbname']}"
    try:
        # Crear un motor de SQLAlchemy
        engine = create_engine(conn_string)
        df.to_sql("acumulado_rutificador", engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Ocurrió un error al guardar los datos: {e}")
        error_traceback = traceback.format_exc()
        print("Traceback detallado en SQL:")
        print(error_traceback) 


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
    except Exception as e:
        fecha_dt = datetime.strptime("2099-01", '%Y-%m')
    
    return fecha_dt


""" Nuevas Columnas para insertar en DB
'organismo_nombre', 'anyo', 'mes', 'tipo_calificacionp', 'tipo_cargo',
       'remuneracionbruta_mensual', 'remuliquida_mensual', 'base', 'tipo_pago',
       'num_cuotas', 'nombrecompleto_x', 'rut', 'nombreencontrado',
       'cantidad_de_pago', 'detalle_de_base', 'tipo_de_contrato', 'homologado',
       'homologado_2', 'key', 'metodo']
"""
def GetDetalle(df):
    #df = pd.read_excel(url)
    organismo = df.iloc[0].organismo_nombre
    df2 =df[(df["anyo"] >= 2000) & (df["anyo"] <= 2024)]
    df2["Fecha"] = df2.apply(get_fecha, axis=1)
    df3 = df2[[ 'Fecha','rut','homologado',"base"]].drop_duplicates()
    agrupado_in = df3.groupby(['rut','homologado',"base"]).min().reset_index()
    agrupado_in2 = agrupado_in.groupby(['Fecha','homologado',"base"]).count().reset_index().rename(columns = {"rut":"rut_in"})
    
    agrupado_out = df3.groupby(['rut','homologado',"base"]).max().reset_index()
    agrupado_out2 = agrupado_out.groupby(['Fecha','homologado',"base"]).count().reset_index().rename(columns = {"rut":"rut_out"})
    
    merge = agrupado_in2.merge(agrupado_out2, on=['Fecha','homologado',"base"], how="outer").sort_values(by=['Fecha', 'homologado', 'base'])
    
    fechas =  merge[["Fecha"]].drop_duplicates()
    Homologado = merge[['homologado']].drop_duplicates()
    bases = merge[['base']].drop_duplicates()
    base = fechas.merge(Homologado, how="cross").merge(bases,how="cross")
    merge = merge.merge(base, on=['Fecha', 'homologado', 'base'],how="outer")
    
    merge["rut_in"] = merge["rut_in"].fillna(0)
    merge["rut_out"] = merge["rut_out"].fillna(0)
    merge["Diferencia"] = merge["rut_in"] - merge["rut_out"]
    #merge['Acumulado'] = merge.groupby(['Fecha', 'Homologado', 'base'])['Diferencia'].cumsum()
    merge['Acumulado'] = merge.groupby(['homologado', 'base'])['Diferencia'].cumsum()
    merge["nueva_columna"] = organismo
    #merge.to_excel(archivo, index=False)
    return merge.rename(columns= lambda x: x.lower())




#def clean_file(url):
def clean_file(df):
    #df = pd.read_excel(url)
    df2 =df[(df["anyo"] >= 2000) & (df["anyo"] <= 2030)]
    df2["Fecha"] = df2.apply(get_fecha2, axis=1)
    fecha_actual = pd.Timestamp.today()
    df2 = df2[df2['Fecha'] < fecha_actual]
    
    return df2

def getEstadistica(df2):
    df_aux = df2[['rut',"Fecha",'remuneracionbruta_mensual', 'remuliquida_mensual']]
    df_oficio = df2[['rut','Fecha','base','Homologado', 'Homologado 2']].drop_duplicates()
    
    min_dates = df2.groupby("rut")["Fecha"].min().reset_index()
    #min_dates = min_dates.rename(columns={'Fecha': 'Fecha-Ingreso'})
    min_dates2 = min_dates.merge(df_aux)
    min_dates3 = min_dates2.groupby(["rut","Fecha"]).sum().reset_index()
    min_dates3 = min_dates3.sort_values("remuneracionbruta_mensual",  ascending=False)
    min_dates3_oficio = min_dates3.merge(df_oficio).drop_duplicates(subset=['rut','Fecha'], keep='first')
    
    max_dates = df2.groupby("rut")["Fecha"].max().reset_index()
    #max_dates = max_dates.rename(columns={'Fecha': 'Fecha-Egreso'})
    max_dates2 = max_dates.merge(df_aux)
    max_dates3 = max_dates2.groupby(["rut","Fecha"]).sum().reset_index()
    max_dates3 = max_dates3.sort_values("remuneracionbruta_mensual",  ascending=False)
    max_dates3_oficio = max_dates3.merge(df_oficio).drop_duplicates(subset=['rut','Fecha'], keep='first')
    min_final = min_dates3_oficio.rename(columns=lambda x: f'{x}_in' if x != 'rut' else x)
    max_final = max_dates3_oficio.rename(columns=lambda x: f'{x}_out' if x != 'rut' else x)
    max_final = max_dates3_oficio.rename(columns=lambda x: f'{x}_out' if x != 'rut' else x)
    max_final = max_dates3_oficio.rename(columns=lambda x: f'{x}_out' if x != 'rut' else x)
    
    merge = min_final.merge(max_final)
    #merge_in  = merge.groupby(["Fecha_in","base_in","Homologado_in"])["rut"].count().reset_index()
    merge_in = merge.groupby(["Fecha_in","base_in","Homologado_in"]).agg({"rut":"count","remuneracionbruta_mensual_in":"sum","remuliquida_mensual_in":"sum"}).reset_index()
    #merge_out = merge.groupby(["Fecha_out","base_out","Homologado_out"])["rut"].count().reset_index()
    merge_out = merge.groupby(["Fecha_out","base_out","Homologado_out"]).agg({"rut":"count","remuneracionbruta_mensual_out":"sum","remuliquida_mensual_out":"sum"}).reset_index()
    merge2 = merge_in.merge(merge_out, left_on=["Fecha_in",'base_in', 'Homologado_in'],right_on=["Fecha_out",'base_out', 'Homologado_out'], how="outer")
    
    merge2_all = merge2[(merge2["rut_x"].notnull()) & (merge2["rut_y"].notnull())]
    
    merge2_out = merge2[merge2["rut_x"].isnull()]
    merge2_out["Fecha_in"] = merge2_out["Fecha_out"]
    merge2_out["base_in"] = merge2_out["base_out"]
    merge2_out["Homologado_in"] = merge2_out["Homologado_out"]
    merge2_out["rut_x"] = 0
    
    merge2_in = merge2[merge2["rut_y"].isnull()]
    merge2_in["Fecha_out"] = merge2_in["Fecha_in"]
    merge2_in["base_out"] = merge2_in["base_in"]
    merge2_in["Homologado_out"] = merge2_in["Homologado_in"]
    merge2_in["rut_y"] = 0
    
    merge3 = pd.concat([merge2_out,merge2_in,merge2_all])
    
    merge4 = merge3.rename(columns={"rut_x":"rut_in","rut_y":"rut_out","Fecha_in":"Fecha","Homologado_in":"Homologado","base_in":"base"})
    del merge4["Fecha_out"]
    del merge4["Homologado_out"]
    del merge4["base_out"]
    fechas =  df2[["Fecha"]].drop_duplicates()
    Homologado = df2[['Homologado']].drop_duplicates()
    bases = df2[['base']].drop_duplicates()
    base = fechas.merge(Homologado, how="cross").merge(bases,how="cross")
    
    merge4 = merge4.merge(base, on=['Fecha', 'Homologado', 'base'],how="outer")
    
    lista_fill = [ 'rut_in','remuneracionbruta_mensual_in', 'remuliquida_mensual_in', 'rut_out',
           'remuneracionbruta_mensual_out', 'remuliquida_mensual_out']
    
    for i in lista_fill:
        merge4[i] = merge4[i].fillna(0)   
    
    Resumen_Fecha2 = merge4.groupby(['Fecha', 'base'])[['rut_in','remuneracionbruta_mensual_in', 'remuliquida_mensual_in',  'rut_out', 'remuneracionbruta_mensual_out', 'remuliquida_mensual_out']].sum().reset_index()
    Resumen_Fecha2["Diferencia"] =  Resumen_Fecha2["rut_in"] - Resumen_Fecha2["rut_out"]
    Resumen_Fecha2['Acumulado'] = Resumen_Fecha2.groupby(['base'])['Diferencia'].cumsum()
    return [merge,merge4,Resumen_Fecha2]

def GetDetalle2(df2):
    df3 = df2[['Fecha','rut','Homologado',"base"]].drop_duplicates()
    agrupado_in = df3.groupby(['rut','Homologado',"base"]).min().reset_index()
    agrupado_in2 = agrupado_in.groupby(['Fecha','Homologado',"base"]).count().reset_index().rename(columns = {"rut":"rut_in"})
    
    agrupado_out = df3.groupby(['rut','Homologado',"base"]).max().reset_index()
    agrupado_out2 = agrupado_out.groupby(['Fecha','Homologado',"base"]).count().reset_index().rename(columns = {"rut":"rut_out"})
    
    merge = agrupado_in2.merge(agrupado_out2, on=['Fecha','Homologado',"base"], how="outer").sort_values(by=['Fecha', 'Homologado', 'base'])
    
    fechas =  df2[["Fecha"]].drop_duplicates()
    Homologado = df2[['Homologado']].drop_duplicates()
    bases = df2[['base']].drop_duplicates()
    base = fechas.merge(Homologado, how="cross").merge(bases,how="cross")
    merge = merge.merge(base, on=['Fecha', 'Homologado', 'base'],how="outer")
    
    merge["rut_in"] = merge["rut_in"].fillna(0)
    merge["rut_out"] = merge["rut_out"].fillna(0)
    merge["Diferencia"] = merge["rut_in"] - merge["rut_out"]
    #merge['Acumulado'] = merge.groupby(['Fecha', 'Homologado', 'base'])['Diferencia'].cumsum()
    merge['Acumulado'] = merge.groupby(['Homologado', 'base'])['Diferencia'].cumsum()
    merge = merge.rename(columns={"rut_in":"homologado_in","rut_out":"homologado_out","Diferencia":"homologado_diferencia","Acumulado":"homologado_acumulado"})
    return merge 

def save_dataframe_general(df,tabla,conn_params):
    conn_string = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params.get('port', 5432)}/{conn_params['dbname']}"
    try:
        # Crear un motor de SQLAlchemy
        engine = create_engine(conn_string)
        df.to_sql(tabla, engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Ocurrió un error al guardar los datos: {e}")
        error_traceback = traceback.format_exc()
        print("Traceback detallado en SQL:")
        print(error_traceback)

def get_resumen_anyo(df):
    df2 = df[['organismo_nombre','anyo', 'Mes','rut','remuneracionbruta_mensual', 'remuliquida_mensual']]
    df2['remuneracionbruta_mean'] = df2.groupby(['organismo_nombre', 'anyo'])['remuneracionbruta_mensual'].transform('mean')
    df2['remuneracionbruta_std'] = df2.groupby(['organismo_nombre', 'anyo'])['remuneracionbruta_mensual'].transform('std')

    df2['remuliquida_mean'] = df2.groupby(['organismo_nombre', 'anyo'])['remuliquida_mensual'].transform('mean')
    df2['remuliquida_std'] = df2.groupby(['organismo_nombre', 'anyo'])['remuliquida_mensual'].transform('std')

    # Crear columnas que indican si cumplen la condición considerando el promedio y la desviación estándar por año
    df2['remuneracionbruta_superior'] = df2['remuneracionbruta_mensual'] > (df2['remuneracionbruta_mean'] + 3 * df2['remuneracionbruta_std'])
    df2['remuliquida_superior'] = df2['remuliquida_mensual'] > (df2['remuliquida_mean'] + 3 * df2['remuliquida_std'])

    df2['remuliquida_10']       = df2['remuliquida_mensual']       > 5000000
    df2['remuneracionbruta_10'] = df2['remuneracionbruta_mensual'] > 5000000


    df2['rut_count'] = df2.groupby(['organismo_nombre', 'anyo', 'rut'])['rut'].transform('count')

    # Crear columnas booleanas para los casos de duplicados, triplicados y más de 4
    df2['rut_12'] = (df2['rut_count'] > 12) & (df2['rut_count'] < 18)
    df2['rut_18'] = (df2['rut_count'] >= 18) & (df2['rut_count'] < 24)
    df2['rut_24'] = df2['rut_count'] >= 24
    df3 = df2.groupby(by=['organismo_nombre', 'anyo']).agg(
        rut_unique_count=('rut', 'nunique'),
        remuneracionbruta_sum=('remuneracionbruta_mensual', 'sum'),
        remuliquida_sum=('remuliquida_mensual', 'sum'),
        meses_unicos=('Mes', 'nunique'),
        pagos=('remuneracionbruta_mensual', 'count'),
        remuneracionbruta_superior_count=('remuneracionbruta_superior', 'sum'),
        remuliquida_superior_count=('remuliquida_superior', 'sum'),
        remuneracionbruta_superior_10=('remuneracionbruta_10', 'sum'),
        remuliquida_superior_10=('remuliquida_10', 'sum'),

        remuneracionbruta_mean=('remuneracionbruta_mean', 'mean'),
        remuliquida_mean=('remuliquida_mean', 'mean'),
        
        rut_12_count=('rut_12', 'sum'),
        rut_24_count=('rut_18', 'sum'),
        rut_36_count=('rut_24', 'sum')
    ).reset_index()
    df3["max_teorico"] = df3["rut_unique_count"] * df3["meses_unicos"]
    df3["dif_pagos"] = df3["pagos"] - df3["max_teorico"]
    df3["pago_x_persona"] = df3["pagos"] / df3["rut_unique_count"]
    save_dataframe_general(df3,"resumen_pago",conn_params)
    return True

def get_resumen_anyo2(df):
    df2 = df[['organismo_nombre','anyo', 'Mes','rut','remuneracionbruta_mensual', 'remuliquida_mensual']]
    df2['remuneracionbruta_mean'] = df2.groupby(['organismo_nombre', 'anyo'])['remuneracionbruta_mensual'].transform('mean')
    df2['remuneracionbruta_std'] = df2.groupby(['organismo_nombre', 'anyo'])['remuneracionbruta_mensual'].transform('std')

    df2['remuliquida_mean'] = df2.groupby(['organismo_nombre', 'anyo'])['remuliquida_mensual'].transform('mean')
    df2['remuliquida_std'] = df2.groupby(['organismo_nombre', 'anyo'])['remuliquida_mensual'].transform('std')

    # Crear columnas que indican si cumplen la condición considerando el promedio y la desviación estándar por año
    df2['remuneracionbruta_superior'] = df2['remuneracionbruta_mensual'] > (df2['remuneracionbruta_mean'] + 3 * df2['remuneracionbruta_std'])
    df2['remuliquida_superior'] = df2['remuliquida_mensual'] > (df2['remuliquida_mean'] + 3 * df2['remuliquida_std'])

    df2['remuliquida_10']       = df2['remuliquida_mensual']       > 5000000
    df2['remuneracionbruta_10'] = df2['remuneracionbruta_mensual'] > 5000000


    df2['rut_count'] = df2.groupby(['organismo_nombre', 'anyo', 'rut'])['rut'].transform('count')

    # Crear columnas booleanas para los casos de duplicados, triplicados y más de 4
    df2['rut_menos_12'] = df2['rut_count'] <= 12
    df2['rut_12'] = (df2['rut_count'] > 12) & (df2['rut_count'] < 18)
    df2['rut_18'] = (df2['rut_count'] >= 18) & (df2['rut_count'] < 24)
    df2['rut_24'] = df2['rut_count'] >= 24

    df3 = df2.groupby(by=['organismo_nombre', 'anyo']).agg(
        rut_unique_count=('rut', 'nunique'),
        remuneracionbruta_sum=('remuneracionbruta_mensual', 'sum'),
        remuliquida_sum=('remuliquida_mensual', 'sum'),
        meses_unicos=('Mes', 'nunique'),
        pagos=('remuneracionbruta_mensual', 'count'),
        
        # Contar RUTs únicos que cumplen con las condiciones
        remuneracionbruta_superior_rut_count=('rut', lambda x: x[df2['remuneracionbruta_superior']].nunique()),
        remuliquida_superior_rut_count=('rut', lambda x: x[df2['remuliquida_superior']].nunique()),
        remuneracionbruta_10_rut_count=('rut', lambda x: x[df2['remuneracionbruta_10']].nunique()),
        remuliquida_10_rut_count=('rut', lambda x: x[df2['remuliquida_10']].nunique()),

        remuneracionbruta_superior_count=('remuneracionbruta_superior', 'sum'),
        remuliquida_superior_count=('remuliquida_superior', 'sum'),
        remuneracionbruta_superior_10=('remuneracionbruta_10', 'sum'),
        remuliquida_superior_10=('remuliquida_10', 'sum'),

        remuneracionbruta_mean=('remuneracionbruta_mean', 'mean'),
        remuliquida_mean=('remuliquida_mean', 'mean'),

        pagos__menos_12_count=('rut_menos_12', 'sum'),
        pagos_12_count=('rut_12', 'sum'),
        pagos_18_count=('rut_18', 'sum'),
        pagos_24_count=('rut_24', 'sum'),


        rut__menos_12_count=('rut', lambda x: x[df2['rut_menos_12']].nunique()),
        rut_12_count=('rut', lambda x: x[df2['rut_12']].nunique()),
        rut_18_count=('rut', lambda x: x[df2['rut_18']].nunique()),
        rut_24_count=('rut', lambda x: x[df2['rut_24']].nunique()),

        
    ).reset_index()
    df3["max_teorico"] = df3["rut_unique_count"] * df3["meses_unicos"]
    df3["dif_pagos"] = df3["pagos"] - df3["max_teorico"]
    df3["pago_x_persona"] = df3["pagos"] / df3["rut_unique_count"]
    save_dataframe_general(df3,"resumen_pago2",conn_params)
    return True



def global_resumen(df):
    organismo = df.iloc[0].organismo_nombre
    df2 = clean_file(df)
    get_resumen_anyo(df2)
    get_resumen_anyo2(df2)
    resultado1 = getEstadistica(df2)
    resultado2 = GetDetalle2(df2)
    merge = resultado1[1].merge(resultado2,on=['Fecha', 'base', 'Homologado'])
    base_rut = DB_RUT[['NombreCompleto', 'rut']].drop_duplicates(subset=["rut"])
    resultado1[0] = resultado1[0].merge(base_rut, how="left")
    """
    with pd.ExcelWriter(archivo_excel) as writer:
        resultado1[0].to_excel(writer, sheet_name='Union', index=False)
        merge.to_excel(writer, sheet_name='Resumen_Fecha', index=False)
        resultado1[2].to_excel(writer, sheet_name='Resumen_Fecha2', index=False)
    """
    acumulado_rut_detallle_entrada_salida    = resultado1[0].copy()
    acumulado_resumen_rut_homolago_acumulado = merge
    acumulado_resumen_solo_rut_acumulado     = resultado1[2].copy()

    acumulado_rut_detallle_entrada_salida   ["organismo"]    =  organismo
    acumulado_resumen_rut_homolago_acumulado["organismo"]    =  organismo
    acumulado_resumen_solo_rut_acumulado    ["organismo"]    =  organismo

    acumulado_rut_detallle_entrada_salida    .columns = ["rut","fecha_in","remuneracionbruta_mensual_in","remuliquida_mensual_in",
                                                         "base_in","homologado_in","homologado2_in","fecha_out","remuneracionbruta_mensual_out",
                                                         "remuliquida_mensual_out","base_out","homologado_out","homologado2_out",
                                                         "nombre_completo","organismo"]
    acumulado_resumen_rut_homolago_acumulado .columns = ["fecha","base","homologado","rut_in","remuneracionbruta_mensual_in",
                                                         "remuliquida_mensual_in","rut_out","remuneracionbruta_mensual_out",
                                                         "remuliquida_mensual_out","homologado_in","homologado_out","homologado_diferencia",
                                                         "homologado_acumulado","organismo"]
    acumulado_resumen_solo_rut_acumulado     .columns = ["fecha","base","rut_in","remuneracionbruta_mensual_in","remuliquida_mensual_in",
                                                         "rut_out","remuneracionbruta_mensual_out","remuliquida_mensual_out",
                                                         "diferencia","acumulado","organismo"]

    save_dataframe_general(acumulado_rut_detallle_entrada_salida,"acumulado_rut_detallle_entrada_salida",conn_params)
    save_dataframe_general(acumulado_resumen_rut_homolago_acumulado,"acumulado_resumen_rut_homolago_acumulado",conn_params)
    save_dataframe_general(acumulado_resumen_solo_rut_acumulado,"acumulado_resumen_solo_rut_acumulado",conn_params)

def process_comuna(url):
    #base = string_to_url(comuna)
    #url = f"https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/{base}.csv"
    #print(url)
    try:   
        df = pd.read_csv(url, compression='xz', sep='\t', dtype=dtype_dict)
    except Exception as e: 
        df = pd.read_csv(url, compression='xz', sep='\t')
    
    try:
        # Leer el archivo CSV
        #print(1)
        #df = pd.read_csv(url, compression='xz', sep='\t', dtype=dtype_dict) #.head(800000)
        

        # Procesar el DataFrame a través de las funciones específicas
        df = get_nombre_completo(df)
        #print(2)
        df = rutificador(df)[['organismo_nombre', 'anyo', 'Mes', 
       'tipo_calificacionp', 'Tipo cargo', 'remuneracionbruta_mensual',
       'remuliquida_mensual', 'base', 'tipo_pago', 'num_cuotas','NombreCompleto', 'rut', 'Nombre_merge']]
        #print(3)
        df = getPagos(df)
        #print(4)
        df = calificacion_nivel_1(df)[['organismo_nombre', 'anyo', 'Mes', 'tipo_calificacionp',
       'Tipo cargo', 'remuneracionbruta_mensual', 'remuliquida_mensual',
       'base', 'tipo_pago', 'num_cuotas', 'NombreCompleto', 'rut',
       'Nombre_merge', 'Cantidad de pagos en un mes',
        'Detalle de base en pagos en un mes',
       'Tipo de contrato distintos', 'Homologado','key',"clean"]]
        #print(5)
        df = calificacion_nivel_2(df)[['organismo_nombre', 'anyo', 'Mes', 'tipo_calificacionp',
       'Tipo cargo', 'remuneracionbruta_mensual', 'remuliquida_mensual',
       'base', 'tipo_pago', 'num_cuotas', 'NombreCompleto', 'rut',
       'Nombre_merge', 'Cantidad de pagos en un mes',
        'Detalle de base en pagos en un mes',
       'Tipo de contrato distintos', 'Homologado',  'Homologado 2','key']]
        #print(6)
        df = df.rename(columns={'NombreCompleto': 'NombreCompleto_x', 'Nombre_merge': 'NombreEncontrado'})
        #print(7)
        df["metodo"] = ""
        src.HISTORIAL.pagos_multiples(df)
        get_historial_persona(df,"2024-06",save_dataframe_general,conn_params)
        #Guardar el DataFrame procesado en un archivo Excel
        #df.to_excel(f"test/{comuna}.xlsx", index=False)
        #df.to_csv(f"test/{comuna}.csv", index=False,compression='xz', sep='\t')
        
        global_resumen(df)
        
        save_dataframe_to_postgres(df, conn_params)
        
        merge = GetDetalle(df)
        save_dataframe_to_postgres2(merge, conn_params)

    except Exception as e:
        print(f"Error al procesar {url}: {e}")
        error_traceback = traceback.format_exc()
        print("Traceback detallado:")
        print(error_traceback)


def listar_archivos(carpeta):
    try:
        # Obtener la lista de todos los archivos y directorios en la carpeta
        archivos = os.listdir(carpeta)
        
        # Filtrar solo los archivos (no subdirectorios)
        archivos = [f for f in archivos if os.path.isfile(os.path.join(carpeta, f))]
        
        return archivos
    except FileNotFoundError:
        print(f"La carpeta '{carpeta}' no existe.")
        return []
    except Exception as e:
        print(f"Error al listar archivos: {e}")
        return []

def save_estadistica_db_rut_historico():
    salida = DB_RUT_HISTORICO.groupby(by=["Fecha"]).size().reset_index()
    salida.to_excel("estadistica_db_rut_historico.xlsx", index=False)

lista_municipalidad = ['Municipios de Tarapacá',
        'Municipios de Los Lagos',
        'Municipios de Aysen del General Carlos Ibáñez del Campo',
        'Municipios de Magallanes y de la Antártica Chilena',
        'Municipios de R. Metropolitana de Santiago',
        'Municipios de Los Ríos',
        'Municipios de Arica y  Parinacota',
        'Municipios de Ñuble',
        'Municipios de Antofagasta',
        'Municipios de Atacama',
        'Municipios de Coquimbo',
        'Municipios de Valparaíso',
        'Municipios del Libertador General Bernardo OHiggins',
        'Municipios del Maule',
        'Municipios del Bíobio',
        'Municipios de La Araucanía', 'Corporaciones Municipales']

def isMuni(padre):
    if padre in lista_municipalidad:
        return 1
    return 0


def save_organismo360():
    
    truncate_table_personal_general(conn_params,"organismo360")
    url =  "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/Organismos_360.csv"
    df = pd.read_csv(url,sep=";",encoding="latin")
    df2 = df.rename(columns = lambda x: x.lower())
    df2["municipal"] = df2["padre_org"].apply(isMuni)
    save_dataframe_general(df2,"organismo360",conn_params)

def recorrer_organismo_historial():
    global DB_RUT
    H2.recorrer_organismo(DB_RUT)
    return None

def GLOBAL():
    save_organismo360()
    print("Limpiando tabla")
    #truncate_table_personal(conn_params)
    truncate_table_personal_general(conn_params,"personal2_base")
    truncate_table_personal_general(conn_params,"acumulado_rutificador")
    
    truncate_table_personal_general(conn_params,"acumulado_rut_detallle_entrada_salida")
    truncate_table_personal_general(conn_params,"acumulado_resumen_rut_homolago_acumulado")
    truncate_table_personal_general(conn_params,"acumulado_resumen_solo_rut_acumulado")
    truncate_table_personal_general(conn_params,"resumen_pago")
    truncate_table_personal_general(conn_params,"resumen_pago2")
    truncate_table_personal_general(conn_params,"resumen_personal_2024_6")
    n = 1
    cantidad_organismo = len(listar_archivos("organismo/"))
    for i in listar_archivos("organismo/"):
        #print(i, end='\r')
        print(f"\r{n} de {cantidad_organismo} {i[:150]:<150} ", end='')
        url = f"organismo/{i}"
        process_comuna(url)
        n += 1

    actualizar_DB_RUT()
    truncate_update_personal2(conn_params)
    save_estadistica_db_rut_historico()
    recorrer_organismo_historial()
    


if __name__ == '__main__':
    GLOBAL()




