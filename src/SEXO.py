import pandas as pd
from gender_guesser.detector import Detector
from collections import Counter


def nombre_sexo(nombres):
    # Configurar detector
    d = Detector(case_sensitive=False)
    acumulador = []

    def determinar_genero(nombre_completo):
        # Dividir el nombre en partes
        partes = [parte.strip() for parte in str(nombre_completo).split()]
        generos = []
        
        for parte in partes:
            # Filtrar partículas comunes y nombres muy cortos
            if len(parte) > 2 and parte.lower() not in ['de', 'la', 'y', 'del', 'los', 'las', 'san', 'santa']:
                genero = d.get_gender(parte.capitalize())
                if genero not in ['unknown', 'andy']:
                    # Mapear a categorías más simples
                    if genero in ['male', 'mostly_male']:
                        generos.append('male')
                    elif genero in ['female', 'mostly_female']:
                        generos.append('female')
        
        # Determinar género por mayoría
        if generos:
            conteo = Counter(generos)
            genero_mas_comun = conteo.most_common(1)[0][0]
            return genero_mas_comun
        else:
            return 'unknown'

    # Aplicar la función a cada nombre
    for nombre in nombres["nombreencontrado"]:
        genero = determinar_genero(nombre)
        acumulador.append((nombre, genero))

    # Convertir a DataFrame
    resultados = pd.DataFrame(acumulador, columns=["nombreencontrado", "sexo"])
    return resultados



def get_sexo(df, csv_path="sexo.csv"):
    df = df.rename(columns={'NombreEncontrado': 'nombreencontrado'})
    # Leer archivo existente con resultados
    resultados = pd.read_csv(csv_path)
    resultados.columns = ["nombreencontrado", "sexo"]
    
    # Primer merge
    merge = df.merge(resultados, how="left", on="nombreencontrado")
    
    # Detectar nombres sin sexo asignado
    problema = merge[merge["sexo"].isnull()][["nombreencontrado"]].drop_duplicates()
    
    # Si hay problemas, agregarlos al CSV
    if not problema.empty:
        # Asignar un valor por defecto
        problema= nombre_sexo(problema)
        
        # Concatenar al dataframe original
        resultados = pd.concat([resultados, problema], ignore_index=True)
        
        # Guardar nuevamente
        resultados.to_csv(csv_path, index=False)
        
        # Rehacer el merge ya con los nuevos registros
        merge = df.merge(resultados, how="left", on="nombreencontrado")
    
    return merge



