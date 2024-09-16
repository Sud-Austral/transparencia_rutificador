import pandas as pd
import datetime
import numpy as np
# Establecer el locale en español para que reconozca el nombre del mes

meses = {
    'Enero': 1,
    'Febrero': 2,
    'Marzo': 3,
    'Abril': 4,
    'Mayo': 5,
    'Junio': 6,
    'Julio': 7,
    'Agosto': 8,
    'Septiembre': 9,
    'Octubre': 10,
    'Noviembre': 11,
    'Diciembre': 12
}

def get_fecha(fila):
    try:
        mes_numero = meses.get(fila["Mes"], 1)  # Si el mes no está en el diccionario, usar Enero (1)
        fecha_string = f'{fila["anyo"]}-{mes_numero:02d}-01'  
        fecha_dt = datetime.datetime.strptime(fecha_string, '%Y-%m-%d')
    except:
        fecha_dt = datetime.datetime.strptime("2099-01-01", '%Y-%m-%d')
    return fecha_dt

def encontrar_nuevos(df,fecha):
    fechaMinima = datetime.datetime.strptime(fecha, '%Y-%m')
    df["Fecha"] = df.apply(get_fecha, axis=1)
    min_dates = df.groupby("rut")["Fecha"].min().reset_index()
    min_dates[f"Marca_{fecha}"] = np.where(min_dates["Fecha"] <= fechaMinima, "Antes", "Después")
    del min_dates["Fecha"]
    #min_dates = min_dates.rename(columns={'Fecha': f'Fecha-{fecha}'})
    del df["Fecha"]
    df2 = df.merge(min_dates)
    return df2


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
 'Municipalidad de Ñuñoa',
 'Municipalidad de Quilpué',
 'Municipalidad de Villa Alemana']

comuna2 = ['Municipalidad de Maipú','Municipalidad de Santiago','Municipalidad de Quilpué','Municipalidad de Villa Alemana']

if __name__ == '__main__':
    #https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/Corporaci%C3%B3n%20Municipal%20de%20Providencia.csv
    for comuna in comunas:
        print(comuna)
        pd.read_csv(f"test/{comuna}.csv",compression='xz', sep='\t').assign(metodo="").to_excel(f"organismoSalida2/{comuna}.xlsx", index=False)
        # Eliminar la variable que contiene los datos grandes para liberar memoria
        #print(url)
        #print(df2)
    print("Primer cierre")
    
    for comuna in comunas:
        df = pd.read_excel(f"organismoSalida2/{comuna}.xlsx")
        df2 = encontrar_nuevos(df,"2021-07")
        df2.to_excel(f"personalNuevo/{comuna}.xlsx", index=False)
    
