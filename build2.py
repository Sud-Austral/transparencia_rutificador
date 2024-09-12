import pandas as pd
import datetime
import locale
import numpy as np
# Establecer el locale en español para que reconozca el nombre del mes
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

def get_fecha(fila):
    try:
        fecha_string = f'{fila["anyo"]}-{fila["Mes"]}'
        fecha_dt = datetime.datetime.strptime(fecha_string, '%Y-%B')
    except:
        fecha_dt = datetime.datetime.strptime("2099-Enero", '%Y-%B')
    return fecha_dt

def encontrar_nuevos(df,fecha):
    fechaMinima = datetime.datetime.strptime(fecha, '%Y-%B')
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
    for comuna in comunas2:
        df = pd.read_excel(f"organismoSalida2/{comuna}.xlsx", index=False)
        df2 = encontrar_nuevos(df,"2021-Julio")
        df2.to_excel(f"personalNuevo/{comuna}.xlsx", index=False)

