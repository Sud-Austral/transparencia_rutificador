import pandas as pd

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

def get_name(): 
        

if __name__ == '__main__':
    #https://github.com/Sud-Austral/BASE_COMUNAS_TRANSPARENCIA/raw/main/comunas/Corporaci%C3%B3n%20Municipal%20de%20Providencia.csv
    for comuna in comunas:
        print(comuna)
        pd.read_csv(f"test/{comuna}.csv",compression='xz', sep='\t').to_excel(f"organismoSalida2/{comuna}.xlsx", index=False)
        # Eliminar la variable que contiene los datos grandes para liberar memoria
        #print(url)
        #print(df2)
