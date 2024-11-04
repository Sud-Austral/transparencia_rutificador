import pandas as pd
from datetime import datetime 
from dateutil.relativedelta import relativedelta
import traceback

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

def get_meses(fila):
    inicial = fila["fecha_min"]
    final = fila["fecha_max"]
    diferencia = relativedelta(final, inicial)
    meses_totales = diferencia.years * 12 + diferencia.months
    return meses_totales
    

def get_df_final(df):
    salida = df[['rut','NombreCompleto_x',"remuneracionbruta_mensual","remuliquida_mensual","base",'Homologado',
         'Homologado 2','Tipo cargo','tipo_calificacionp']].groupby('rut').agg(
        nombre = ('NombreCompleto_x', 'first'),
        remuneracionbruta_final = ('remuneracionbruta_mensual', 'first'),
        remuliquida_final = ('remuliquida_mensual', 'first'),
        base_final = ('base', 'first'),
        homologado = ('Homologado', 'first'), 
        homologado_2 = ('Homologado 2', 'first'),
        tipo_cargo = ('Tipo cargo', 'first'),
        tipo_calificacionp = ('tipo_calificacionp', 'first'),
        pago_mes = ('rut', 'count'),
    ).reset_index()
    return salida

def get_df_fecha(df):
    salida = df.groupby(by=['rut']).agg(
        fecha_max = ('Fecha', 'max'),
        fecha_min = ('Fecha', 'min'),
    ).reset_index()
    return salida

def get_df_min(df,df_filtro2):
    merge = df[["rut","fecha_min"]].merge(df_filtro2[["rut","base",'remuneracionbruta_mensual', 'remuliquida_mensual',"Fecha"]],
                                left_on=["rut","fecha_min"],right_on=["rut","Fecha"])
    salida = merge[['rut', 'base', 'remuneracionbruta_mensual',
           'remuliquida_mensual']].groupby("rut").agg(
            base_inicial = ('base', 'first'),
            remuneracionbruta_inicial = ('remuneracionbruta_mensual', 'first'),
            remuliquida_inicial = ('remuliquida_mensual', 'first'),
           ).reset_index()
    return salida
    
    

def get_historial_persona(df,fecha,save_dataframe_general,conn_params):
    try:
        fecha_referencia = datetime.strptime(fecha, '%Y-%m')
        df["Fecha"] = df.apply(get_fecha2, axis=1)
        df2 = df[df["Fecha"] == fecha_referencia].sort_values('remuneracionbruta_mensual', ascending=False)
        if df2.empty:
            return None
        df_final = get_df_final(df2)
        rut = df2['rut'].unique()
        df_filtro1 = df[df['rut'].apply(lambda x: x in rut)].sort_values('remuneracionbruta_mensual')
        df_filtro2 = df_filtro1[df_filtro1["Fecha"] <= fecha_referencia].sort_values("remuneracionbruta_mensual")
        df_fecha = get_df_fecha(df_filtro2)
        df_inicial =  get_df_min(df_fecha,df_filtro2)
        salida = df_final.merge(df_fecha).merge(df_inicial)
        salida["meses"] = salida.apply(get_meses, axis=1)
        salida["organismo"] = df.iloc[0].organismo_nombre
        save_dataframe_general(salida,"resumen_personal_2024_6",conn_params)
        del df["Fecha"]
        return salida
    except Exception as e:
        print(f"Error al procesar: {e}")
        error_traceback = traceback.format_exc()
        print("Traceback detallado:")
        print(error_traceback)
        return None