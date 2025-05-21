import pandas as pd
import json
import gc
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def cargar_config(path: str):
    with open(path, 'r') as file:
        return json.load(file)

@task()
def comparar_referencias(config_path: str):
    config = cargar_config(config_path)

    #Extraer parámetros
    '''
    fecha_inicio = config['fecha_inicio']
    fecha_fin = config['fecha_fin']
    '''
    today = datetime.today()
    fecha_inicio = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_fin = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=997000)



    campo1 = config['campo_comparacion_source']
    campo2 = config['campo_comparacion_target']

    #Conexiones
    source_hook = MsSqlHook(mssql_conn_id=config['source_conn_id'])
    target_hook = MsSqlHook(mssql_conn_id=config['target_conn_id'])

    #Queries
    #query1 = config['query_source'].format(fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
    #query2 = config['query_target'].format(fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)

    query1 = config['query_source'].format(
        fecha_inicio=fecha_inicio.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        fecha_fin=fecha_fin.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )
    query2 = config['query_target'].format(
        fecha_inicio=fecha_inicio.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        fecha_fin=fecha_fin.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )

    #Ejecutar queries
    df1 = source_hook.get_pandas_df(query1)
    df2 = target_hook.get_pandas_df(query2)  # <- corregido: antes tenías "pangas_df"

    #Normalizar campos
    df1[campo1] = pd.to_numeric(df1[campo1], errors='coerce').astype('Int64')
    df2[campo2] = pd.to_numeric(df2[campo2], errors='coerce').astype('Int64')

    #Comparación
    df_matches = df1[df1[campo1].isin(df2[campo2])]
    df1_diff = df1[~df1[campo1].isin(df2[campo2])]
    df2_diff = df2[~df2[campo2].isin(df1[campo1])]

    #Resultados
    print(f'Coincidencias: {len(df_matches)}')
    print(f'Diferencias en tabla 1: {len(df1_diff)}')
    print(f'Diferencias en tabla 2: {len(df2_diff)}')

    gc.collect()

    return {
        'coincidencias': df_matches.to_dict(orient='records'),
        'diferencias_tabla1': df1_diff.to_dict(orient='records'),
        'diferencias_tabla2': df2_diff.to_dict(orient='records'),
    }
