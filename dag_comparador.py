import pandas as pd
import json
import os
import gc
import tempfile
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage



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


    suma_monto = df1_diff['MONTO'].sum() if 'MONTO' in df1_diff.columns else 0
    suma_abono = df2_diff['NumAbono'].sum() if "NumAbono" in df2_diff.columns else 0

    #Archivo temporal
    temp_file = temp_file.NamedTemporaryFile(delete = False, suffix = ".xlsx")
    temp_path = temp_file.name
    temp_file.close()


    with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
        for df, name in zip([df_matches, df1_diff, df2_diff], ['coincidencias', 'a favor', 'en contra']):
            df.to_excel(writer, sheet_name=name, index=False, startrow=1, header=False)
            workbook  = writer.book
            worksheet = writer.sheets[name]
            (max_row, max_col) = df.shape
            column_settings = [{'header': column} for column in df.columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': column_settings,
                'name': f'Tabla_{name.replace(" ", "_")}',
                'style': 'Table Style Medium 9'
            })
        


        if 'a favor' in writer.sheets:
            sheet = writer.sheets['a favor']
            sheet.write(max_row + 2, 0, 'Suma MONTO')
            sheet.write(max_row + 2, 1, suma_monto)

        if 'en contra' in writer.sheets:
            sheet = writer.sheets['en contra']
            sheet.write(max_row + 2, 0, 'Suma NumAbono')
            sheet.write(max_row + 2, 1, suma_abono)

    print(f"Archivo generado en: {temp_path}")



    #Subimos a GCS
    gcs_hook = GCSHook(gcp_conn_id = config['gcp_conn_id'])
    bucket_name = config['bucket_name']

    
    file_date = today.strftime("%Y%m%d")
    gcs_filename = f'Comparacion_{file_date}.xlsx'



    gcs_hook.upload(
        bucket_name = bucket_name,
        object_name = gcs_filename,
        filename = temp_path
    )
    print(f'Archivo subido a GCS: gs://{bucket_name}/{gcs_filename}')


    #Eliminamos archivo temporal
    if os.path.exists(temp_path):
        os.remove(temp_path)
        print(f"Archivo temporal eliminado: {temp_path}")
    else:
        print(f"Archivo temporal no encontrado para eliminar: {temp_path}")
    
    gc.collect()



    return {
        'archivo_gcs': f"gs://{bucket_name}/{gcs_filename}",
        'coincidencias': len(df_matches),
        'a_favor': len(df1_diff),
        'en_contra': len(df2_diff),
        'suma_monto': float(suma_monto),
        'suma_abono': float(suma_abono)
    }
'''
    return {
        'archivo_gcs': f"gs://{bucket_name}/{gcs_filename}",
        'coincidencias': df_matches.to_dict(orient='records'),
        'diferencias_tabla1': df1_diff.to_dict(orient='records'),
        'diferencias_tabla2': df2_diff.to_dict(orient='records')
    }
'''