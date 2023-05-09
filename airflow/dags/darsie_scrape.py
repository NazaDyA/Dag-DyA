import requests
import pandas as pd
import json
import math
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

def scrape_result():

    headers = {
        'authority': 'www.darsie.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'es-AR,es;q=0.9,en;q=0.8',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.darsie.com',
        'referer': 'https://www.darsie.com/152-ladrillos-bloques-y-viguetas?page=2',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'ajax': '1',
    }

    data = {
        'params': 'id_manufacturer=0&id_supplier=0&page=3&nb_items=12&controller_product_ids=&current_controller=category&page_name=category&id_parent_cat=152&orderBy=sales&orderWay=desc&defaultSorting=sales%3Adesc&customer_groups=1&random_seed=22041810&layout=vertical&count_data=1&hide_zero_matches=1&dim_zero_matches=1&sf_position=0&include_group=0&compact=980&compact_offset=2&compact_btn=1&npp=12&default_order_by=sales&default_order_way=desc&random_upd=1&reload_action=1&p_type=3&autoscroll=0&combination_results=1&oos_behaviour_=0&oos_behaviour=0&combinations_stock=0&url_filters=1&url_sorting=1&url_page=1&dec_sep=.&tho_sep=&merged_attributes=0&merged_features=0&available_options%5Bc%5D%5B152%5D=162%2C167%2C151%2C225&available_options%5Bin_stock%5D%5B0%5D=1&listView=grid&sliders%5Bp%5D%5B0%5D%5Bfrom%5D=37&sliders%5Bp%5D%5B0%5D%5Bmin%5D=37.05&sliders%5Bp%5D%5B0%5D%5Bto%5D=6927&sliders%5Bp%5D%5B0%5D%5Bmax%5D=6926.02&page_from=1',
        'current_url': 'https://www.darsie.com/148-cemento-cales-y-adicionales?page=4',
        'trigger': 'af_page',
    }

    tiposMateriales = ['150-construcciãn']
    #,'184-instalaciones','216-pisos-y-revestimientos','93-baão','141-cocina','140-artãculos-del-hogar']

    datos = []
    
    for m in tiposMateriales:
        response = requests.get(f'https://www.darsie.com/{m}?page=1', headers=headers, params=params, data=data)
        result = json.loads(response.text)
        pages = math.ceil(int(result['pagination']['total_items'])/int(result['pagination']['items_shown_to']))
        for p in range(pages):
            response = requests.get(f'https://www.darsie.com/{m}?page={p}', headers=headers, params=params, data=data)
            for i in range(len(result['products'])):
                
                dato = {
                    'nombre':str(result['products'][i]['name']),
                    'precio':str(result['products'][i]['price_amount']),
                    'fecha_ext': datetime.now(),
                    'fuente': 'darsie'
                }
    
                datos.append(dato)
    
    
    return pd.DataFrame(datos)


def create_conn():
    
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/etl_process')
                           #postgresql+psycopg2://postgres:Jo39go525124@localhost/airflow')
    
    return engine.connect()


def update_data():
    
     df = scrape_result()
     conn = create_conn()
     
     df['fecha_ext'] = df['fecha_ext'].astype('str')
     #df = df.to_dict(orient='records')
     
     return df.to_sql('step_1',conn, schema='public',if_exists='replace',index=True)

def store_data():
    
    df = scrape_result()
    df['nombre'] = df['nombre'].astype(str)
    df['fecha_ext'] = df['fecha_ext'].astype(str)
    hook = PostgresHook(postgres_conn_id='postgres_db')
    
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS etl_db (
        indice BIGINT,
        nombre VARCHAR,
        precio FLOAT,
        fecha_ext VARCHAR,
        fuente VARCHAR );'''

    hook.run(sql=create_table_query)
    
    #conn = hook.get_conn()
    data = list(df.itertuples(index=True,name=None))
    hook.insert_rows(table='etl_db', rows=data)
    
    select_query = 'SELECT * FROM etl_db;'
    records = hook.get_records(sql=select_query)
    
    return print(records)



#hook.insert_rows(table='step_1', rows=df.to_dict(orient='records')),'todo ok store' #df.to_sql('step_1',conn, if_exists='replace',index=True)
