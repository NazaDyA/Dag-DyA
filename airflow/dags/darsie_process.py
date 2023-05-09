import pandas as pd
import re
import numpy as np

#from scipy.stats import gmean

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
import pandas as pd

from stopwords import del_stopwords


def get_values():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/etl_process')
                           #postgresql://postgres:Jo39go525124@localhost/airflow')
    return pd.read_sql_query("SELECT * FROM step_1",engine)

def transform_data():

    datos = get_values()
    datos['precio'] = datos['precio'].astype(float)
    items = datos['nombre']
    items = [i.lower() for i in items]

    items = [del_stopwords(i) for i in items]

    items = [re.sub('(cm|Cm|CM)','cm',i) for i in items]
    items = [re.sub('(mts|Mts|Metros|Metro|Mt)','mt',i) for i in items]
    items = [re.sub('(lts|Lts|Lt)','lt',i) for i in items]
    items = [re.sub('(kg|Kg|Kgs)','kg',i) for i in items]
    items = [re.sub('\s(?=x)(?<=\D)|(?<=x)\s(?=\d)','',i) for i in items]

    items = [i.lower() for i in items]
    items= [re.sub('\.|,','',i) for i in items]
    items= [re.sub('1/2|dot|v/po|cba|cuyo|sikadur-31|v/p|superboard','',i) for i in items]
    items= [re.sub('e(?<=e)\d{1,3}','',i) for i in items]
    items= [re.sub('latex|látex|proclassic','pintura latex',i) for i in items]
    items= [re.sub('brikol |brikol|weber','pintura',i) for i in items]
    items= [re.sub('lajamax','microcemento',i) for i in items]
    items= [re.sub('minwax','revestimiento',i) for i in items]
    items= [re.sub('m/mando lavapl vert gourmet','lavaplato vertical',i) for i in items]
    items= [re.sub('tec(?=\s)|impermeabi|antigoteras|techos|sikamur|impermeab.duralba','impermeabilizante',i) for i in items]
    items= [re.sub('piso|pisos','pintura',i) for i in items]
    items= [re.sub('aislatec','aislante',i) for i in items]
    items= [re.sub('klaukol','adhesivo',i) for i in items]
    items= [re.sub('ladrillos','vigueta',i) for i in items]
    items= [re.sub('griferia','',i) for i in items]

    items = [re.sub('\(','',i) for i in items]


    items = [re.sub('(?<=x)(?=\d)',' ',i) for i in items]
    items = [re.sub('(?<=\d)(?=kg|cm|mt|mm)',' ',i) for i in items]

    items = [re.sub('\A\s','',i) for i in items]


    uMed = [re.findall('kg|cm|mt|mm',i) for i in items]
    uMedida = []

    for i in uMed:
        try:
            uMedida.append(i[0])
        except:
            uMedida.append('unidad')
            
    med = [re.findall('((?<=\s)\d+(?=kg|cm|mm|mt))|(\d+(?=kg|mt|cm|mm))|(\d+(?=\skg))',i) for i in items]
    med = pd.DataFrame(med)
    med[0] = [i[-1] if i is not None else "noValue" for i in med[0]]

    categ = [re.findall('(^[\w\-]+)',i) for i in items]
    categoria = []

    for i in categ:
        try:
            categoria.append(i[0])
        except:
            categoria.append('revisar')
            
    tabla = pd.DataFrame([categoria,
                          items,
                          uMedida,
                          med[0],
                          datos['precio'],
                          datos['fecha_ext'],
                          datos['fuente']]).T.set_axis(['categoria','items','unidadMedida','medida','precio','fecha_ext','fuente'],axis=1)
    
    tabla['precio'] = tabla['precio'].astype(float)
    pivot = pd.pivot_table(tabla, values='precio',index=['categoria','unidadMedida','medida'],aggfunc={'precio':[max, np.mean,min]})
    return tabla

def upload_data_temp():
    try:
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/etl_process')
        df = transform_data()
        return df.to_sql('step_2',engine, if_exists='replace')
    except:
        print('err')