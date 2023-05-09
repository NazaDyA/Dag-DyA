from sqlalchemy import create_engine


def load_data():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/etl_process')
    engine.execute('INSERT INTO step_3 SELECT * FROM step_2')
    return print('todo ok pa')

