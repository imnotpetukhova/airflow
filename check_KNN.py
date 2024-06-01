from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id='check_KNN', start_date=datetime(2021, 1, 1), schedule='@once')
def new_dag():
    import pandas as pd
    import logging

    @task(task_id='python_postgres', retries=2)
    def postgres_callable():
        from sqlalchemy import create_engine

        engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Temps')
        with engine.connect() as conn:
            df = pd.read_sql('select actor_id, release_year, title, language_id, rental_duration, length, rental_rate, rating, special_features FROM film_actor JOIN movie ON film_actor.film_id = movie.film_id', con=conn.connection)
            conn.close()
        
        return df

    @task(task_id='python_mongo', retries=2)
    def mongo_callable():
        from pymongo import MongoClient

        m_client = MongoClient()
        coll = m_client.test_db.actor
        df = pd.DataFrame(list(coll.find()))
        df.drop('_id', axis=1, inplace=True)
        
        return df
    
    @task.branch
    def concat_result_callable(postgres_df, mongo_df):
        from sklearn.neighbors import KNeighborsClassifier
        from sklearn.preprocessing import LabelEncoder

        marge_df = mongo_df.join(postgres_df.set_index('actor_id'), on='actor_id', validate='1:m')
        y = marge_df['rating'].dropna()
        x = marge_df.loc[:, marge_df.columns != 'rating'].dropna()
        x.drop('last_update', axis=1, inplace=True)

        f_name_encoder = LabelEncoder()
        l_name_encoder = LabelEncoder()
        title_encoder = LabelEncoder()
        features_encoder = LabelEncoder()

        x['first_name'] = f_name_encoder.fit_transform(x['first_name'])
        x['last_name'] = l_name_encoder.fit_transform(x['last_name'])
        x['title'] = title_encoder.fit_transform(x['title'])
        x['special_features'] = features_encoder.fit_transform(x['special_features'])

        neigh = KNeighborsClassifier(n_neighbors=3)
        neigh.fit(x.head(15), y.head(15))

        logging.info(neigh.predict(x.tail(4)))

    postgres_task = postgres_callable()
    mongo_task = mongo_callable()
    result_task = concat_result_callable(postgres_task, mongo_task)
    
    result_task

new_dag()