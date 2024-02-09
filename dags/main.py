import datetime
import pendulum
import os
from airflow.decorators import task,dag
from load_movie_data import  get_video_url
from kafka_job_stream import stream_to_kafka
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from spark_job_stream import read_stream_from_kafka_write_to_dynamodb


@dag(
    dag_id="process-movies",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_movie():
    
    @task  
    def get_trending_movies():
        import requests
        url = "https://api.themoviedb.org/3/trending/movie/day?language=en-US"

        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIwYmI5NTE3ZWY3YTgwOTU1MzFhYjVhZGU3YzkzMmU3NSIsInN1YiI6IjYyNTAzMmMxYjZjMjY0MTA1OGJmMjg3NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.DEqtb_abwcVJf25GENMt7GJykqi8pXkZRbEk3lWLnUk"
        }

        movie_response = requests.get(url, headers=headers).json()['results']
        movies = [dict({'adult': movie['adult'],
                    'backdrop_path':  movie['backdrop_path'],
                    'genre_ids':  movie['genre_ids'],
                    'id':  movie['id'],
                    'media_type':  movie['media_type'],
                    'original_language':  movie['original_language'],
                    'original_title':  movie['original_title'],
                    'overview':  movie['overview'],
                    'popularity':  movie['popularity'],
                    'poster_path':  movie['poster_path'],
                    'release_date':  movie['release_date'],
                    'title':  movie['title'],
                    'video': get_video_url(movie['id']),
                    'vote_average': movie['vote_average'],
                    'vote_count': movie['vote_count']}) for movie in movie_response]
        return movies
     
     
    
    movie_data = get_trending_movies()
    
    stream_data_to_kafka = PythonOperator(
        task_id="stream_data_to_kafka",
        python_callable=stream_to_kafka,
        op_kwargs={'topic':'latest_trending_movie','movies':movie_data}
    )
    
    stream_data_from_spark = PythonOperator(
        task_id="stream_from_kafka_write_to_dynamodb",
        python_callable=read_stream_from_kafka_write_to_dynamodb
    )
    
    movie_data >> stream_data_to_kafka >> stream_data_from_spark
    
    
    
    


dag = process_movie()
    