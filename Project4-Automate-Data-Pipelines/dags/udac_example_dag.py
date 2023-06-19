"""Defines main DAG, follows the data flow provided in the instructions."""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'duongaity',
    'start_date': datetime(2023, 6, 19),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    aws_region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    aws_region="us-west-2",
    extra_params="format as json 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    database_name="dev",
    table_name="public.songplays",
    table_fields="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    database_name="dev",
    table_name="public.users",
    table_fields="(userid, first_name, last_name, gender, level)",
    table_truncate=True,
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    database_name="dev",
    table_name="public.songs",
    table_fields="(songid, title, artistid, year, duration)",
    table_truncate=True,
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    database_name="dev",
    table_name="public.artists",
    table_fields="(artistid, name, location, lattitude, longitude)",
    table_truncate=True,
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    database_name="dev",
    table_name="public.time",
    table_fields="(start_time, hour, day, week, month, year, weekday)",
    table_truncate=True,
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.staging_events", "public.staging_songs", "public.songplays", "public.artists", "public.time", "public.songs", "public.users"]
)

end_operator = DummyOperator(
    task_id='Stop_execution', 
    dag=dag
)

start_operator >> [ stage_events_to_redshift, stage_songs_to_redshift ] >> load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks >> end_operator
