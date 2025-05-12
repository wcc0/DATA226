from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
import logging
import time
import ast
import snowflake.connector
import pandas as pd
# import base64
# # import json
import math
from requests import post, get
from spotify_data_manager import SpotifyDataManager
from spotify_data_fetcher import SpotifyDataFetcher
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

client_id = Variable.get("spotify_client_id")
client_secret = Variable.get("spotify_client_secret")
artist_list = ast.literal_eval(Variable.get("spotify_artist_list", default_var="['Taylor Swift']"))

# def get_snowflake_cursor():
#     return snowflake.connector.connect(
#         user=Variable.get("snowflake_userid"),
#         password=Variable.get("snowflake_password"),
#         account=Variable.get("snowflake_account"),
#         warehouse=Variable.get("snowflake_warehouse"),
#         database=Variable.get("snowflake_database"),
#         schema=Variable.get("snowflake_schema")
#     ).cursor()


def get_snowflake_cursor():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(
    snowflake_conn_id='snowflake_conn'
    )
    conn = hook.get_conn()
    return conn.cursor()

# def validate_records(data, required_fields):
#     for record in data:
#         for field in required_fields:
#             if field not in record or record[field] is None:
#                 raise ValueError(f"Missing required field: {field}")

def serialize_timestamps(records):
    for row in records:
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                row[k] = v.isoformat()
    return records


with DAG(
    dag_id="spotify_etl_snowflake_combined",
    start_date=datetime(2025, 5, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["spotify", "etl", "snowflake"]
) as dag:

    @task
    def wait_30_seconds():
        logging.info("Waiting 30 seconds to avoid rate limiting...")
        time.sleep(30)
        
    @task
    def get_album_data():
        manager = SpotifyDataManager(client_id, client_secret)
        fetcher = SpotifyDataFetcher(manager.get_token())
        album_ids = manager.get_album_ids(artist_list)
        raw_albums = manager.get_album_data(album_ids)
        df = manager.albums_df(raw_albums).drop_duplicates(subset=["track_id"])
        cleaned = fetcher.cleanup(df)
        records = cleaned.to_dict(orient="records")
        for r in records:
            r["ingestion_date"] = str(datetime.now(timezone.utc).date())
        records = serialize_timestamps(records)    
        logging.info(f"Fetched {len(records)} album records.")
        if records:
            logging.info(f"Sample album record: {records[0]}")

        return records  # still needed by other tasks


    @task
    def get_tracks_data(album_data):
        ids = [r["track_id"] for r in album_data]
        fetcher = SpotifyDataFetcher(SpotifyDataManager(client_id, client_secret).get_token())
        df = fetcher.tracks_df(ids)
        records = df.to_dict(orient="records")
        for r in records:
            r["ingestion_date"] = str(datetime.now(timezone.utc).date())
        records = serialize_timestamps(records)
        logging.info(f"Fetched {len(records)} artist records.")
        if records:
            logging.info(f"Sample artist record: {records[0]}")

        return records

    @task
    def get_artist_data(album_data):
        ids = list(set([r["artist_id"] for r in album_data]))
        fetcher = SpotifyDataFetcher(SpotifyDataManager(client_id, client_secret).get_token())
        df = fetcher.artists_df(ids)
        records = df.to_dict(orient="records")
        for r in records:
            r["ingestion_date"] = str(datetime.now(timezone.utc).date())
        records = serialize_timestamps(records)
        logging.info(f"Fetched {len(records)} track metadata records.")
        if records:
            logging.info(f"Sample track record: {records[0]}")

        return records

    @task
    def create_snowflake_tables():
        logging.info("Creating Snowflake tables (if not exist)...")
        cur = get_snowflake_cursor()

        table_defs = {
            "albums": [
                "TRACK_ID VARCHAR PRIMARY KEY",
                "TRACK_NAME VARCHAR",
                "TRACK_NUMBER INT",
                "DURATION_MS INT",
                "DURATION_SEC FLOAT",
                "ALBUM_ID VARCHAR",
                "ALBUM_NAME VARCHAR",
                "ALBUM_TYPE VARCHAR",
                "TOTAL_TRACKS INT",
                "RELEASE_DATE TIMESTAMP",
                "LABEL VARCHAR",
                "ALBUM_POPULARITY INT",
                "ARTIST_ID VARCHAR",
                "ARTIST_0 VARCHAR",
                "INGESTION_DATE DATE"
            ],
            "tracks": [
                "TRACK_ID VARCHAR PRIMARY KEY",
                "TRACK_POPULARITY INT",
                "EXPLICIT BOOLEAN",
                "INGESTION_DATE DATE"
            ],
            "artist": [
                "ARTIST_ID VARCHAR PRIMARY KEY",
                "NAME VARCHAR",
                "ARTIST_POPULARITY INT",
                "FOLLOWERS INT",
                "GENRE_0 VARCHAR",
                "GENRE_1 VARCHAR",
                "GENRE_2 VARCHAR",
                "GENRE_3 VARCHAR",
                "GENRE_4 VARCHAR",
                "INGESTION_DATE DATE"
            ]
        }

        for table, columns in table_defs.items():
            ddl = f"CREATE TABLE IF NOT EXISTS {table.upper()} ({', '.join(columns)});"
            logging.info(f"Creating table {table.upper()}...")
            cur.execute(ddl)

        cur.close()
        logging.info("Table creation complete.")

    
    @task
    def load_albums(album_data):
        cur = get_snowflake_cursor()
        cur.execute("BEGIN;")
        try:
            table = "albums"
            allowed = [
                "track_id", "track_name", "track_number", "duration_ms", "duration_sec",
                "album_id", "album_name", "album_type", "total_tracks", "release_date",
                "label", "album_popularity", "artist_id", "artist_0", "ingestion_date"
            ]

            cur.execute(f"DELETE FROM {table.upper()};")

            keys = [k.upper() for k in allowed]
            cols = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))

            values_list = [tuple(row.get(k) for k in allowed) for row in album_data]
            cur.executemany(f"INSERT INTO {table.upper()} ({cols}) VALUES ({placeholders});", values_list)

            cur.execute("COMMIT;")
            logging.info(f"Loaded {len(values_list)} album records.")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error("Failed to load albums", exc_info=True)
            raise e
        finally:
            cur.close()

    
    @task
    def load_tracks(track_data):
        cur = get_snowflake_cursor()
        cur.execute("BEGIN;")
        try:
            table = "tracks"
            allowed = ["track_id", "track_popularity", "explicit", "ingestion_date"]

            cur.execute(f"DELETE FROM {table.upper()};")

            # Patch: convert "id" to "track_id"
            for row in track_data:
                if "track_id" not in row and "id" in row:
                    row["track_id"] = row["id"]

            keys = [k.upper() for k in allowed]
            cols = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))

            values_list = [tuple(row.get(k) for k in allowed) for row in track_data]
            cur.executemany(f"INSERT INTO {table.upper()} ({cols}) VALUES ({placeholders});", values_list)

            cur.execute("COMMIT;")
            logging.info(f"Loaded {len(values_list)} track records.")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error("Failed to load tracks", exc_info=True)
            raise e
        finally:
            cur.close()
    
    @task
    def load_artists(artist_data):
        cur = get_snowflake_cursor()
        cur.execute("BEGIN;")
        try:
            table = "artist"
            allowed = [
                "artist_id", "name", "artist_popularity", "followers",
                "genre_0", "genre_1", "genre_2", "genre_3", "genre_4", "ingestion_date"
            ]

            cur.execute(f"DELETE FROM {table.upper()};")

            for row in artist_data:
                # Handle missing artist_id
                if "artist_id" not in row and "id" in row:
                    row["artist_id"] = row["id"]
                row.pop("id", None)

                # Replace NaNs with None
                for k in allowed:
                    val = row.get(k)
                    if isinstance(val, float) and math.isnan(val):
                        row[k] = None

            keys = [k.upper() for k in allowed]
            cols = ", ".join(keys)
            placeholders = ", ".join(["%s"] * len(keys))

            values_list = [tuple(row.get(k) for k in allowed) for row in artist_data]
            cur.executemany(f"INSERT INTO {table.upper()} ({cols}) VALUES ({placeholders});", values_list)

            cur.execute("COMMIT;")
            logging.info(f"Loaded {len(values_list)} artist records.")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error("Failed to load artists", exc_info=True)
            raise e
        finally:
            cur.close()
        

    # Task execution flow
    album_data = get_album_data()
    wait1 = wait_30_seconds()
    tracks = get_tracks_data(album_data)
    wait2 = wait_30_seconds()
    artists = get_artist_data(album_data)
    create = create_snowflake_tables()
    load_albums_task = load_albums(album_data)
    load_tracks_task = load_tracks(tracks)
    load_artists_task = load_artists(artists)

    # DAG flow
    album_data >> wait1 >> tracks >> wait2 >> artists >> create
    create >> [load_albums_task, load_tracks_task, load_artists_task]
