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
import base64
import json
import math
import requests
from requests import post, get

class SpotifyDataManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        auth_string = self.client_id + ":" + self.client_secret
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {"Authorization": "Basic " + auth_base64, "Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}

        result = requests.post(url, headers=headers, data=data)
        if result.status_code != 200:
            raise ValueError(f"Invalid response code: {result.status_code} -- Reason: {result.reason}")

        json_result = json.loads(result.content)
        return json_result["access_token"]

    def get_auth_header(self, token):
        if not token:
            raise ValueError("Token is None or empty")
        return {"Authorization": "Bearer " + token}
    
    def get_album_ids(self, artist_names):
        url = "https://api.spotify.com/v1/search?"
        token = self.get_token()
        headers = self.get_auth_header(token)
        ids = []

        for artist in artist_names:
            logging.info(f"Fetching album IDs for artist: {artist}")
            offset = 0
            total = 1000
            while offset < total:
                query = f"q={artist}&type=album&limit=50&market=US&offset={offset}"
                query_url = url + query
                result = get(query_url, headers=headers)
                if result.status_code != 200:
                    print(f"Request failed with status code {result.status_code}, Reason: {result.reason}")
                    break
                content = json.loads(result.content)["albums"]
                if "items" in content:
                    items = content["items"]
                    ids.extend([item["id"] for item in items])
                offset += 50
        return ids

    def get_album_data(self, album_ids):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        token = self.get_token()
        headers = self.get_auth_header(token)
        all_content = []

        for chunk in chunks(album_ids, 20):
            chunk_ids = ",".join(chunk)
            url = f"https://api.spotify.com/v1/albums?ids={chunk_ids}&market=us"

            result = get(url, headers=headers)

            # Handle rate limit (429) response
            if result.status_code == 429:
                retry_after = int(result.headers.get("Retry-After", 5))
                logging.warning(f"Rate limit hit while fetching albums. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                result = get(url, headers=headers)  # Retry once

            if result.status_code != 200:
                raise ValueError(f"Invalid response code: {result.status_code}, Reason: {result.reason}")

            content = json.loads(result.content)
            all_content.append(content)
            time.sleep(0.1)  # gentle pacing between calls

        return all_content

    def albums_df(self, data):
        track_objects = []
        for data_item in data:
            for album in data_item.get("albums", []):
                album_info = {
                    "album_type": album.get("album_type", ""),
                    "total_tracks": album.get("total_tracks", 0),
                    "album_name": album.get("name", ""),
                    "release_date": album.get("release_date", ""),
                    "label": album.get("label", ""),
                    "album_popularity": album.get("popularity", 0),
                    "album_id": album.get("id", ""),
                }
                for artist in album.get("artists", []):
                    artist_info = {"artist_id": artist.get("id", "")}
                    for item in album.get("tracks", {}).get("items", []):
                        track_info = {
                            "track_name": item.get("name", ""),
                            "track_id": item.get("id", ""),
                            "track_number": item.get("track_number", 0),
                            "duration_ms": item.get("duration_ms", 0),
                            "artists": [a["name"] for a in item.get("artists", [])]
                        }
                        track_info.update(album_info)
                        track_info.update(artist_info)
                        track_objects.append(track_info)
        return pd.DataFrame(track_objects)


class SpotifyDataFetcher:
    """
    SpotifyDataFetcher class to retrieve and process track, artist, and audio feature data from Spotify.
    Handles API rate limiting (429s), request retries, and chunking.
    """

    def __init__(self, token):
        self.token = token

    def get_auth_header(self):
        if not self.token:
            raise ValueError("Token is None or empty")
        return {"Authorization": f"Bearer {self.token}"}

    def chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def _safe_get(self, url, headers):
        """
        Makes a GET request and handles Spotify rate limiting (429 errors).
        """
        result = get(url, headers=headers)

        if result.status_code == 429:
            retry_after = int(result.headers.get("Retry-After", 5))
            logging.warning(f"Rate limit hit. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            result = get(url, headers=headers)  # Retry once

        if result.status_code != 200:
            raise ValueError(f"Spotify API error {result.status_code}: {result.reason} | URL: {url}")

        return result
    
    def tracks_df(self, track_ids):
        headers = self.get_auth_header()
        data = []

        for chunk in self.chunks(track_ids, 50):
            chunk_ids = ",".join(chunk)
            url = f"https://api.spotify.com/v1/tracks?ids={chunk_ids}"
            result = self._safe_get(url, headers)

            content = json.loads(result.content)["tracks"]
            for track_content in content:
                if isinstance(track_content, dict):
                    features = {
                        "id": track_content.get("id", ""),
                        "track_popularity": track_content.get("popularity", 0),
                        "explicit": track_content.get("explicit", False),
                    }
                    data.append(features)
            time.sleep(0.1)

        return pd.DataFrame(data)

    def artists_df(self, artist_ids):
        headers = self.get_auth_header()
        data = []

        for chunk in self.chunks(artist_ids, 25):
            chunk_ids = ",".join(chunk)
            url = f"https://api.spotify.com/v1/artists?ids={chunk_ids}"
            result = self._safe_get(url, headers)

            content = json.loads(result.content)
            for artist_content in content.get("artists", []):
                if artist_content is None:
                    continue

                artist_data = {
                    "id": artist_content.get("id", ""),
                    "name": artist_content.get("name", ""),
                    "artist_popularity": artist_content.get("popularity", 0),
                    "artist_genres": artist_content.get("genres", []),
                    "followers": artist_content.get("followers", {}).get("total", 0),
                }
                data.append(artist_data)
            time.sleep(0.1)

        df = pd.DataFrame(data)

        # Split genres into separate columns
        df_genres = df["artist_genres"].apply(pd.Series)
        df_genres = df_genres.rename(columns=lambda x: f"genre_{x}")
        return pd.concat([df, df_genres], axis=1)

    def cleanup(self, df):
        df["artist_id"] = (
            df["artist_id"].astype(str).str.replace("(", "").str.replace(")", "").str.replace("'", "")
        )

        # Expand list of artists
        df_artist = df["artists"].apply(pd.Series).rename(columns=lambda x: f"artist_{x}")
        df = pd.concat([df, df_artist], axis=1)

        df["duration_sec"] = df["duration_ms"] / 1000
        df["track_name"] = df["track_name"].str.title()
        df["album_name"] = df["album_name"].str.title()
        df["release_date"] = pd.to_datetime(df["release_date"], format="mixed", errors="coerce")

        return df


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

client_id = Variable.get("spotify_client_id")
client_secret = Variable.get("spotify_client_secret")
artist_list = ast.literal_eval(Variable.get("spotify_artist_list", default_var="['Taylor Swift']"))

def get_snowflake_cursor():
    return snowflake.connector.connect(
        user=Variable.get("snowflake_userid"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema=Variable.get("snowflake_schema")
    ).cursor()

def validate_records(data, required_fields):
    for record in data:
        for field in required_fields:
            if field not in record or record[field] is None:
                raise ValueError(f"Missing required field: {field}")

def serialize_timestamps(records):
    for row in records:
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                row[k] = v.isoformat()
    return records


with DAG(
    dag_id="spotify_etl_snowflake_combined",
    start_date=days_ago(1),
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
                "TRACK_ID VARCHAR",
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
                "TRACK_ID VARCHAR",
                "TRACK_POPULARITY INT",
                "EXPLICIT BOOLEAN",
                "INGESTION_DATE DATE"
            ],
            "artist": [
                "ARTIST_ID VARCHAR",
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

            # 🔁 Patch: convert "id" to "track_id"
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
