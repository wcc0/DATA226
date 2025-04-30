from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import snowflake.connector

# Set up Spotify API credentials
def spotify_auth():
    return spotipy.Spotify(auth_manager=spotipy.oauth2.SpotifyClientCredentials(
        client_id=Variable.get("spotify_client_id"),
        client_secret=Variable.get("spotify_client_secret")
    ))


# Set up Snowflake connection
def return_snowflake_conn():
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_userid"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse="BLUEJAY_QUERY_WH",
        database="USER_DB_BLUEJAY",
        schema="raw"
    )
    return conn.cursor()

# Extract Spotify Data
@task
def extract_spotify_data():
    sp = spotify_auth()
    artist_name = Variable.get("spotify_artist", default_var="Taylor Swift")

    # Search for artist
    results = sp.search(q=f"artist:{artist_name}", type="artist", limit=1)
    if not results["artists"]["items"]:
        raise Exception(f"Artist '{artist_name}' not found on Spotify.")
    artist_id = results["artists"]["items"][0]["id"]

    # Get all albums and singles
    albums = []
    seen_album_names = set()
    results = sp.artist_albums(artist_id, album_type="album,single", limit=50)
    albums.extend(results["items"])
    while results["next"]:
        results = sp.next(results)
        albums.extend(results["items"])

    # Remove duplicate albums
    unique_albums = {album['name']: album for album in albums}.values()

    # Extract track info
    records = []
    for album in unique_albums:
        release_date = album.get("release_date")
        album_tracks = sp.album_tracks(album["id"])["items"]
        for track in album_tracks:
            track_info = sp.track(track["id"])
            records.append({
                "track_name": track_info["name"].replace("'", "''"),
                "artist_name": track_info["artists"][0]["name"].replace("'", "''"),
                "popularity": track_info["popularity"],
                "release_date": release_date,
                "duration_ms": track_info["duration_ms"]
            })
    return records


# Load data into Snowflake
@task
def load_to_snowflake(records):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS top_tracks (
                track_name VARCHAR,
                artist_name VARCHAR,
                popularity INT,
                release_date DATE,
                duration_ms INT
            );
        """)
        cur.execute("DELETE FROM top_tracks;")

        for record in records:
            sql = f"""
                INSERT INTO top_tracks (track_name, artist_name, popularity, release_date, duration_ms)
                VALUES ('{record["track_name"]}', '{record["artist_name"]}', {record["popularity"]},
                        '{record["release_date"]}', {record["duration_ms"]});
            """
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


# Define the DAG
with DAG(
    dag_id="spotify_popularity_prediction",
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule="0 2 * * *",
    tags=["spotify", "etl"]
) as dag:
    spotify_data = extract_spotify_data()
    load_to_snowflake(spotify_data)