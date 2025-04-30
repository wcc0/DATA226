from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import snowflake.connector

# Set up Spotify API credentials
def spotify_auth():
    return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=Variable.get("spotify_client_id"),
        client_secret=Variable.get("spotify_client_secret"),
        # redirect_uri="localhost:8081",
        # scope="user-top-read"
    ))

# Set up Snowflake connection
def return_snowflake_conn():
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_userid"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse="bluejay_query_wh",
        database="user_db_bluejay",
        schema="raw"
    )
    return conn.cursor()

# Extract Spotify Data
# Extract tracks from a public playlist (e.g., Top 50 Global)
@task
def extract_playlist_tracks():
    sp = spotify_auth()
    playlist_id = "37i9dQZEVXbMDoHDwVN2tF"  # Top 50 Global
    results = sp.playlist_tracks(playlist_id, limit=50)

    track_data = []
    for item in results['items']:
        track = item['track']
        track_data.append({
            'id': track['id'],
            'track_name': track['name'],
            'artist_name': track['artists'][0]['name']
        })

    return track_data

# Enrich tracks with audio features (danceability, energy, etc.)
@task
def extract_audio_features(track_data):
    sp = spotify_auth()
    track_ids = [track['id'] for track in track_data]
    features_list = sp.audio_features(tracks=track_ids)

    enriched = []
    for track, features in zip(track_data, features_list):
        if features:  # Skip nulls
            enriched.append({
                'track_name': track['track_name'],
                'artist_name': track['artist_name'],
                'danceability': features['danceability'],
                'energy': features['energy'],
                'valence': features['valence'],
                'tempo': features['tempo'],
                'popularity': features.get('popularity', 0)  # Fallback
            })
    return enriched

# Load into Snowflake
@task
def load_to_snowflake(records):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS spotify_tracks (
                track_name VARCHAR,
                artist_name VARCHAR,
                danceability FLOAT,
                energy FLOAT,
                valence FLOAT,
                tempo FLOAT,
                popularity INT
            );
        """)
        cur.execute("DELETE FROM spotify_tracks;")
        for r in records:
            cur.execute("""
                INSERT INTO spotify_tracks (track_name, artist_name, danceability, energy, valence, tempo, popularity)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (r['track_name'], r['artist_name'], r['danceability'], r['energy'], r['valence'], r['tempo'], r['popularity']))
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e

# Define the DAG
with DAG(
    dag_id="spotify_tracks",
    start_date=datetime(2025, 4, 28),
    catchup=False,
    schedule="0 2 * * *",
    tags=["spotify", "etl", "tracks"]
) as dag:

    track_data = extract_playlist_tracks()
    enriched_data = extract_audio_features(track_data)
    load = load_to_snowflake(enriched_data)

    track_data >> enriched_data >> load