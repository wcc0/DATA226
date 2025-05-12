import logging
import time
import pandas as pd
import json
from requests import get

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
