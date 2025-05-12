import logging
import time
import pandas as pd
import base64
import json
import requests
from requests import get

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