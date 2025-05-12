-- models/input/tracks.sql

SELECT
  TRACK_ID,
  TRACK_POPULARITY,
  EXPLICIT
FROM {{ source('raw', 'tracks') }}

