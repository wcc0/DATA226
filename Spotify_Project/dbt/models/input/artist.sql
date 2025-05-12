-- models/input/artist.sql

SELECT
    ARTIST_ID,
    NAME,
    ARTIST_POPULARITY,
    FOLLOWERS,
    GENRE_0,
    GENRE_1,
    GENRE_2,
    GENRE_3,
    GENRE_4
FROM {{ source('raw', 'artist') }}
WHERE NAME IS NOT NULL
