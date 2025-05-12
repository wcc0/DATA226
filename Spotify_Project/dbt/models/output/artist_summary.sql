-- models/output/artist_summary.sql
SELECT
    artist_id,
    name AS artist_name,
    followers,
    artist_popularity,
    genre_0,
    genre_1,
FROM {{ ref('artist') }}
WHERE followers > 10000
ORDER BY followers DESC