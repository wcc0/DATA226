-- models/output/album_overview.sql
SELECT
    album_id,
    album_name,
    artist_id,
    artist_0,
    COUNT(DISTINCT track_id) AS track_count,
    AVG(duration_sec) AS avg_track_length_sec,
    MIN(release_date) AS album_release_date
FROM {{ ref('albums') }}
GROUP BY album_id, album_name, artist_id, artist_0
ORDER BY track_count DESC