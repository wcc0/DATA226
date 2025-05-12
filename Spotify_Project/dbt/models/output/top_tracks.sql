-- models/output/top_tracks.sql
SELECT
    a.track_id,
    a.track_name,
    a.album_name,
    t.track_popularity,
    a.release_date,
    a.artist_id,
    a.artist_0,
FROM {{ ref('albums') }} a
JOIN {{ ref('tracks') }} t ON a.track_id = t.track_id
WHERE t.track_popularity >= 80
ORDER BY t.track_popularity DESC