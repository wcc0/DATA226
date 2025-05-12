SELECT
    COALESCE(ar.GENRE_0, 'Unknown') AS genre,
    COUNT(*) AS total_tracks,
    SUM(CASE WHEN t.EXPLICIT = TRUE THEN 1 ELSE 0 END) AS explicit_tracks,
    ROUND(100.0 * SUM(CASE WHEN t.EXPLICIT = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS explicit_percentage
FROM {{ ref('tracks') }} t
JOIN {{ ref('albums') }} a ON t.TRACK_ID = a.TRACK_ID
JOIN {{ ref('artist') }} ar ON a.ARTIST_ID = ar.ARTIST_ID
GROUP BY genre
HAVING COUNT(*) > 5
ORDER BY explicit_percentage DESC
