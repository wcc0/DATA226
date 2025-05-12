SELECT
    ar.NAME AS artist_name,
    ar.FOLLOWERS,
    AVG(t.TRACK_POPULARITY) AS avg_track_popularity
FROM {{ ref('tracks') }} t
JOIN {{ ref('albums') }} a ON t.TRACK_ID = a.TRACK_ID
JOIN {{ ref('artist') }} ar ON a.ARTIST_ID = ar.ARTIST_ID
GROUP BY artist_name, ar.FOLLOWERS
ORDER BY avg_track_popularity DESC
