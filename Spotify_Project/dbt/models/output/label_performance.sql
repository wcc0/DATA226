SELECT
    LABEL,
    COUNT(*) AS album_count,
    AVG(ALBUM_POPULARITY) AS avg_popularity
FROM {{ ref('albums') }}
GROUP BY LABEL
ORDER BY avg_popularity DESC