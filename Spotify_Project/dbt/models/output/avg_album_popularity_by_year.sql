SELECT
    EXTRACT(YEAR FROM RELEASE_DATE) AS release_year,
    AVG(ALBUM_POPULARITY) AS avg_album_popularity
FROM {{ ref('albums') }}
GROUP BY 1
ORDER BY 1
