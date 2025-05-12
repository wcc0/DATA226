SELECT
    ALBUM_TYPE,
    EXTRACT(YEAR FROM RELEASE_DATE) AS release_year,
    COUNT(*) AS album_count
FROM {{ ref('albums') }}
GROUP BY 1, 2
ORDER BY 2, 1