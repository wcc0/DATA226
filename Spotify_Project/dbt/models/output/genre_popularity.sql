SELECT
    GENRE_0 AS primary_genre,
    COUNT(*) AS artist_count,
    AVG(ARTIST_POPULARITY) AS avg_popularity,
    AVG(FOLLOWERS) AS avg_followers
FROM {{ ref('artist') }}
WHERE GENRE_0 IS NOT NULL
GROUP BY GENRE_0
ORDER BY avg_popularity DESC
