-- models/output/avg_popularity.sql

SELECT
    EXTRACT(YEAR FROM a.RELEASE_DATE) AS release_year,
    AVG(t.TRACK_POPULARITY) AS avg_popularity
FROM {{ ref('tracks') }} t
JOIN {{ source('raw', 'albums') }} a
  ON t.TRACK_ID = a.TRACK_ID
WHERE a.RELEASE_DATE IS NOT NULL
GROUP BY 1
ORDER BY 1
