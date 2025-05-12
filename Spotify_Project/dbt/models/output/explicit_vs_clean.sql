SELECT
    EXTRACT(YEAR FROM a.RELEASE_DATE) AS release_year,
    t.EXPLICIT,
    COUNT(*) AS track_count,
    AVG(t.TRACK_POPULARITY) AS avg_popularity
FROM {{ ref('tracks') }} t
JOIN {{ source('raw', 'albums') }} a
  ON t.TRACK_ID = a.TRACK_ID
WHERE a.RELEASE_DATE IS NOT NULL
GROUP BY 1, t.EXPLICIT
ORDER BY 1
