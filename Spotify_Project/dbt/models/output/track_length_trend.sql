SELECT
    EXTRACT(YEAR FROM RELEASE_DATE) AS release_year,
    AVG(DURATION_SEC) AS avg_duration_sec
FROM {{ ref('albums') }}
WHERE DURATION_SEC < 600  -- only include tracks less than 10 minutes
  AND RELEASE_DATE IS NOT NULL
GROUP BY 1
ORDER BY 1