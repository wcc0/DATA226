SELECT
    EXTRACT(YEAR FROM a.RELEASE_DATE) AS release_year,
    t.TRACK_ID,
    t.TRACK_POPULARITY,
    RANK() OVER (
        PARTITION BY EXTRACT(YEAR FROM a.RELEASE_DATE)
        ORDER BY t.TRACK_POPULARITY DESC
    ) AS rank
FROM {{ ref('tracks') }} t
JOIN {{ source('raw', 'albums') }} a
  ON t.TRACK_ID = a.TRACK_ID
WHERE a.RELEASE_DATE IS NOT NULL
