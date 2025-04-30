SELECT
  EXTRACT(YEAR FROM release_date) AS release_date,
  AVG(popularity) AS avg_popularity
FROM {{ ref('top_tracks') }}
GROUP BY 1
ORDER BY 1
