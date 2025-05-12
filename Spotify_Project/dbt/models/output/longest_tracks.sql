SELECT TRACK_NAME, ARTIST_0, DURATION_SEC / 60 AS duration_min
FROM {{ ref('albums') }}
ORDER BY duration_min DESC
LIMIT 10