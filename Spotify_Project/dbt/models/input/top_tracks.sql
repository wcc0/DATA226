-- models/input/top_tracks.sql

with temp as (
    select * from {{ source('raw', 'top_tracks') }}
)

select
    track_name,
    artist_name,
    popularity,
    release_date,
    duration_ms
from temp