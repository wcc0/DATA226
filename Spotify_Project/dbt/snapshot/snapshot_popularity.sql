{% snapshot top_tracks_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='track_name',
      strategy='check',
      check_cols=['popularity']
    )
}}

select
    track_name,
    artist_name,
    popularity,
    release_date,
    duration_ms
from {{ ref('top_tracks') }}

{% endsnapshot %}