{% snapshot top_tracks_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='TRACK_ID',
      strategy='check',
      check_cols=['TRACK_POPULARITY']
    )
}}

SELECT
    TRACK_ID,
    TRACK_POPULARITY,
    EXPLICIT
FROM {{ ref('tracks') }}

{% endsnapshot %}
