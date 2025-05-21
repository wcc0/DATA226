
  create or replace   view USER_DB_DOG.analytics.session_timestamp
  
   as (
    SELECT
    sessionId,
    ts
FROM USER_DB_DOG.raw.session_timestamp
  );

