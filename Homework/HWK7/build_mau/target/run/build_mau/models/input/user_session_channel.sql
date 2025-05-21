
  create or replace   view USER_DB_DOG.analytics.user_session_channel
  
   as (
    SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_DOG.raw.user_session_channel
  );

