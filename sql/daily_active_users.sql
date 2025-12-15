-- Daily active users (unique sessions per day)
SELECT
    event_date_str           AS event_date,
    COUNT(DISTINCT session_id) AS active_users
FROM events
GROUP BY event_date_str
ORDER BY event_date_str;
