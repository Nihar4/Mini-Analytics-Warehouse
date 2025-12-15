-- Hourly traffic patterns
SELECT
    event_hour,
    COUNT(*)                   AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM events
WHERE event_hour IS NOT NULL
GROUP BY event_hour
ORDER BY event_hour;
