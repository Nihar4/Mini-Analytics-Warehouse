-- Top event types (pages visited by sequence position)
SELECT
    page_number          AS page_position,
    COUNT(*)             AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM events
GROUP BY page_number
ORDER BY total_events DESC;
