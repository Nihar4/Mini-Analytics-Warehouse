-- Monthly trends
SELECT
    event_month,
    COUNT(*)                   AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(SUM(price), 2)       AS total_revenue
FROM events
WHERE event_month IS NOT NULL
GROUP BY event_month
ORDER BY event_month;
