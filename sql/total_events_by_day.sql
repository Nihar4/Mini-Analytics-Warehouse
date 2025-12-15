-- Total events per day
SELECT
    event_date_str AS event_date,
    COUNT(*)       AS total_events
FROM events
GROUP BY event_date_str
ORDER BY event_date_str;
