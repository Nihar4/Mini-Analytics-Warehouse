-- Revenue by category
SELECT
    category_name,
    COUNT(*)                   AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(SUM(price), 2)       AS total_revenue,
    ROUND(AVG(price), 2)       AS avg_price,
    ROUND(MIN(price), 2)       AS min_price,
    ROUND(MAX(price), 2)       AS max_price
FROM events
GROUP BY category_name
ORDER BY total_revenue DESC;
