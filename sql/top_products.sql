-- Top products by view count
SELECT
    product_id,
    category_name,
    COUNT(*)               AS view_count,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(AVG(price), 2)   AS avg_price,
    ROUND(SUM(price), 2)   AS total_revenue
FROM events
GROUP BY product_id, category_name
ORDER BY view_count DESC
LIMIT 25;
