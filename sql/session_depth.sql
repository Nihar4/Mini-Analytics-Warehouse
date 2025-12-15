-- Session depth analysis: how many pages per session
SELECT
    pages_per_session,
    COUNT(*) AS session_count
FROM (
    SELECT
        session_id,
        COUNT(*) AS pages_per_session
    FROM events
    GROUP BY session_id
)
GROUP BY pages_per_session
ORDER BY pages_per_session;
