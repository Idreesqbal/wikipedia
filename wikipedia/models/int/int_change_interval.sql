WITH slots AS (
    SELECT 
        start_time,
        end_time
    FROM {{ ref('stg_time_slots') }}
)
SELECT 
    t.start_time AS slot_start_time,
    t.end_time AS slot_end_time,
    COUNT(*) AS total_changes
FROM slots t
LEFT JOIN {{ ref('stg_wikipedia_api') }} w
ON CAST(w.timestamp AS TIMESTAMP) >= t.start_time 
   AND CAST(w.timestamp AS TIMESTAMP) < t.end_time
GROUP BY t.start_time, t.end_time
ORDER BY t.start_time
