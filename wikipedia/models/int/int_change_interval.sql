WITH slots AS (
    SELECT 
        start_time,
        end_time
    FROM {{ ref('stg_time_slots') }}
),
intervals AS (
    SELECT 
        timestamp,
        FLOOR(EXTRACT(EPOCH FROM timestamp) / (30 * 60)) AS slot
    FROM {{ ref('stg_wikipedia_api') }}
),
shifted_intervals AS (
    SELECT 
        timestamp,
        FLOOR((EXTRACT(EPOCH FROM timestamp) - 15 * 60) / (30 * 60)) AS shifted_slot
    FROM {{ ref('stg_wikipedia_api') }}
),
combined AS (
    SELECT slot, COUNT(*) AS changes FROM intervals GROUP BY slot
    UNION ALL
    SELECT shifted_slot AS slot, COUNT(*) AS changes FROM shifted_intervals GROUP BY shifted_slot
),
slot_changes AS (
    SELECT 
        TO_TIMESTAMP(slot * 30 * 60) AS start_time,
        TO_TIMESTAMP((slot + 1) * 30 * 60) AS end_time,
        SUM(changes) AS total_changes
    FROM combined
    GROUP BY slot
)
SELECT 
    t.start_time AS slot_start_time,
    t.end_time AS slot_end_time,
    COALESCE(SUM(CASE 
        WHEN w.timestamp >= t.start_time AND w.timestamp < t.end_time THEN 1 
        ELSE 0 
    END), 0) AS total_changes
FROM public.time_slots t
LEFT JOIN public.stg_wikipedia_api w
ON w.timestamp >= t.start_time AND w.timestamp < t.end_time
GROUP BY t.start_time, t.end_time
ORDER BY t.start_time 