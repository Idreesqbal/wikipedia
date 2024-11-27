SELECT
    start_time,
    end_time
FROM
    {{ source('wikipedia_source', 'time_slots') }}