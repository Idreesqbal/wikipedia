SELECT 
    title,
    timestamp,
    "user",
    userid,
    comment,
    type
FROM {{ source('wikipedia_source', 'wikipedia_api') }}
