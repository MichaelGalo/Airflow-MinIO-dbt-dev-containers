{{config(
    materialized='incremental',
    unique_key='messageID',
)}}

-- cast the messageType to the actual name of the event

SELECT
    "messageID",
    "messageType",
    "messageBody",
    CAST("messageIssueTime" AS TIMESTAMP) as "messageIssueTime_formatted"
FROM
    {{ source('bronze', 'SPACE_WEATHER_RAW') }}
WHERE
    "messageType" IS NOT NULL
-- this block below is optional I think
-- {% if is_incremental() %}
--     AND messageIssueTime > (SELECT MAX(messageIssueTime) FROM {{ this }})
-- {% endif %}
ORDER BY
    "messageIssueTime"



-- -- Possibly required for Snowflake to use quotes
-- SELECT
--     "messageID",
--     "messageType",
--     "messageBody",
--     CAST("messageIssueTime" AS TIMESTAMP) as "messageIssueTime_formatted"
-- FROM
--     {{ source('bronze', 'SPACE_WEATHER_RAW') }}
-- WHERE
--     "messageType" IS NOT NULL
-- ORDER BY
--     "messageIssueTime"