{{config(
    materialized='incremental',
    unique_key='"messageID"',
)}}

SELECT
    "messageID",
    "messageType",
    "messageBody",
    CAST("messageIssueTime" AS TIMESTAMP) as "messageIssueTime_formatted"
FROM
    {{ source('bronze', 'space_weather_raw') }}
WHERE
    "messageType" IS NOT NULL
ORDER BY
    "messageIssueTime"