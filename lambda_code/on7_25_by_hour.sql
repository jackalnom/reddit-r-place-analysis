WITH pixels_to_select AS (
    SELECT x, y, pixel_color, timestamp
    FROM "processed" 
    WHERE date_trunc('day', timestamp) = timestamp '2023-07-25'
),
adjusted AS (
    SELECT ROUND(x/128) new_x, (y/3) new_y, ROUND(date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00', timestamp) / 750) AS rounded_timestamp,
        *
    FROM pixels_to_select
),
query_filter AS (
    SELECT * FROM (
        SELECT new_x, new_y, rounded_timestamp, COUNT(*) pixels FROM
        (SELECT DISTINCT x, y, new_x, new_y, rounded_timestamp
        FROM adjusted)
        GROUP BY new_x, new_y, rounded_timestamp
    ) WHERE pixels >= 16
),
joined AS (
    SELECT adjusted.*, date_trunc('hour', adjusted.timestamp) date_bin
    FROM adjusted
    JOIN query_filter ON query_filter.new_x = adjusted.new_x AND query_filter.new_y = adjusted.new_y AND query_filter.rounded_timestamp = adjusted.rounded_timestamp
)
SELECT date_bin, x, y, pixel_color 
FROM (
    SELECT date_bin, x, y, pixel_color, ROW_NUMBER() OVER (PARTITION BY date_bin, x, y ORDER BY timestamp DESC) ranking
    FROM joined
)
WHERE ranking = 1