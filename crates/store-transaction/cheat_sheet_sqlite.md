```sql 
WITH obj AS (
SELECT t.name AS table_name, t.name AS obj_name
FROM sqlite_schema t
WHERE t.type = 'table'
UNION ALL
SELECT i.tbl_name AS table_name, i.name AS obj_name
FROM sqlite_schema i
WHERE i.type = 'index' AND i.name NOT LIKE 'sqlite_%'
)
SELECT
o.table_name,
ROUND(SUM(s.pgsize) / 1024.0 / 1024.0, 2) AS size_mb_incl_indexes
FROM obj o
JOIN dbstat s ON s.name = o.obj_name
GROUP BY o.table_name
ORDER BY size_mb_incl_indexes DESC;
```


```sql
SELECT
version,
value,
CASE
WHEN length(value) < 1024
THEN printf('%d B', length(value))
WHEN length(value) < 1024 * 1024
THEN printf('%.2f KB', length(value) / 1024.0)
WHEN length(value) < 1024 * 1024 * 1024
THEN printf('%.2f MB', length(value) / 1024.0 / 1024.0)
ELSE
printf('%.2f GB', length(value) / 1024.0 / 1024.0 / 1024.0)
END AS blob_size
FROM cdc
ORDER BY version;
```
