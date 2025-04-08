-- Query to get table size and average row size based on collected statistics
-- Replace 'your_database_name' and 'your_table_name' with your actual names

SELECT
    ts.DatabaseName,
    ts.TableName,
    ts.CurrentPerm AS TotalCurrentPermBytes,
    CAST(ts.CurrentPerm / 1024.0 / 1024.0 AS DECIMAL(18,2)) AS TotalCurrentPermMB,
    CAST(ts.CurrentPerm / 1024.0 / 1024.0 / 1024.0 AS DECIMAL(18,2)) AS TotalCurrentPermGB,
    CAST(ts.CurrentPerm / 1024.0 / 1024.0 / 1024.0 / 1024.0 AS DECIMAL(18,2)) AS TotalCurrentPermTB,
    t.RowCount AS EstimatedRowCount_FromStats,
    t.LastCollectTimeStamp AS StatsLastCollected, -- Crucial info! Check how recent this is.
    CASE
        WHEN t.RowCount IS NULL OR t.RowCount = 0 THEN 0.0 -- Avoid division by zero
        ELSE CAST(ts.CurrentPerm * 1.0 / t.RowCount AS DECIMAL(18,2)) -- Multiply by 1.0 for float division
    END AS AvgRowSizeBytes_StatBased
FROM DBC.TableSizeV ts
JOIN DBC.TablesV t ON ts.DatabaseName = t.DatabaseName
                  AND ts.TableName = t.TableName
WHERE ts.DatabaseName = UPPER('your_database_name')  -- Use UPPER for safety unless names are case-sensitive
  AND ts.TableName = UPPER('your_table_name')
  AND t.TableKind = 'T'; -- Ensure it's a table (not view, macro etc.)
