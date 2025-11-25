SELECT t.ID
FROM dbo.vw_pop_today t
LEFT JOIN dbo.vw_pop_yesterday y
    ON t.ID = y.ID
WHERE y.ID IS NULL;
