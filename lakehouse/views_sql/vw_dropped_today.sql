SELECT y.ID
FROM dbo.vw_pop_yesterday y
LEFT JOIN dbo.vw_pop_today t
    ON t.ID = y.ID
WHERE t.ID IS NULL;
