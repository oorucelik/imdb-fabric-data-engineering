SELECT 
    t.ID,
    t.popularity AS popularity_today,
    y.popularity AS popularity_yesterday,
    t.popularity - y.popularity AS popularity_diff,
    t.popularity_rank AS rank_today,
    y.popularity_rank AS rank_yesterday,
    y.popularity_rank - t.popularity_rank AS rank_diff
FROM dbo.vw_pop_today t
LEFT JOIN dbo.vw_pop_yesterday y
    ON t.ID = y.ID;
