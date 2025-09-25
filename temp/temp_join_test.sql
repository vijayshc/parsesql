
    with limited_cte as (
        select col1, col2 from test_table  
    )
    select * from limited_cte l
    inner join unknown_table u on 1=1
    