
    with known_cte as (
        select col1, col2, col3 from test_table
    )
    select t_message, col1 from known_cte f 
    inner join unknown_table on 1=2
    