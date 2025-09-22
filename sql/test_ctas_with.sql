create table test1
as
with fmf_int as (
    select  col1,col2,col3 from test_table
)
,fmf as (
    select f.* from fmf_int f
    inner join (select test_col from another_table) as a on 1=2
)
,msg_rank as (
    select t_message,col1 from fmf f 
    inner join table2 on 1=2
    inner join table3 on 1=2
    where col2 > 100
)
,msg as (
    select t_message from msg_rank
)
select * from msg;