with temp_init(
    select b.dr_cr as debit_credit,cif
    from (select cif from db.test1) as a
    inner join (select dr_cr from db.test3) as b on a.cif = b.cif
)
, temp_oth(
    select sum(case when debit_credit = 'D' then 1 else 0 end) as cnt_debit
    ,cif
    from temp_init
)
,perc_temp(
    select a.*,2.5*col1 as col2, 
    from (
        select cif,avg(cnt_debit) as col1 from temp_oth
    ) a
),
final_temp(
    select case when col2 > 10 then cif else 0 end as col3
    from perc_temp
)
select col3 from final_temp
