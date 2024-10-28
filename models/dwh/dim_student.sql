
with join_table as (

    select 
        t1.student_id as t1_student_id
        ,t1.student_name as t1_student_name
        ,t1.class as t1_class
        ,t1.home_region as t1_home_region
        ,t1.start_date as t1_start_date
        ,t1.end_date as t1_end_date
        ,t2.student_id as t2_student_id
        ,t2.student_name as t2_student_name
        ,t2.class as t2_class
        ,t2.home_region as t2_home_region
        ,t2.start_date as t2_start_date
        ,t2.end_date as t2_end_date
    from 
        {{ ref('stg_student') }} t2 
    left join 
        {{ source('dwh', 'dim_student')}} t1 on t1.student_id = t2.student_id

)
, update_current_record as (

    -- Update the end_date of the existing record when there is a change
    select
        {{ dbt_utils.generate_surrogate_key([' t1_student_id', ' t1_student_name', 't1_class', 't1_home_region']) }} sk_student
        ,t1_student_id as student_id
        , t1_student_name as student_name
        ,t1_class as class
        ,t1_home_region as home_region
        ,t1_start_date as start_date
        ,getdate() as end_date    
    from
       join_table
    where
         t1_student_id = t2_student_id
    and 
        t1_start_date <> t2_start_date
    and 
        t1_end_date is null
    and( 
        t1_class <> t2_class
    or 
        t1_home_region <> t2_home_region
    )

)
, insert_updated_record as (

    --Insert a new record when a change is detected
    select
        {{ dbt_utils.generate_surrogate_key(['t2_student_id', 't2_student_name', 't2_class', 't2_home_region']) }} sk_student,
        t2_student_id
        ,t2_student_name
        ,t2_class
        ,t2_home_region
        ,t2_start_date
        ,t2_end_date    
    from
        join_table
    where
         t1_student_id= t2_student_id
    and  
        t1_start_date <> t2_start_date
    and( 
        t1_class <> t2_class
    or 
        t1_home_region <> t2_home_region
    )
)
, insert_new_record as (

    -- insert new record
    select
        {{ dbt_utils.generate_surrogate_key(['t2_student_id', 't2_student_name', 't2_class', 't2_home_region' ]) }} sk_student
        ,t2_student_id
        ,t2_student_name
        ,t2_class
        ,t2_home_region
        ,t2_start_date
        ,t2_end_date    
    from
        join_table
    where
         t1_student_id is null
)
, insert_all_record as (

    select
        {{ dbt_utils.generate_surrogate_key(['t2_student_id', 't2_student_name', 't2_class', 't2_home_region' ]) }} sk_student
        ,t2_student_id
        ,t2_student_name
        ,t2_class
        ,t2_home_region
        ,t2_start_date
        ,t2_end_date    
    from
        join_table
)

select *
from update_current_record

union

select *
from insert_updated_record

union

select *
from insert_new_record

union

select *
from insert_all_record