
select
    {{ dbt_utils.generate_surrogate_key(['t1.teacher_id', 't1.teacher_name','t2.class' ]) }} sk_teacher
    ,t1.teacher_id
    ,t1.teacher_name
    ,t1.subject_id
    ,t1.subject_name
    ,t2.class
from 
    {{ ref('stg_teacher')}} t1
left join
    {{ ref('stg_teacher_mapping')}} t2 on t1.teacher_id = t2.teacher_id
