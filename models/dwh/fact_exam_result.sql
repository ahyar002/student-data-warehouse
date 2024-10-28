select 
    exam_result_id
    ,{{ dbt_utils.generate_surrogate_key(['exam_event']) }} sk_exam_event
    ,sk_student
    ,sk_teacher
    ,sk_subject
    ,t1.student_id
    ,exam_date
    ,exam_submit_time
    ,exam_score
    
from {{ ref('stg_exam_result')}} t1 
join (select * from {{ ref('dim_student')}} where end_date is null) t2 on t1.student_id = t2.student_id
join {{ ref('dim_subject')}} t3 on t1.subject_id = t3.subject_id
join {{ ref('dim_teacher')}} t4 on t1.subject_id = t4.subject_id and t2.class = t4.class


