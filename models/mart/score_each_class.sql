select class
		,t3.subject_name
        ,exam_event
        ,avg(exam_score) as avg_score
        ,MAX(exam_score) as max_score
        ,min(exam_score) as min_score
        ,STDDEV_SAMP(exam_score) as std_score
from {{ ref('fact_exam_result')}} t1
join {{ ref('dim_student')}} t2 on t1.sk_student = t2.sk_student
join {{ ref('dim_subject')}} t3 on t1.sk_subject = t3.sk_subject
join {{ ref('dim_exam_event')}} t4 on t1.sk_exam_event = t4.sk_exam_event
group by 1,2,3
order by 1,2,3
