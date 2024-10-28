
select
    distinct {{ dbt_utils.generate_surrogate_key(['exam_event' ]) }} sk_exam_event
    ,case
        when exam_event = 'Exam 1' then 1
        when exam_event = 'Exam 2' then 2
    end as exam_event_id
    ,exam_event
from 
    {{ ref('stg_exam_result')}}
