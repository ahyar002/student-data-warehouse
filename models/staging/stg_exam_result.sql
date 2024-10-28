select
    distinct exam_result_id
    ,student_id
    ,exam_date
    ,exam_event
    ,exam_score
    ,exam_submit_time
    ,subject_id
from 
    {{ source('sources', 'exam_result')}}