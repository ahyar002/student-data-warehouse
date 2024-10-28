select
    distinct teacher_id
    ,teacher_name
    ,subject_id
    ,subject_name
from 
    {{ source('sources', 'teacher')}}