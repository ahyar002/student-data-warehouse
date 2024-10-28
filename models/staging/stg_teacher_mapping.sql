select
    distinct teacher_id
    ,class
    ,subject_id
from 
    {{ source('sources', 'teacher_mapping')}}