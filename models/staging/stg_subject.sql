select
    distinct
    subject_id
    ,subject_name
from 
    {{ source('sources', 'subject')}}
where 
    subject_id is not null