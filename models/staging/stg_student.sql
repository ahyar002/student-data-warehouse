select
    distinct
    student_id
    ,student_name
    ,registered_class as class
    ,home_region
    ,getdate() as start_date
    ,null as end_date
from 
    {{ source('sources', 'student')}}
where 
    student_id is not null