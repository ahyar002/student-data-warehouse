select
    distinct exam_date
    ,extract(year from exam_date) AS year
    ,extract(month from exam_date) AS month
    ,extract(day from exam_date) AS day
    ,extract(dayofweek from exam_date) as weekday
    ,extract(week from exam_date) as week_of_year
from 
    {{ ref('stg_exam_result')}}