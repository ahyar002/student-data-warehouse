
select
    {{ dbt_utils.generate_surrogate_key(['subject_id', 'subject_name' ]) }} sk_subject
    ,subject_id
    ,subject_name
from 
    {{ ref('stg_subject')}}
