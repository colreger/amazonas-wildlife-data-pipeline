select cast(date_trunc('month', observed_on) as date) as observation_month,
       species,
       count(*) as observation_count
from {{ ref('fct_observations') }}
where observed_on is not null
group by 1, 2
