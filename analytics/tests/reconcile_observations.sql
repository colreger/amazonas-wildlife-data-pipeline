-- No observation may disappear in the joins or be counted twice in the mart.
select 1 as mismatch
where (select count(*) from {{ ref('fct_observations') }})
      <> (select count(*) from {{ ref('stg_observations') }})
   or (select sum(observation_count) from {{ ref('mart_species_month') }})
      <> (select count(*) from {{ ref('stg_observations') }} where observed_on is not null)
