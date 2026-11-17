select h.gbif_id, species.species, sat.observed_on, src.source_url
from {{ ref('hub_observation') }} as h
join {{ ref('link_observation_species') }} as link using (observation_key)
join {{ ref('hub_species') }} as species using (species_key)
join {{ ref('sat_observation_date') }} as sat using (observation_key)
join {{ ref('stg_observations') }} as src using (gbif_id)
where not sat.is_simulated
qualify row_number() over (partition by h.gbif_id order by sat.version_number desc) = 1
