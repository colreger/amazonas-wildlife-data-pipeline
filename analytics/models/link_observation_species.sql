select o.observation_key, s.species_key
from {{ ref('stg_observations') }} as input
join {{ ref('hub_observation') }} as o using (gbif_id)
join {{ ref('hub_species') }} as s using (species)
