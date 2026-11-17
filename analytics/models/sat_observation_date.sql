select h.observation_key, v.observed_on, v.version_number, v.version_loaded_at, v.is_simulated
from {{ source('gbif', 'observation_versions') }} as v
join {{ ref('hub_observation') }} as h using (gbif_id)
