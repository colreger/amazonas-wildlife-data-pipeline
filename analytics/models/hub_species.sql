select distinct species, md5(species) as species_key
from {{ ref('stg_observations') }}
