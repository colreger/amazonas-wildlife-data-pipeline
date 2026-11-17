select gbif_id, species, observed_on, source_url
from {{ source('gbif', 'observation_input') }}
