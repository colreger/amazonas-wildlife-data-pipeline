select gbif_id, md5(cast(gbif_id as varchar)) as observation_key
from {{ ref('stg_observations') }}
