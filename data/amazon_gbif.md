# Amazonas wildlife observations

This local sample contains 765 GBIF occurrence records for 374 animal species in the Brazilian state of Amazonas. The sample is not a complete inventory of the state or the Amazon basin.

Source: [GBIF Occurrence Search API](https://techdocs.gbif.org/en/openapi/v1/occurrence), retrieved on 2026-11-16. The search requested present human observations from Brazil, with coordinates, no flagged geospatial issues, and a CC BY 4.0 license. Results were filtered to records whose reported state was Amazonas and whose interpreted kingdom was Animalia. The sample includes three pages of general results and additional searches for jaguars, giant otters, harpy eagles, and capybaras.

Each JSON row stores the GBIF occurrence ID, scientific species name, observation date, rights holder, and license URL. The app represents locations at state level. It displays the record and license links alongside the evidence. Individual records are available at `https://www.gbif.org/occurrence/{id}`.
