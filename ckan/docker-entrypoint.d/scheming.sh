#!/bin/sh
# Register the FAIR sensor scheming schema directly in the CKAN ini.
#
# Why this is needed:
#   ckanext-scheming reads `scheming.dataset_schemas` in its IConfigurer
#   update_config (plugins.py), but ckanext-envvars populates that key only
#   *after* scheming has already read it — so when configured purely via the
#   CKAN___SCHEMING__DATASET_SCHEMAS env var the schema silently never
#   registers (scheming_dataset_schema_list returns []), and every custom FAIR
#   field is dropped on package_create/patch.
#
#   The ini is parsed before any plugin update_config runs, so writing the
#   value here (start_ckan.sh sources this before launching uwsgi) guarantees
#   scheming sees it. Keep this in sync with .env's CKAN___SCHEMING__* values.
ckan config-tool "$CKAN_INI" \
  "scheming.dataset_schemas=file:///srv/app/schemas/sensor_schema.yaml" \
  "scheming.presets=ckanext.scheming:presets.json"
