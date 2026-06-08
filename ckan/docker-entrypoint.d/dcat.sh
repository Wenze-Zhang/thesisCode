#!/bin/sh
# Register ckanext-dcat's RDF serialisation config directly in the CKAN ini.
#
# Why this is needed (same reasoning as scheming.sh):
#   Configuring `ckanext.dcat.*` purely through CKAN___ env vars relies on
#   ckanext-envvars populating those keys before the dcat blueprints read them.
#   Writing them into the ini here (start_ckan.sh sources this before launching
#   uwsgi, the ini is parsed before any plugin update_config) guarantees the
#   profile is in effect when /dataset/{id}.ttl|.jsonld and /catalog.ttl render.
#
#   euro_dcat_ap_3 emits the core DCAT-AP triples (dct:license, dcat:keyword,
#   dct:publisher, dct:issued/modified, dct:spatial) for free. The standard
#   profile does NOT serialise the sensor-specific FAIR URIs (sosa_*, qudt_*,
#   provenance_json) — that needs the in-repo `sensordcat` profile (Work item
#   1b); append it to the profile list once ckanext-sensordcat is installed:
#     ckanext.dcat.rdf.profiles=euro_dcat_ap_3 sensordcat
#   (On older ckanext-dcat without DCAT-AP 3, use euro_dcat_ap_2.)
ckan config-tool "$CKAN_INI" \
  "ckanext.dcat.rdf.profiles=euro_dcat_ap_3" \
  "ckanext.dcat.base_uri=https://localhost:8443"
