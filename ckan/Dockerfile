FROM ckan/ckan-base:2.10

# Extensions install
RUN pip install --no-cache-dir ckanext-scheming
RUN pip install --no-cache-dir ckanext-envvars
# DCAT-AP RDF / JSON-LD serialisation (FAIR I1). Pulls rdflib/geomet.
RUN pip install --no-cache-dir ckanext-dcat
