import os
try:
  import dotenv
  dotenv.load_dotenv()
except ImportError:
  pass

SCHEMA_URL = os.environ.get('CFDE_C2M2_SCHEMA_URL', 'https://osf.io/download/3sra4/')
SCHEMA_FILENAME = 'C2M2_datapackage.json'
INDEX_FILENAME = 'C2M2_datapackage.sqlite'
PACKAGE_FILENAME = 'C2M2_datapackage.zip'

OLS_URL = os.environ.get('CFDE_C2M2_OLS_URL', 'https://cfde-ols.k8s.dev.maayanlab.cloud')

# TODO: if we bake this information into the C2M2_datapackage we won't need to update this library
#       otherwise it *could* come from the OLS service as well
import functools
@functools.cache
def CV_TABLES():
  import requests, logging
  req = requests.get(f"{OLS_URL}/api/v1/c2m2-cv-tables")
  if req.ok:
    return req.json()
  else:
    logging.warning("Failed to get c2m2-cv-tables from OLS")
    return {
      'compound': 'CID',
      'substance': 'SID',
      'gene': 'Ensembl',
      'protein': 'UniProtKB',
      'sample_prep_method': 'OBI',
      'assay_type': 'OBI',
      'analysis_type': 'OBI',
      'ncbi_taxonomy': 'NCBITaxon',
      'anatomy': 'UBERON_anatomy',
      'biofluid': 'UBERON_biofluid',
      'file_format': 'EDAM_format',
      'data_type': 'EDAM_data',
      'disease': 'DO',
      'phenotype': 'HPO',
    }
