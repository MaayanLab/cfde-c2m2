SCHEMA_FILENAME = 'C2M2_datapackage.json'
SCHEMA_URL = 'https://osf.io/download/3sra4/'

# TODO: let's create this service
OLS_URL = None#'https://ols.cfde.cloud'

# TODO: if we bake this information into the C2M2_datapackage we won't need to update this library
#       otherwise it *could* come from the OLS service as well
CV_TABLES = {
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
