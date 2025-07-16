from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils
import cfde_c2m2.cli.unsimplify
import pathlib
import click
import requests
import traceback
from urllib.parse import quote
from tqdm import tqdm
from frictionless import Package

def iri_resolve(resource, iri):
  try:
    if not const.OLS_URL: raise NotImplementedError('OLS is not yet implemented')
    req = requests.get(f"{const.OLS_URL}/{resource}/{quote(iri)}")
    req.raise_for_status()
    return req.json()
  except Exception as e:
    click.echo(f"[{resource}]: {iri}\n{traceback.format_exc()}")
    return { 'id': iri, 'name': iri, 'description': '' }

def prepare():
  ''' Finish preparing your c2m2 submission, filling in any blanks and resolving ontological identifiers
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()

  package = Package(const.SCHEMA_FILENAME)

  # go through all the tables and write all IRIs to a temporary {rc}.ids.tsv file
  with utils.LazyDictWriters() as writers:
    for resource in package.resources:
      if resource.name in const.CV_TABLES: continue
      click.echo(f"[{resource.name}]: analysing")
      fields_to_resolve = {}
      for fk in resource.schema.foreign_keys:
        if fk['reference']['resource'] in const.CV_TABLES:
          fields_to_resolve[utils.one(utils.ensure_list(fk['fields']))] = fk['reference']['resource']
      with resource:
        for row in tqdm(resource.row_stream, desc=f"[{resource.name}]: finding ontology IRIs..."):
          for field, rc in fields_to_resolve.items():
            if row.get(field): writers(f"{rc}.ids.tsv", fieldnames=('id',)).writerow({ 'id': row[field] })

  # re-write all the CV tables, re-using already resolved IRIs and resolving any new ones
  for resource in package.resources:
    if resource.name not in const.CV_TABLES: continue
    click.echo(f"[{resource.name}]: preparing")
    resource_path = pathlib.Path(resource.path)
    # load existing iris into memory
    with resource:
      existing_iris = {
        row['id']: row.to_dict()
        for row in tqdm(resource.row_stream, desc=f"[{resource.name}]: loading existing IRIs...")
      }
    # process all unique iri metadata into the cv table
    with utils.OpenDictWriter(resource_path.with_suffix('.tmp'), fieldnames=[field.name for field in resource.schema.fields]) as writer:
      rc_ids = pathlib.Path(f"{resource.name}.ids.tsv")
      if rc_ids.exists():
        ids = set()
        with utils.OpenDictReader(f"{resource.name}.ids.tsv") as reader:
          for row in tqdm(reader, desc=f"[{resource.name}]: resolving ontology IRIs..."):
            if row['id'] in ids: continue
            if row['id'] in existing_iris:
              writer.writerow(existing_iris[row['id']])
            else:
              writer.writerow(iri_resolve(resource.name, row['id']))
            ids.add(row['id'])
        #
        rc_ids.unlink()
    resource_path.with_suffix('.tmp').rename(resource_path)

cli.command()(prepare)
