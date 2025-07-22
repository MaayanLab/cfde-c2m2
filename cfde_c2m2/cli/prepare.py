from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils, ols
import cfde_c2m2.cli.unsimplify
import pathlib
import click
from tqdm import tqdm
from frictionless import Package

def prepare():
  ''' Finish preparing your c2m2 submission, filling in any blanks and resolving ontological identifiers
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()

  package = Package(const.SCHEMA_FILENAME)

  # go through all the tables and write all IRIs to a temporary {rc}.ids.tsv file
  with utils.LazyDictWriters() as writers:
    for resource in package.resources:
      if resource.name in const.CV_TABLES(): continue
      click.echo(f"[{resource.name}]: analysing")
      fields_to_resolve = {}
      for fk in resource.schema.foreign_keys:
        if fk['reference']['resource'] in const.CV_TABLES():
          fields_to_resolve[utils.one(utils.ensure_list(fk['fields']))] = fk['reference']['resource']
      with resource:
        for row in tqdm(resource.row_stream, desc=f"[{resource.name}]: finding ontology IRIs..."):
          for field, rc in fields_to_resolve.items():
            if row.get(field): writers(f"{rc}.ids.tsv", fieldnames=('id',)).writerow({ 'id': row[field] })

  # re-write all the CV tables, re-using already resolved IRIs and resolving any new ones
  for resource in package.resources:
    if resource.name not in const.CV_TABLES(): continue
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
        with utils.OpenDictWriter(resource_path.with_suffix('.get'), fieldnames=['id'], writeheader=False) as to_get:
          ids = set()
          to_get_any = False
          with utils.OpenDictReader(f"{resource.name}.ids.tsv") as reader:
            for row in tqdm(reader, desc=f"[{resource.name}]: preserving existing IRIs..."):
              if row['id'] in ids: continue
              if row['id'] in existing_iris:
                writer.writerow(existing_iris[row['id']])
              else:
                # we don't have this one? we need to get it
                to_get_any = True
                to_get.writerow(dict(id=row['id']))
              ids.add(row['id'])
          #
          rc_ids.unlink()
        #
        # any IRIs to find?
        if to_get_any:
          # we submit the file of ids to the OLS API bulk-by-id endpoint
          #  it returns the records
          ontology = const.CV_TABLES()[resource.name]
          writer.writerows(
            {field.name: record.get(field.name, '') for field in resource.schema.fields}
            for record in tqdm(ols.bulk_by_id(ontology, resource_path.with_suffix('.get')), desc=f"[{resource.name}]: resolving new IRIs...")
          )
        #
        resource_path.with_suffix('.get').unlink()

    resource_path.with_suffix('.tmp').rename(resource_path)

cli.command()(prepare)
