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

  # this package/package_schema business is a hack because once you open the context manager
  #   `with resource:` the schema gets broken and foreign keys go missing
  #   thus we only use `with` using `package` and whenever we want the schema we use `package_schema`
  package = Package(const.SCHEMA_FILENAME)
  package_schema = Package(const.SCHEMA_FILENAME)

  writers = {}
  Q = [('find', resource_name) for resource_name in package_schema.resource_names if resource_name not in const.CV_TABLES()]

  # 1. extract IRIs from all resources {rc}.ids.tsv file
  # 2. create updated CV files
  # 3. if those updated CV files may contain other references, extract ids from it
  # 4. update that CV file again
  while Q:
    op, resource_name = Q.pop(0)
    if op in ('rewrite', 'rewrite2'):
      writers.pop(resource_name).close()
      resource_path = pathlib.Path(package_schema.get_resource(resource_name).path)
      # load existing iris into memory
      resource = package.get_resource(resource_name)
      with resource:
        existing_iris = {
          str(row['id']): row.to_dict()
          for row in tqdm(resource.row_stream, desc=f"[{op} {resource_name}]: loading existing IRIs...")
          # ignore existing iris where only the id exists
          if any(bool(value) for key, value in row.items() if key != 'id')
        }
      # process all unique iri metadata into the cv table
      with utils.OpenDictWriter(resource_path.with_suffix('.tmp'), fieldnames=[field.name for field in package_schema.get_resource(resource_name).schema.fields]) as writer:
        rc_ids = pathlib.Path(f"{resource_name}.ids.tsv")
        if rc_ids.exists():
          ids = set()
          to_get_any = False
          with utils.OpenDictWriter(resource_path.with_suffix('.get'), fieldnames=['id'], writeheader=False) as to_get:
            with utils.OpenDictReader(f"{resource_name}.ids.tsv") as reader:
              for row in tqdm(reader, desc=f"[{op} {resource_name}]: preserving existing IRIs..."):
                if str(row['id']) in ids: continue
                if str(row['id']) in existing_iris:
                  writer.writerow(existing_iris.pop(str(row['id'])))
                else:
                  # we don't have this one? we need to get it
                  to_get_any = True
                  to_get.writerow(dict(id=str(row['id'])))
                ids.add(str(row['id']))
            #
            rc_ids.unlink()
          #
          # any IRIs to find?
          if to_get_any:
            # we submit the file of ids to the OLS API bulk-by-id endpoint
            #  it returns the records
            ontology = const.CV_TABLES()[resource_name]
            writer.writerows(
              {field.name: record.get(field.name, '') for field in package_schema.get_resource(resource_name).schema.fields}
              for record in tqdm(ols.bulk_by_id(ontology, resource_path.with_suffix('.get')), desc=f"[{op} {resource_name}]: resolving new IRIs...")
            )
          #
          resource_path.with_suffix('.get').unlink()

      assert resource_path.with_suffix('.tmp').exists()
      if resource_path.exists(): resource_path.unlink()
      resource_path.with_suffix('.tmp').rename(resource_path)

    if op in {'find', 'rewrite'}:
      fields_to_resolve = {}
      for fk in package_schema.get_resource(resource_name).schema.foreign_keys:
        if fk['reference']['resource'] in const.CV_TABLES():
          fields_to_resolve[utils.one(utils.ensure_list(fk['fields']))] = fk['reference']['resource']
      resource = package.get_resource(resource_name)
      with resource:
        for row in tqdm(resource.row_stream, desc=f"[{op} {resource.name}]: finding ontology IRIs..."):
          for field, rc in fields_to_resolve.items():
            if row.get(field):
              if rc not in writers:
                writers[rc] = utils.OpenDictWriter(f"{rc}.ids.tsv", fieldnames=('id',))
                Q.append(('rewrite2' if op == 'rewrite' else 'rewrite', rc))
              writers[rc].writerow({ 'id': row[field] })

cli.command()(prepare)
