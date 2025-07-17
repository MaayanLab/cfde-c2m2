from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils
import cfde_c2m2.cli.unsimplify
import os
import sys
import click
import subprocess
from tqdm import tqdm

def validate_fks(schema, index, max_errors=100):
  import sqlite3
  from frictionless import Package
  pkg = Package(schema)
  con = sqlite3.connect(index)
  cur = con.cursor()
  valid = True
  for rc in tqdm(pkg.resources):
    for fk in rc.schema.foreign_keys:
      # checking for orphaned fk references using the index
      query = f"""
        select
          {','.join(f"{rc.name}.{field}" for field in utils.ensure_list(rc.schema.primary_key))}
          ,
          {','.join(f"{rc.name}.{field}" for field in utils.ensure_list(fk['fields']))}
        from {rc.name}
        left join {fk['reference']['resource']}
          on ({','.join(f"{rc.name}.{field}" for field in utils.ensure_list(fk['fields']))}) = ({','.join(f"{fk['reference']['resource']}.{field}" for field in utils.ensure_list(fk['reference']['fields']))})
        where
        ({' or '.join(f"{rc.name}.{field} is not null" for field in utils.ensure_list(fk['fields']))})
        and ({' or '.join(f"{fk['reference']['resource']}.{field} is null" for field in utils.ensure_list(fk['reference']['fields']))})
        limit {max_errors}
        ;
      """
      n_pks = len(utils.ensure_list(rc.schema.primary_key))
      for i, row in enumerate(cur.execute(query)):
        pks, fks = row[:n_pks], row[n_pks:]
        valid = False
        click.echo(f"{rc.name}: pk=({','.join(pks)}) missing foreign key ({', '.join(field for field in utils.ensure_list(fk['fields']))}) = ({', '.join(fks)}) in reference table {fk['reference']['resource']} ({', '.join(field for field in utils.ensure_list(fk['reference']['fields']))})")
  #
  return valid

def validate():
  ''' Validate that your C2M2 submission is valid
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()
  click.echo(f"Validating {const.SCHEMA_FILENAME}")
  subprocess.run(
    [sys.executable, '-m', 'frictionless', 'validate', const.SCHEMA_FILENAME],
    env=os.environ,
    check=True,
  )
  click.echo(f"Indexing {const.SCHEMA_FILENAME}...")
  subprocess.run(
    [sys.executable, '-m', 'frictionless', 'index', const.SCHEMA_FILENAME, f"--database=sqlite:///{const.INDEX_FILENAME}"],
    env=os.environ,
    check=True,
  )
  #
  click.echo(f"Validating foreign key integrity...")
  if not validate_fks(const.SCHEMA_FILENAME, const.INDEX_FILENAME):
    sys.exit(1)

cli.command()(validate)
