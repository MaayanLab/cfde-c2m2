from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils
import cfde_c2m2.cli.check
import os
import sys
import click
import subprocess
from tqdm import tqdm

def validate_fks(schema, index, max_errors=100):
  import sqlite3
  from frictionless import Package
  pkg = Package(schema)
  with sqlite3.connect(index) as con:
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

def validate_unique_persistent_ids(schema, index, max_errors=100):
  import sqlite3
  from frictionless import Package
  pkg = Package(schema)
  with sqlite3.connect(index) as con:
    cur = con.cursor()
    valid = True
    tables = {
      rc.name
      for rc in pkg.resources
      if any(field.name == 'persistent_id' for field in rc.schema.fields)
    }
    # checking non-unique persistent ids
    query = f"""
      select persistent_id, count(persistent_id) as count, group_concat(tbl) as tables
      from (
      {' union '.join(f"select persistent_id, '{table}' as tbl from {table} where persistent_id is not null" for table in tables)}
      ) t
      group by t.persistent_id
      having count(persistent_id) > 1
      order by count(persistent_id) desc
      limit {max_errors}
      ;
    """
    for persistent_id, count, tables in cur.execute(query):
      valid = False
      click.echo(f"[{tables}]: {persistent_id} duplicated {count} times")
    cur.close()
    #
    return valid

def validate_access_url_checksums(schema, index, max_errors=100):
  import sqlite3
  from frictionless import Package
  pkg = Package(schema)
  with sqlite3.connect(index) as con:
    cur = con.cursor()
    valid = True
    # checking missing checksums when access_url is not
    query = f"""
      select id_namespace, local_id
      from file
      where access_url is not null
      and (sha256 is null and md5 is null)
      limit {max_errors}
      ;
    """
    for id_namespace, local_id in cur.execute(query):
      valid = False
      click.echo(f"[file]: ({id_namespace}, {local_id}) checksum must be present for file with access_url defined")
    #
    return valid

def validate_no_drs_persistent_id(schema, index, max_errors=100):
  import sqlite3
  from frictionless import Package
  pkg = Package(schema)
  with sqlite3.connect(index) as con:
    cur = con.cursor()
    valid = True
    # check drs in persistent_id
    query = f"""
      select id_namespace, local_id
      from file
      where persistent_id like 'drs://%'
      limit {max_errors}
      ;
    """
    for id_namespace, local_id in cur.execute(query):
      valid = False
      click.echo(f"[file]: ({id_namespace}, {local_id}) persistent_id contains drs:// -- this belongs in access_url, see updated docs!")
    #
    return valid

def validate():
  ''' Validate that your C2M2 submission is valid
  '''
  cfde_c2m2.cli.check.check()
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
  valid = True
  #
  click.echo(f"Validating foreign key integrity...")
  valid = validate_fks(const.SCHEMA_FILENAME, const.INDEX_FILENAME) and valid
  #
  click.echo(f"Validating uniqueness of persistent ids...")
  valid = validate_unique_persistent_ids(const.SCHEMA_FILENAME, const.INDEX_FILENAME) and valid
  #
  click.echo(f"Validating checksums on files with access urls...")
  valid = validate_access_url_checksums(const.SCHEMA_FILENAME, const.INDEX_FILENAME) and valid
  #
  click.echo(f"Validating drs is not in persistent_id...")
  valid = validate_no_drs_persistent_id(const.SCHEMA_FILENAME, const.INDEX_FILENAME) and valid
  #
  if not valid:
    sys.exit(1)

cli.command()(validate)
