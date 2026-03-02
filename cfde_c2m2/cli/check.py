from cfde_c2m2.cli import cli
from cfde_c2m2 import const
import sys
import cfde_c2m2.cli.unsimplify
import pathlib
import click
from frictionless import Package

def check():
  ''' Simply check that columns match what's in the schema
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()
  package = Package(const.SCHEMA_FILENAME)
  for resource in package.resources:
    resource_path = pathlib.Path(resource.path)
    if not resource_path.exists():
      click.echo(f"{resource.name} missing! did you run init?")
      sys.exit(1)
    else:
      with resource_path.open('r') as fr:
        current_columns = fr.readline().rstrip('\r\n').split('\t')
        schema_columns = [field.name for field in resource.schema.fields]
        if current_columns == schema_columns: continue
        #
        add_columns = set(schema_columns) - set(current_columns)
        for column in add_columns:
          click.echo(f"[{resource.name}]: {column} is missing! did you run init?")
          sys.exit(1)
        #
        del_columns = set(current_columns) - set(schema_columns)
        for column in del_columns:
          click.echo(f"[{resource.name}]: {column} was removed! did you run init?")
          sys.exit(1)

cli.command()(check)
