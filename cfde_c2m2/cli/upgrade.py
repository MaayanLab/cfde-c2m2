from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils
import cfde_c2m2.cli.unsimplify
from frictionless import Package
import click
import pathlib
from tqdm import tqdm

def upgrade():
  ''' Attempt to automatically upgrade your current directory to the newest version of C2M2
  All we do is put blanks in new columns/remove old columns
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()

  package = Package(const.SCHEMA_FILENAME)
  for resource in package.resources:
    resource_path = pathlib.Path(resource.path)
    if not resource_path.exists():
      click.echo(f"creating empty {resource.name}...")
      resource_path.write_text('\t'.join(
        field.name
        for field in resource.schema.fields
      ))
    else:
      with resource_path.open('r') as fr:
        current_columns = fr.readline().rstrip('\r\n').split('\t')
        schema_columns = [field.name for field in resource.schema.fields]
        if current_columns == schema_columns: continue
        #
        add_columns = set(schema_columns) - set(current_columns)
        for column in add_columns:
          click.echo(f"[{resource.name}]: {column} was added")
        #
        del_columns = set(current_columns) - set(schema_columns)
        for column in del_columns:
          click.echo(f"[{resource.name}]: {column} was removed")
        #
        click.echo(f"[{resource.name}]: upgrading")
        with resource_path.with_suffix('.tmp').open('w') as fw:
          writer = utils.JsonDictWriter(fw, fieldnames=schema_columns)
          writer.writeheader()
          reader = utils.JsonDictWriter(fr, fieldnames=current_columns)
          for record in tqdm(reader, desc=f"[{resource.name}]: upgrading"):
            for column in add_columns:
              record[column] = None
            for column in del_columns:
              del record[column]
            writer.writerow(record)
      #
      resource_path.with_suffix('.tmp').rename(resource_path)
  #
  click.echo('upgraded!')

cli.command()(upgrade)
