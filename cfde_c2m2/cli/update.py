from cfde_c2m2.cli import cli
from cfde_c2m2 import const, utils
import sys
import click
import pathlib
import urllib.request

def update():
  ''' Update to the latest version of C2M2_datapackage.json
  '''
  datapackage = pathlib.Path(const.SCHEMA_FILENAME)
  click.echo(f"Checking current `{const.SCHEMA_FILENAME}`...")
  current_sha256 = utils.sha256(datapackage) if datapackage.exists() else None
  click.echo(f"Fetching latest `{const.SCHEMA_FILENAME}`...")
  urllib.request.urlretrieve(const.SCHEMA_URL, datapackage)
  new_sha256 = utils.sha256(datapackage)
  if current_sha256 != new_sha256:
    click.echo(f'Updated!')
  else:
    click.echo('Nothing changed!')

cli.command()(update)
