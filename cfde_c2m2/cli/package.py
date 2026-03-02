from cfde_c2m2.cli import cli
from cfde_c2m2 import const
import cfde_c2m2.cli.check
from frictionless import Package
import click
import shutil
import zipfile
import pathlib
from tqdm import tqdm

@click.option('-o', '--output', type=click.Path(), default=const.PACKAGE_FILENAME, help='The name of your output zip file')
def package(output):
  ''' Package your C2M2 submission
  '''
  cfde_c2m2.cli.check.check()
  package = Package(const.SCHEMA_FILENAME)
  with zipfile.ZipFile(pathlib.Path(output).with_suffix('.zip'), 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=-1) as zf:
    for path in tqdm([const.SCHEMA_FILENAME]+[rc.path for rc in package.resources]):
      with zf.open(path, 'w') as fw:
        with open(path, 'rb') as fr:
          shutil.copyfileobj(fr, fw)

cli.command()(package)
