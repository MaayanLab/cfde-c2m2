from cfde_c2m2.cli import cli
from cfde_c2m2 import const
import pathlib
from frictionless import Package

def simplify():
  ''' Simplify your C2M2 directory, hiding tables that can be filled in automatically
  '''
  cv_dir = pathlib.Path('cv')
  cv_dir.mkdir(exist_ok=True, parents=True)
  package = Package(const.SCHEMA_FILENAME)
  for resource in package.resources:
    if resource.name not in const.CV_TABLES(): continue
    pathlib.Path(resource.path).rename(cv_dir/resource.path)

cli.command()(simplify)
