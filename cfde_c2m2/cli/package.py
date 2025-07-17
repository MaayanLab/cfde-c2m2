from cfde_c2m2.cli import cli
from cfde_c2m2 import const
import cfde_c2m2.cli.unsimplify
from frictionless import Package
import shutil
import zipfile
from tqdm import tqdm

def package():
  ''' Package your C2M2 submission
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()
  package = Package(const.SCHEMA_FILENAME)
  with zipfile.ZipFile(const.PACKAGE_FILENAME, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=-1) as zf:
    for path in tqdm([const.SCHEMA_FILENAME]+[rc.path for rc in package.resources]):
      with zf.open(path, 'w') as fw:
        with open(path, 'rb') as fr:
          shutil.copyfileobj(fr, fw)

cli.command()(package)
