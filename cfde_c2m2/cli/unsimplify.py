from cfde_c2m2.cli import cli
import pathlib

def unsimplify():
  ''' Un-simplify the C2M2 directory, moving CV tables back into the main directory
  '''
  cv_dir = pathlib.Path('cv')
  if cv_dir.exists():
    for f in cv_dir.glob('*'):
      f.rename(f.name)
    cv_dir.rmdir()
  
cli.command()(unsimplify)
