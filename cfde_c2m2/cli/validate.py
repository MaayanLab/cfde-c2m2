from cfde_c2m2.cli import cli
import cfde_c2m2.cli.unsimplify
import os
import sys
import subprocess

def validate():
  ''' Validate that your C2M2 submission is valid
  '''
  cfde_c2m2.cli.unsimplify.unsimplify()

  sys.exit(
    subprocess.run(
      [sys.executable, '-m', 'frictionless', 'validate', 'C2M2_datapackage.json'],
      env=os.environ
    ).returncode
  )

cli.command()(validate)
