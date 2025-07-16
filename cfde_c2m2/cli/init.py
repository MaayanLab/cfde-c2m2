from cfde_c2m2.cli import cli
import cfde_c2m2.cli.update
import cfde_c2m2.cli.upgrade
import cfde_c2m2.cli.simplify

def init():
  ''' Initialize a fresh CFDE C2M2 submission
  (update, upgrade, simplify)
  '''
  cfde_c2m2.cli.update.update()
  cfde_c2m2.cli.upgrade.upgrade()
  cfde_c2m2.cli.simplify.simplify()

cli.command()(init)
