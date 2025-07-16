from cfde_c2m2.cli import cli
import click

def submit():
  ''' (Coming soon) Submit your C2M2 submission to the CFDE Workbench
  '''
  raise click.ClickException('Coming soon!')

cli.command()(submit)
