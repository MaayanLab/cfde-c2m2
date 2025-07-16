from cfde_c2m2.cli import cli
import pathlib, importlib

dirname = (pathlib.Path(__file__).parent)
package_dirname = dirname.parent
for f in (dirname/'cli').glob('**/*.py'):
  import_path = '.'.join(
    f.relative_to(package_dirname).with_suffix('').parts
  )
  importlib.import_module(import_path)

if __name__ == '__main__':
  cli()
