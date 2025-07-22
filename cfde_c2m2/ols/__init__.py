import pathlib
import sys
import subprocess

def bulk_by_id(ontology: str, ids_file: pathlib.Path):
  ''' We run the request in another process so that we can achieve a bidirectional stream
  ids from ids_file stream to the API endpoint
  and the API endpoint streams results back to us which we yield on this iterator
  '''
  proc = subprocess.Popen([
    sys.executable,
    '-m',
    'cfde_c2m2.ols.bulk_by_id',
    ontology,
    str(ids_file),
  ], stdout=subprocess.PIPE, stderr=sys.stderr)
  try:
    columns = proc.stdout.readline().decode().rstrip('\r\n').split('\t')
    while True:
      line = proc.stdout.readline()
      if not line: break
      yield dict(zip(columns, line.decode().rstrip('\r\n').split('\t')))
  except:
    proc.kill()
    raise
  finally:
    proc.wait()
