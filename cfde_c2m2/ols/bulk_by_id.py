''' Not meant to be used directly, use with cfde_c2m2.ols:bulk_by_id
'''
import sys
import requests
from cfde_c2m2 import const

if __name__ == '__main__':
  _, ontology = sys.argv
  req = requests.post(
    f"{const.OLS_URL}/api/v1/bulk-by-id/{ontology}",
    headers={
      'Content-Type': 'text/tsv',
      'Accept': 'text/tsv',
    },
    data=sys.stdin.buffer,
    stream=True,
  )
  req.raise_for_status()
  for chunk in req.iter_content(512):
    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
