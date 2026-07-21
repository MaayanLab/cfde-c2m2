''' Not meant to be used directly, use with cfde_c2m2.ols:bulk_by_id_hash
'''
import sys
import requests
from cfde_c2m2 import const, utils

# with this scheme packets will be sent ~1MB at a time
#  chunk size is not strict since we break it up on line boundaries
cs = 1<<20

if __name__ == '__main__':
  _, ontology = sys.argv
  for i, chunk in enumerate(utils.chunked_read_lines(sys.stdin.buffer, cs)):
    # try to retry in case of momentary service downtime during what is likely a long running process
    @utils.run_with_retry()
    def _(chunk=chunk):
      req = requests.post(
        f"{const.OLS_URL}/api/v1/bulk-by-id-hash/{ontology}",
        headers={
          'Content-Type': 'text/tsv',
          'Accept': 'text/tsv',
        },
        data=b''.join(chunk),
        stream=True,
      )
      req.raise_for_status()
      content = b''.join(req.iter_content(None))
      if i != 0:
        _,_,content = content.partition(b'\n')
      sys.stdout.buffer.write(content)
      sys.stdout.buffer.flush()
