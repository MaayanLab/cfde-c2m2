import csv
import pathlib
import contextlib

def sha256(file: pathlib.Path, chunk=8192):
  import hashlib
  hash = hashlib.sha256()
  with file.open('rb') as fr:
    while True:
      buf = fr.read(chunk)
      if not buf: break
      hash.update(buf)
  return hash.hexdigest()

def ensure_list(L):
  if isinstance(L, list): return L
  else: return [L]

def one(it):
  it = iter(it)
  el = next(it)
  try: next(it)
  except StopIteration: pass
  else: raise RuntimeError('Expected one got multiple')
  return el

@contextlib.contextmanager
def OpenDictWriter(file, *, writeheader=True, fieldnames, dialect='excel-tab', quoting=csv.QUOTE_NONE, quotechar=None, **kwargs):
  import csv
  with open(file, 'w') as fw:
    writer = csv.DictWriter(fw, fieldnames=fieldnames, dialect=dialect, quoting=quoting, quotechar=quotechar, **kwargs)
    if writeheader: writer.writeheader()
    yield writer

@contextlib.contextmanager
def OpenDictReader(file, *, dialect='excel-tab', quoting=csv.QUOTE_NONE, quotechar=None, **kwargs):
  import csv
  with open(file, 'r') as fr:
    fieldnames = fr.readline().rstrip().split('\t')
    reader = csv.DictReader(fr, fieldnames=fieldnames, dialect=dialect, quoting=quoting, quotechar=quotechar, **kwargs)
    yield reader

@contextlib.contextmanager
def LazyDictWriters():
  import csv
  writers: dict[str, csv.DictWriter] = {}
  with contextlib.ExitStack() as stack:
    def EnsureDictWriter(file, *, fieldnames, dialect='excel-tab', quoting=csv.QUOTE_NONE, quotechar=None, **kwargs):
      if file not in writers:
        writers[file] = stack.enter_context(OpenDictWriter(file, fieldnames=fieldnames, dialect=dialect, quoting=quoting, quotechar=quotechar, **kwargs))
      return writers[file]
    yield EnsureDictWriter
