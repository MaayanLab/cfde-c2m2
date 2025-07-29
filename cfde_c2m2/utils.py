import csv
import json
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

def maybe_json_dumps(v):
  if v is None:  return ''
  elif isinstance(v, str): return v
  else: return json.dumps(v)

def maybe_json_loads(v):
  if v == '': return None
  try: return json.loads(v)
  except: return v

class JsonDictReader(csv.DictReader):
  def __init__(self, f, fieldnames, dialect = "excel-tab", quotechar = None, lineterminator = "\n", quoting = csv.QUOTE_NONE, **kwargs):
    super().__init__(f, fieldnames, **dict(dialect=dialect, quotechar=quotechar, lineterminator=lineterminator, quoting=quoting, **kwargs))

  def __next__(self):
    rowdict = super().__next__()
    return { k: maybe_json_loads(v) for k, v in rowdict.items() }

class JsonDictWriter(csv.DictWriter):
  def __init__(self, f, fieldnames, dialect = "excel-tab", quotechar = None, lineterminator = "\n", quoting = csv.QUOTE_NONE, **kwargs):
    super().__init__(f, fieldnames, **dict(dialect=dialect, quotechar=quotechar, lineterminator=lineterminator, quoting=quoting, **kwargs))

  def writerow(self, rowdict):
    return super().writerow({ k: maybe_json_dumps(v) for k, v in rowdict.items() })
  def writerows(self, rowdicts):
    return super().writerows(({ k: maybe_json_dumps(v) for k, v in rowdict.items() } for rowdict in rowdicts))

@contextlib.contextmanager
def OpenDictWriter(file, *, fieldnames, writeheader=True, **kwargs):
  with open(file, 'w') as fw:
    writer = JsonDictWriter(fw, fieldnames=fieldnames, **kwargs)
    if writeheader: writer.writeheader()
    yield writer

@contextlib.contextmanager
def OpenDictReader(file, **kwargs):
  with open(file, 'r') as fr:
    fieldnames = fr.readline().rstrip('\r\n').split('\t')
    reader = JsonDictReader(fr, fieldnames=fieldnames, **kwargs)
    yield reader

@contextlib.contextmanager
def LazyDictWriters():
  import csv
  writers: dict[str, csv.DictWriter] = {}
  with contextlib.ExitStack() as stack:
    def EnsureDictWriter(file, *, fieldnames, **kwargs):
      if file not in writers:
        writers[file] = stack.enter_context(OpenDictWriter(file, fieldnames=fieldnames, **kwargs))
      return writers[file]
    yield EnsureDictWriter
