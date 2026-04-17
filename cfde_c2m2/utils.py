import csv
import time
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

def chunked_read_lines(readable, cs=8192):
  buf = b''
  while True:
    chunk = readable.read(cs)
    if not chunk: break
    buf += chunk
    if b'\n' in buf:
      buf_lines = buf.splitlines(keepends=True)
      *buf_lines, buf = buf_lines
      yield buf_lines
  if buf:
    yield buf.splitlines(keepends=True)

def chunked(it, cs=8192):
  buf = []
  for el in it:
    buf.append(el)
    if len(buf) >= cs:
      yield buf
      buf = []
  if buf:
    yield buf

def run_with_retry(maxretries=3):
  def wrapper(fn):
    backoff = 1.
    exceptions = []
    for _ in range(maxretries):
      try:
        return fn()
      except Exception as e:
        exceptions.append(e)
        time.sleep(backoff)
        backoff *= 2
    raise ExceptionGroup(f"Error despite {maxretries} retries", exceptions)
  return wrapper

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

class OpenDictWriter:
  def __init__(self, file, *, fieldnames, writeheader=True, **kwargs):
    self.file = file
    self.writeheader = writeheader
    self.fieldnames = fieldnames
    self.kwargs = kwargs
    self.open()

  def open(self):
    self.fw = open(self.file, 'w')
    self.writer = JsonDictWriter(self.fw, fieldnames=self.fieldnames, **self.kwargs)
    if self.writeheader: self.writer.writeheader()
    return self

  def close(self):
    self.fw.close()

  def __enter__(self):
    if self.fw.closed:
      self.open()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()
  
  def writerow(self, rowdict):
    return self.writer.writerow(rowdict)

  def writerows(self, rowdicts):
    return self.writer.writerows(rowdicts)

class OpenDictReader:
  def __init__(self, file, **kwargs):
    self.file = file
    self.kwargs = kwargs
    self.open()

  def open(self):
    self.fr = open(self.file, 'r')
    self.fieldnames = self.fr.readline().rstrip('\r\n').split('\t')
    self.reader = JsonDictReader(self.fr, fieldnames=self.fieldnames, **self.kwargs)
    return self

  def close(self):
    self.fr.close()

  def __enter__(self):
    if self.fr.closed:
      self.open()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  def __iter__(self):
    return self.reader.__iter__()

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
