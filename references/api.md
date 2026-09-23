# freeze_dried_data API reference (v2.6.4)

Source: `/usr/local/lib/python3.12/dist-packages/freeze_dried_data/freeze_dried_data.py`
and `efficient_index.py`. Read them when something here is insufficient.

## File layout (on disk)

```
[cell bytes for row 1 col 1][col 2]...[row 2]...      <- appended as rows are set
[column_def (pickle, or dill if custom fns)]
[custom property values (pickle each)]
[split indices: b'\x01' + packed ints  (keyless) | pickled index object (keyed)]
[columns tuple (pickle)]
[index_index: {'_column_def_':(s,e), '_prop_X':(s,e), '_split_X':(s,e), '_columns_':(s,e)}]
[8-byte little-endian length of index_index]
```
Reader seeks to the end, reads the trailer, and only then reads the chosen split's index.
Each row index entry is a tuple of `len(columns)+1` byte offsets; cell i spans
`offsets[i]:offsets[i+1]`; an empty span means `None`.

## `RFDD(filename, split='all_rows', allow_cell_modification=False, system_deserialize=pkl.loads)`

Factory: returns `RFDDImpl`, or `RFDDCombined` if `filename` contains `,`.
Path grammar: `file.fdd[^split[+split2...][$<python expr on r>]]` (`,` joins files).
The `$` filter is parsed only inside the `^` split spec: `x.fdd^all_rows$r.label==1`.

Attributes / methods (RFDDImpl):
- `columns` — `dict name -> position` (None if column-less)
- `column_def` — `dict name -> type str | (ser, deser)`
- `column_to_deserialize` / `column_to_serialize` — tuples by position
- `custom_properties` — `dict name -> (start,end)`; value via attribute access `f.name` (cached)
- `get_available_splits()` — list of names (includes `'all_rows'`)
- `load_new_split(spec)` — replaces `f.index`; supports `+` and `$` grammar
- `load_keys(row_to_key, filter_function=lambda x: True)` — rebuilds index (in RAM) with computed keys; warns on duplicate keys
- `filter(filter_function, row_to_key=None)` — same as `load_keys(row_to_key, filter_function)`
- `__getitem__(key)` → `FDDReadRow` (or raw object if no columns); `f[key, 'col']` → single decoded cell
- `keys()`, `items()` (len-aware), `__iter__` yields `(key, row)`, `__len__`, `__contains__`
- `read_chunk(start, end)` → bytes
- `index` — one of `FDDIndexGeneral` (dict-like, any keys), `FDDIndexComparableKey` (sorted keys),
  `FDDIndexKeyless` (in-RAM int rows), `FDDOnDiskIndex` (keyless split read lazily from disk)
- Fork-safe (`os.register_at_fork`; reopens `rb+`, so needs write permission).
  `__getstate__` drops the file handle, but column files hold lambda codecs, so
  std `pickle` fails (use fork, `dill`, or open per worker). Column-less files pickle.
- Caches the last `FDDReadRow` (`read_row_cache`); repeated `f[k]` returns the same object.

`RFDDCombined`: same surface over several files. If all are keyless, int index spans them
in order; otherwise keys are looked up in each file in turn.

## `FDDReadRow`

Lazy per-cell cache. `row.col`, `row['col']`, `row[i]`, `row.as_dict()`, `row.items()`,
`row.keys()`, `row.values()`, `'col' in row`, `repr(row)` (decodes all columns!).
Assigning `row.col = v` only changes the in-memory cache (used for modified copies)
unless parent has `allow_cell_modification=True`, in which case it writes in place and
requires identical serialized byte length.

## `WFDD(filename, columns=None, overwrite=False, reopen=False, allow_cell_modification=False, system_serialize=pkl.dumps, system_deserialize=pkl.loads)`

- Raises `FileExistsError` if the file exists and neither `overwrite` nor `reopen`.
- `columns`: `{'name': type_str | (serialize_fn, deserialize_fn)}` — order matters.
- `w[key] = value`:
  - no columns: any picklable value
  - dict (subset of columns OK; missing → None), tuple (must be full length),
    object with matching attributes, or an `FDDReadRow` (positional byte copy of uncached cells; requires identical schema)
  - `KeyError` if key already exists
- `w[key].col = v` → `FDDSetter`; auto-finalizes when all columns set; `.finalize()` to
  write early. Unfinalized setters are flushed at `close()` (warning if >1000 pending).
- `make_split(name, rows, overwrite=False, keyless=False, preserve_order=True)`;
  `rows` may be an iterable of keys or a callable `row -> bool` filter over all rows.
  `add_split` is an alias. `make_split('all_rows', ...)` replaces the main index.
- `add_to_split(name, keys)` — keyed splits only.
- `w.prop = value` → custom property (anything not already an attribute). `del w.prop` removes.
- `close()` — flushes setters, writes column_def (dill if any custom fn), props, splits,
  columns, trailer; truncates file. Always use `with` or call `close()`.
- `reopen=True` — reads existing trailer, loads index/splits/props into RAM, seeks to the
  end of row data, so new rows append and the trailer is rewritten on close.

## Do not use `add_column()`

Use an explicit rewrite when adding columns. Implementation notes explaining
why the helper is avoided:

`column_data` is `{key: value}` or an iterable of `(key, value)`. In 2.6.4, only
those supplied keys are written. Split copying then uses each source split's
`.keys()` without recovering keyless identities or preserving keyless type.
This can fail or silently select the wrong rows. Only built-in `column_type`
strings work. Use an explicit rewrite for cluster-bearing datasets.

## Built-in column types

| type | bytes | notes |
|---|---|---|
| `any` | pickle | default; slowest, most flexible |
| `str` | utf-8 | |
| `str_compressed` | zlib(utf-8) | |
| `bytes` | raw | images, audio, npy, etc. |
| `int`, `int64` | 8 LE signed | also `int8/16/32/128` |
| `uint`, `uint64` | 8 LE unsigned | also `uint8/16/32/128` (`uint128` fits an md5/uuid) |
| `float` | 8 (`struct 'd'`) | |

Custom `(ser, deser)`: `ser(obj) -> bytes`, `deser(bytes) -> obj`. Stored via dill; module-level functions normally require their defining module
importable under the same name.
For embedding codec implementations and their project helpers, see
[portable-codecs.md](portable-codecs.md): nested functions plus recurse=True at
close, validated without the author module in a fresh isolated interpreter.
Typical tensor pattern:
```python
def t_to_bytes(t):  return t.half().cpu().numpy().tobytes()
def bytes_to_t(b):  return torch.from_numpy(np.frombuffer(b, dtype=np.float16).copy())
```

## Common recipes

For a same-schema copy, `w[k] = row` transfers uncached cells as raw bytes
by position. Accessed cells are cached and reserialized, even if unchanged.
Appending a new trailing cell to `row._fdd_row_cache` supports fast column
addition without decoding existing cells. Pad the source offsets with an empty
span for a None new value; see the rewrite reference for tested code.
For column subsets/reordering, pass `{name: row[name] for name in new_schema}`;
passing the row object directly is unsafe. Clearing a cell to `None` also needs
an explicit dict because `None` is the cache's sentinel in the row-copy path.

See [rewrite-preserving-splits.md](rewrite-preserving-splits.md) for a tested
filtering recipe that recovers keyless identities and copies properties.

Export to CSV/parquet (skip heavy columns):
```python
import pandas as pd
with RFDD(path) as r:
    df = pd.DataFrame({'key': k, 'caption': row.caption} for k, row in r.items())
```

Decode an image cell: `PIL.Image.open(io.BytesIO(row.img_bytes))`.

Split arithmetic on the command line: `RFDD('x.fdd^train+val$r.label==1')`.

Torch Dataset:
```python
class DS(torch.utils.data.Dataset):
    def __init__(self, path, split): self.f = RFDD(path, split=split)
    def __len__(self): return len(self.f)
    def __getitem__(self, i): r = self.f[i]; return decode(r.img_bytes), r.embedding
```
(keyless split → `i` is an int; keyed split → keep `self.keys = list(self.f.keys())`).

## Fast reopening and fixed-size cell edits (verified 2.6.4)

`WFDD(path, reopen=True)` opens the existing file with `rb+`, restores its schema,
all split indices and properties, and seeks to the start of the old trailer.
New row bytes append there; `close()` rewrites the trailer and truncates the file.
It does not copy the existing row data. Use it to append rows or update splits
and properties. Existing named splits do not automatically gain appended rows;
update their membership explicitly. Do not use `overwrite=True` for this task.
Reopening loads split indices into RAM, including keyless indices.

`RFDD(path, allow_cell_modification=True)` opens `rb+`. Assign through a row:
`r[key].column = value` (or `setattr(r[key], column_name, value)`). It serializes the new
value, checks its byte length against the existing span, seeks to that span,
writes it and restores the previous file position. No index/trailer rebuild or
whole-file copy is needed. Every split referencing those bytes sees the edit.
Other already cached reader objects may still hold the old value.

`WFDD(path, reopen=True, allow_cell_modification=True)` permits the same edits
while also supporting appends and metadata changes. Replace an existing cell
through `w[key].column`, not `w[key] = whole_row` (duplicate keys still fail).

Length mismatch raises `ValueError` before modifying that cell on disk. However,
the setter updates its row cache *before* checking length, so after a failed
assignment do not trust that row's cached value: close/reopen the reader or
explicitly invalidate the affected cache entry. Multiple cell edits are not a
transaction. Use these operations without competing readers/writers when
consistency matters; the shared seek/read/write file handle has no locking.

The required equality is serialized bytes: UTF-8 character count is not enough,
pickle integer encodings can change length, and compressed data size is variable.
Fixed-width scalar serializers and fixed-length raw feature arrays are natural
fits. Null/empty cells require checking the actual serializer; setting None is
not a general-purpose way to erase arbitrary existing bytes.

See [test-examples.md](test-examples.md) for every upstream test scenario,
including reopening with renamed/reinterpreted columns, combined-file views,
custom system codecs, index variants, and legacy-test limitations.

In 2.6.4 `FDDReadRow.__setitem__` only modifies the cache, even with
allow_cell_modification=True. Use attribute assignment or setattr for persistent
in-place changes. This does not describe unfinished FDDSetter objects, whose
attribute and item assignments both participate in row construction.
