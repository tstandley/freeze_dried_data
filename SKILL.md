---
name: freeze-dried-data
description: 'Work with freeze_dried_data (.fdd) dataset files: inspect columns/splits/properties, read rows (RFDD), write or append files (WFDD), define columns and custom serializers, make keyless/keyed splits, filter, add columns, convert to/from CSV/parquet, use in PyTorch DataLoaders, and fix ModuleNotFoundError from dill-pickled custom serializers. Use when the user mentions .fdd, FDD, RFDD, WFDD, freeze dried data, or freeze_dried_data.'
---

# Freeze Dried Data (FDD)

`freeze_dried_data` is a single-file dataset format with fast random access.
Verified against the installed 2.6.4 source on 2026-09-22, at
`/usr/local/lib/python3.12/dist-packages/freeze_dried_data/`. Upstream:
https://github.com/tstandley/freeze_dried_data. A (older, 2.5.0) local checkout
lives at `/persist/tstandley/freeze_dried_data/` — prefer the installed package.

Check the active interpreter's package version and module path before relying on
version-specific behavior; the checkout and installed package differ.

Full API notes: [references/api.md](./references/api.md).
Every upstream test example is covered in [test-examples.md](./references/test-examples.md).
For self-contained serializers, read [portable-codecs.md](./references/portable-codecs.md).
For deletion/filtering that preserves clusters, read
[rewrite-preserving-splits.md](./references/rewrite-preserving-splits.md).

## Mental model

- A file is a dict: `key -> row`. Keys are any picklable object (commonly int
  hashes / str). Rows are either a single pickled object (no columns) or a tuple
  of columns, each with its own (de)serializer.
- **Write-once**: `WFDD` appends cell bytes immediately; index/splits/props are
  written at `close()`. Reopen with `WFDD(path, reopen=True)` to append more.
- **Column reads are lazy and independent.** Obtaining `row = f[key]` loads
  row offsets, not every cell. Reading `row.img_hash` or `row['img_hash']`
  reads/deserializes only that column and caches its value; image bytes, features,
  and other columns are not loaded from disk. This makes hash-only split scans
  cheap even for image datasets. `as_dict()`, `repr(row)`, and consuming all
  values/items do access every column.
- **Splits** = named subsets of keys. `keyless=True` splits use an on-disk index on read and
  index rows by int `0..len-1`. Load via `RFDD(path, split=name)` or
  `RFDD('path.fdd^name')`. `'a+b'` = union. `'path.fdd$<expr>'` filters with
  `lambda r: <expr>`. `'a.fdd,b.fdd'` concatenates files.
- Custom properties = dataset metadata: `wfdd.anything = obj`, read as `rfdd.anything`.

## Procedure: inspect an unknown .fdd

1. Run the bundled script (adds the file's directory to `sys.path` so custom
   serializer modules resolve):
   ```bash
   python /root/.agents/skills/freeze-dried-data/scripts/fdd_inspect.py <file.fdd> [--split NAME] [--rows N] [--import-dir DIR]
   ```
2. If it fails with `ModuleNotFoundError: No module named 'X'`, the column_def
   holds dill-pickled `(serializer, deserializer)` functions from module `X`.
   Find it (`rg --files`), then `cd` to that dir or pass `--import-dir`.
   In your own code: `sys.path.insert(0, dir_containing_X)` before `RFDD(...)`.
3. Report: ordered columns + types, row count, split names, custom properties,
   and one decoded sample row (shape/dtype for tensors, length for bytes/str).

## Procedure: read

```python
import sys; sys.path.insert(0, '/dir/with/custom/serializer/module')  # only if needed
from freeze_dried_data import RFDD

with RFDD('data.fdd') as f:                # or RFDD('data.fdd^train'), RFDD('data.fdd', split='train')
    f.columns          # {'name': idx}  (None if column-less file)
    f.column_def       # {'name': 'str' | 'uint128' | (ser, deser)}
    f.get_available_splits(); f.custom_properties   # names; values via f.<name>
    len(f); key in f; list(f.keys())
    row = f[key]       # FDDReadRow, lazy
    row.caption; row['caption']; row[4]; row.as_dict()
    f[key, 'caption']  # read a single cell without building a row
    for key, row in f.items(): ...   # tqdm-friendly (has __len__)
    f.load_new_split('other')        # swap split in place
    f.filter(lambda r: r.label == 1) # in-memory subset; optional row_to_key
```
Keyless split: `RFDD('data.fdd^keyless')[0]` … `[len-1]`; `f.load_keys(lambda r: r.img_hash)`
restores keyed access.

## Copying an existing FDD

A faithful copy includes **all splits**, their membership and order, their
keyless/keyed behavior, custom properties, schema, and row data. Copying only
`all_rows` is not a complete copy; do not assume it covers every other split.
For an unchanged file copy, a filesystem byte copy preserves all of these.

For a rewrite, establish how every split's rows map to destination keys before
writing. Prefer a stable key column (for example `img_hash`). Keyless positions
are not original keys. A surviving keyed index can sometimes recover identity
through source byte offsets, but only for rows it covers; without either source
of identity, the original keyspace may be unrecoverable. Do not silently invent
keys or drop splits. See the rewrite reference for preconditions and validation.

## Procedure: write

```python
from freeze_dried_data import WFDD
cols = {'img_hash': 'uint128', 'img_bytes': 'bytes', 'caption': 'str'}
with WFDD('out.fdd', columns=cols, overwrite=True) as w:
    w[key] = {'img_hash': h, 'img_bytes': b, 'caption': c}   # dict, tuple, or obj with attrs
    w[key2].caption = 'x'; w[key2].finalize()   # partial rows; finalize() writes now
    w.make_split('train', train_keys, keyless=True)   # or pass a filter callable
    w.make_split('val', val_keys)                     # keyed; preserve_order=True default
    w.description = 'metadata'                        # custom property
```
- Built-in types: `any` (pickle), `str`, `str_compressed`, `bytes`, `int/int8..int128`,
  `uint/uint8..uint128`, `float`. Missing/None cell → zero bytes → reads back `None`.
- Custom `(ser, deser)` tuples are dilled at writer close. Top-level imported
  functions normally remain module references. For files readable without the
  author module, use nested codecs/helpers with recursive dill enabled through
  close; follow the tested portable-codecs reference. Do not assume a lambda or
  `recurse=True` alone embeds every dependency.
- Fast copy: `w[key] = rfdd_row` copies uncached cells byte-for-byte **by column
  position**, not name. Use identical column order and serializers. Cached values
  (even merely read ones) are reserialized. For a reordered/subset schema, pass an
  explicit dict. For **new trailing columns**, append values to the row cache
  to retain fast raw copying; see the tested rewrite reference. Assigning `None` to a cached row is not a reliable way to clear a
  cell in this copy path; use a dict for that operation.
- Append later: `WFDD(path, reopen=True)`; use `add_to_split` / `make_split(..., overwrite=True)`.
- **Do not use `add_column()`.** Add columns with an explicit rewrite instead,
  preserving splits and properties as described in the rewrite reference.
- Never mutate a finalized cell unless `allow_cell_modification=True` and byte size is identical.
- Keys must be unique; `w[key] = ...` on an existing key raises `KeyError`.

## Modifying an already-written FDD

Pick the cheapest operation that fits. All of these were verified on 2.6.4.

| Want to… | Use | Cost |
|---|---|---|
| Overwrite a cell, same byte size | `RFDD(p, allow_cell_modification=True)`; `r[k].col = v` | one seek+write; no trailer rewrite |
| Append rows / edit props / edit splits | `WFDD(p, reopen=True)` | loads all indices into RAM; rewrites only trailer |
| Both of the above | `WFDD(p, reopen=True, allow_cell_modification=True)` | same |
| Rename columns / swap decoder | `WFDD(p, reopen=True, columns={...same count/order...})` | trailer only; bytes reinterpreted |
| Logically delete rows | reopen + `make_split('all_rows', keep, overwrite=True)` | bytes stay on disk |
| Change cell size, add/drop/reorder columns, reclaim space | rewrite to a new file | full copy (fast raw byte copy for untouched cells) |

### Overwrite cells in place (the most useful trick)

```python
# Schema: name:str, score:float, n:int, v:any, b:bytes
with RFDD(path, allow_cell_modification=True) as r:
    r['k1'].score = 9.5           # 'float' is always 8 bytes -> always OK
    r['k1'].n = -7                # 'int'/'uintN' fixed width -> always OK
    r['k1'].name = 'dog1'         # OK only because len(b'cat1') == len(b'dog1')
    r['k1'].b = b'\xff' * 4       # fixed-size raw vector/embedding -> OK
    r['k1'].v = 200               # 'any' pickle: 100..255 same length; 256 is not
    setattr(r['k1'], 'n', 5)      # dynamic column name

# Keyless splits are editable too; the edit is visible through every split/key.
with RFDD(path + '^train_keyless', allow_cell_modification=True) as r:
    r[0].n = 1234

# Bulk: e.g. patch a label column for many rows.
with RFDD(path, allow_cell_modification=True) as r:
    for k, new_label in fixes.items():
        r[k].label = new_label    # label defined as 'int8'/'uint16'/etc.
```
- Only **attribute assignment** writes to disk. `r[k]['n'] = 55` changes the
  cache only (verified: disk kept the old value).
- The **serialized byte length** must match exactly. Mismatch → `ValueError`
  and nothing written, **but the row cache already holds the rejected value**
  (and RFDD caches the last row object, so `r[k]` returns it). Re-open or move to
  another key before trusting reads of that row.
- File size and all splits/props are untouched. No transaction/locking; don't
  edit while other processes read if consistency matters.
- Design for it: store mutable fields as `float`, `int*`, `uint*`, or fixed-size
  `bytes`; avoid `any`/`str`/`str_compressed` for fields you'll patch. Pad
  strings to fixed width if you must patch text.
- Cannot set an existing value to `None`/empty (that changes size to 0).
- Without the flag, `r[k].col = v` only updates the in-memory cache (used for
  modified copies). On a WFDD without the flag, `w[existing].col = v` raises
  `AttributeError: Row has already been finalized.`

### Reopen to append, edit metadata, and edit splits

```python
with WFDD(path, reopen=True, allow_cell_modification=True) as w:
    w['k3'].n = 333                                   # in-place cell edit
    w['new'] = {'name': 'new', 'n': 1}                # append rows
    w.add_to_split('val', ['new'])                    # extend a *keyed* split
    w.make_split('ev', ['k0', 'k2'], overwrite=True)  # replace a split
    w.make_split('odd', lambda row: row.n % 2 == 1)   # split from a predicate (reads every row)
    del w.split_to_index['old_split']                 # remove a split (internal dict, works)
    w.note2 = 'y'; del w.note                          # add/replace/remove properties
    w.make_split('all_rows', [k for k in w.keys() if k != 'bad'], overwrite=True)  # hide rows
```
- Existing rows, schema, splits, properties are retained; appended rows are
  **not** added to named splits automatically.
- Reopen loads every split index into RAM (keyless ones become in-RAM
  `FDDIndexKeyless`). `add_to_split` on a keyless split fails
  (`AttributeError: ... no attribute 'update'`); rebuild it with
  `make_split(..., keyless=True, overwrite=True)`.
- Replacing `all_rows` drops keys from the main index only; bytes remain, and
  other splits that reference those rows still see them. Replacing `all_rows`
  with a keyless split loses keyed access to the file.
- Duplicate keys still raise `KeyError`; you cannot replace a whole row. Write
  under a new key and re-point splits, or rewrite.

### Rename columns or reinterpret bytes (no data copy)

```python
# Same column count/order; new names and/or codecs compatible with existing bytes.
with WFDD(path, reopen=True, columns={'title': 'str', 'score': 'float', 'n': 'int'}):
    pass
```
The old bytes are decoded by the new definitions. Useful for renames, or
swapping a `(ser, deser)` pair for a portable one. Not a migration.

### When a rewrite is required

Copy with `w[k] = src_row`: uncached cells are copied as raw bytes (no
deserialization); cells you read or set are reserialized. See the rewrite
reference for preserving splits/props and for appending trailing columns fast.

## Procedure: PyTorch DataLoader

```python
class DS(torch.utils.data.Dataset):
    def __init__(self, path):
        self.f = RFDD(path + '^train')          # keyless split: ints 0..len-1, tiny RAM
    def __len__(self): return len(self.f)
    def __getitem__(self, i):
        row = self.f[i]
        return decode(row.img_bytes), row.label  # only these two cells are read
```
Keep the `RFDD` open in `__init__`; it re-opens its handle in forked workers
(`os.register_at_fork`), so `num_workers>0` works with the Linux default `fork`.
For keyed splits keep `self.keys = list(self.f.keys())`. Caveats (verified):
- An RFDD **with columns is not picklable with std pickle** (built-in codecs
  are lambdas), so `spawn`/`forkserver` multiprocessing contexts fail. Column-less
  files pickle fine; `dill` works. Or open the RFDD lazily inside the worker.
- `RFDDCombined` (`'a.fdd,b.fdd'`) cannot be pickled at all.
- After fork/unpickle the file is reopened `rb+`, which needs **write
  permission** on the file (read-only mounts break workers).

## Performance characteristics

- **Open**: reads 8-byte trailer length, the trailer dict, then only the chosen
  split's index. Keyless split → reads a 24-byte header; open is O(1) and
  instant. Keyed split → unpickles the whole index (dict of keys → slot).
- **Index RAM**: offsets are packed 6-byte ints, `(ncols+1)*6` bytes/row, plus
  the Python key objects for keyed indices. Keyless on-disk = ~0 RAM; each
  lookup is one extra seek+read of `(ncols+1)*6` bytes.
- `preserve_order=False` stores keys sorted in a list with binary-search lookup
  (less RAM than a dict; iteration is sorted). Falls back to ordered dict if keys
  aren't mutually comparable. Default `preserve_order=True` keeps insertion order.
- **Row access** is lazy per column; each cell = one seek+read+deserialize,
  cached on the row. RFDD also caches the last row object, so
  `r[k].a; r[k].b` reuses one row.
- `f[key, 'col']` reads one cell without building a row object.
- **Codecs**: `bytes`/`int*`/`float` are near-free; `str` is utf-8; `any` is
  pickle (slowest); `str_compressed` is zlib. Custom raw tensor codecs beat pickle.
- **Writes** stream cell bytes straight to disk; only the index is in RAM.
  Unfinished setters stay in RAM until finalized (warning at >1000 at close).
- Reading from a WFDD mid-write is supported (it restores the append position).
- O(N) operations that read every row: `make_split(name, callable)`,
  `load_keys`, `filter`, `'$expr'` paths, `+` unions of keyless splits (dedup by
  offsets, materialized in RAM).
- Custom properties are read lazily on first attribute access and cached.
- Cell overwrite with the flag is one seek+write: ideal for scores/labels.

## Workspace datasets

The classifier pool is `/persist/tstandley/classifier_agent/diverse_sorting/vlm_ds.fdd`.
As inspected in September 2026 it contains image bytes, VisMod and SigLIP features,
and both `kcluster_` and `scluster_` families. The neighboring
`vlm_ds.fdd.siglip.fdd` is an embedding-only file (`img_hash`, `siglip`), not an
image dataset. Inspect actual columns and splits rather than infer from filenames.
Serializer modules are importable from `/persist/tstandley/classifier_agent`.
Pool-backed project datasets may reference these hashes; a pool rewrite can
invalidate those references and positional sampler state.

## Gotchas

- Files are huge; never `list(f.items())` or load a column for every row unless
  needed. Iterate lazily and access only the columns you need.
- `open()` of `all_rows` for a keyed file deserializes the whole index (seconds
  and GBs for millions of keys). Prefer keyless splits when keys aren't needed.
- Some code prints `torch`/`pynvml` FutureWarnings on import of custom modules;
  harmless — filter with `-W ignore`.
- `'^'`, `'$'`, `','`, `'+'` are special in path strings passed to `RFDD`.

- RFDD is fork-aware, **not thread-safe**: `read_chunk` and on-disk index reads
  use shared `seek`/`read` without a lock. Give each thread its own reader or lock
  the whole lookup and cell read, not just creation of a lazy row.
- Keyless `.keys()` yields positions, not original hashes. Recover identity from
  a stable key column or an offset-to-original-key map before rebuilding splits.
- `filter()` changes the reader's in-memory index only; it does not remove bytes
  from disk. Replacing `all_rows` alone is not physical deletion either.
- `repr(row)` and `as_dict()` decode all cells; avoid them for large image rows.
- Empty serializations read back as `None`: `''` in a `str` column and `b''`
  in a `bytes` column both come back as `None`.
- `'$expr'` in paths is `eval`'d: never pass untrusted strings.
- Mixing keyed and keyless split types in one `a+b` union raises `ValueError`.
