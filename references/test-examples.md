# Examples covered by the FDD tests

Source review: installed freeze_dried_data 2.6.4, 2026-09-22. This reference accounts for every test method definition, including the shadowed duplicate and legacy examples. Descriptions report what each test exercises; they do not claim the whole suite was run.

Use [api.md](api.md) for API details and [rewrite-preserving-splits.md](rewrite-preserving-splits.md) for complete-copy and fast-column-append recipes.

## test_freeze_dried_data.py

### `test_basic_operations_no_columns` (line 32)

Write strings and integers with `WFDD(path)` using `w[key] = value`; reopen with RFDD for dictionary access. No column schema means pickle-per-row, not keyless indexing.

### `test_basic_operations_with_columns` (line 44)

Declare `name:str, area:any, price:any`; write dicts or a full ordered tuple. Read by `r[k].name`, `r[k]['name']`, `r[k][0]`, or `r[k, 'name']`. Check `k in r` and iterate `r.items()`. Printing rows decodes their cells.

### `test_in_behavior` (line 107)

`for key, row in r` yields pairs, not bare keys. `row.as_dict()` materializes a complete dict while the reader is open.

### `test_file_exists` (line 127)

Creating WFDD on an existing file without overwrite/reopen raises FileExistsError.

### `test_file_does_not_exist` (line 137)

RFDD on a missing path raises FileNotFoundError.

### `test_modify_and_write` (line 142)

Read a row, change `row.price *= 1.5` in its cache, and assign it to a same-schema destination under a new key. Original source bytes stay unchanged; reproduce splits separately.

### `test_modify_and_write_use_indices` (line 165)

The same copy-and-edit workflow uses `row[2] = row[2] * 1.5`. Item assignment changes the cache; the destination writer serializes that changed value.

### `test_custom_attributes` (line 189)

Writer attributes persist metadata: set, overwrite, read, and delete a property. Reader property values are cached; assigning/deleting reader attributes is local, not a persistent metadata update. Use a reopened writer for persistence.

### `test_incomplete_columns` (line 225)

A dict may omit schema columns; omitted cells read as None. A tuple must still have the full column count.

### `test_single_column_functionality` (line 242)

A single-column schema accepts `w[k].value = x`, `w[k]['value'] = x`, or `w[k] = (x,)`. Completing a setter finalizes it. Reader row edits without modification enabled remain cached only.

### `test_set_all_of_column` (line 259)

Fill per-row setters, then read/copy each row to another file while multiplying one column; other columns retain their values. This is a rewrite, not a vectorized in-place column update.

### `test_set_columns_by_attribute_or_item` (line 278)

For unfinished WFDD setters, attribute and item assignment both fill named cells and auto-finalize when all columns are supplied. This differs from item assignment on an already finalized FDDReadRow.

### `test_out_of_order_writes` (line 300)

Fill the first column across 100 rows, then the second across 50. Close finalizes pending rows; the unfilled second cells read as None.

### `test_multiple_datatypes` (line 316)

Column-less records accept int, float, string, list, and dict values, round-tripped via pickle.

### `test_overwrite_existing_file` (line 334)

`overwrite=True` replaces the whole file; old keys disappear. It is not append or update mode.

### `test_items_has_len` (line 350)

Both `w.items()` and `w.keys()` support len, useful for progress reporting without materializing rows.

### `test_splits` (line 362)

Create odd/even, threshold, and reverse-order splits from key lists. Open using `split=...` or `path^name`; switch with `load_new_split`. Duplicate split names require overwrite=True; passing a string as rows is invalid; loading an unknown split raises KeyError.

### `test_splits_with_callable` (line 413)

`make_split(name, lambda row: condition)` filters existing rows, e.g. parity of area or a size threshold. Explicit reverse-order lists preserve order. Duplicate names and string rows are rejected as above.

### `test_split_operations` (line 465)

Open `odds+evens`, `odds+big houses`, three-way unions, or reversed operand orders. Overlapping rows appear once (50 odd plus 20 big rows yields 60). The test exercises keyless unions; it does not assert original member ordering, so do not use unions to preserve cluster order.

### `test_large_data_handling` (line 541)

Write and verify 1,000 column-less keyed rows; iterate/check keys without needing to decode unrelated cells.

### `test_reads_in_between_writes` (line 554)

Read any already-written column-less row from the same WFDD while appending more rows; the writer restores its append position.

### `test_column_not_found` (line 569)

Unknown attribute access/assignment raises AttributeError on setters/read rows; tuples with too many columns raise ValueError.

### `test_print_finalize_warning` (line 588)

Closing with 1,100 incomplete setters emits UserWarning but still writes them, filling absent cells with None. Explicitly finalize large batches to avoid retaining all pending rows.

### `test_key_not_found` (line 603)

Reading an absent key from a column-less writer raises KeyError; a column-mode writer can instead create an unfinished setter for a new key.

### `test_key_already_exists` (line 609)

Whole-row assignment to an existing key raises KeyError. Reopening does not make duplicate-key replacement valid.

### `test_invalid_object_for_column_mode` (line 615)

A scalar such as 15 is invalid for a multi-column schema, but valid in a column-less file.

### `test_object_set_for_column_mode` (line 627)

An object with attributes matching every schema column is accepted; unrelated objects and dicts with no schema fields are rejected.

### `test_reaads_in_between_writes_with_columns` (line 653)

Read already-written row attributes from a WFDD while continuing column-mode appends; validate persisted values after close. Source test name contains the typo `reaads`.

### `test_empty_file` (line 672)

An empty, closed WFDD is a readable file with zero rows; missing-key reads raise KeyError.

### `test_single_record` (line 681)

A one-record column-less file round-trips normally.

### `test_nonexistent_key` (line 687)

Missing-key reads on an otherwise populated RFDD raise KeyError.

### `test_custom_serialization_deserialization` (line 696)

Exercises five paired system codecs: pickle plus newline, zlib/pickle, bz2/pickle, gzip/pickle, and JSON with pickle fallback for FDDIndexBase. Match writer system_serialize with reader system_deserialize. Repeats column-less writes; mixes built-in employee fields with a custom position column; uses custom tensor-column codecs with default system serialization; finally round-trips raw 10x10 CPU bfloat16 tensors via contiguous cloned storage/ctypes and torch.frombuffer(bytearray(...)). Shape and dtype are supplied out of band. These are examples, not a requirement to use ctypes or GPU pointers.

### `test_row_has_already_been_finalized` (line 815)

After a setter receives every column, ordinary writer attribute assignment to that finalized row raises AttributeError. Use allow_cell_modification=True only for same-size serialized replacements.

### `test_reopen_file` (line 822)

Append a second 1,000-row batch using reopen=True; overwrite one metadata value, retain another, add a third; extend an existing keyed evens split and create odds. Missing files fail; unknown split names and scalar add_to_split arguments fail.

### `test_reopen_file_with_columns` (line 881)

Append using a replacement schema with renamed columns in the same order, then reopen again to change a column decoder (existing name bytes decoded as an integer suffix). Existing bytes are reinterpreted, not converted. Preserve column count/order and compatible codecs. Also replaces named splits and all_rows with keyless callable-built indices. See the schema reinterpretation example below.

### `test_cell_modificaiton` (line 991)

Uses RFDD(..., allow_cell_modification=True) to change area 100 to 99, and WFDD(..., reopen=True, allow_cell_modification=True) to change 200 to 199. Both use attribute assignment and equal pickle byte lengths. Source test name contains `modificaiton`; our added validation checks persistence, failure on length mismatch, and untouched split membership.

### `test_reopen_file_without_columns` (line 1012)

Despite its name, this test creates a column schema. It reopens WITHOUT passing columns, inherits the existing schema, appends rows, edits properties, extends a keyed split, creates keyless odds, and reads values via row.dict().

### `test_columns_with_partial_dicts` (line 1065)

Missing dict fields become None for each row; explicitly supplied fields retain their values.

### `test_columns_with_dicts_with_extra_keys` (line 1078)

A dict containing any field outside the schema raises ValueError.

### `test_as_dict` (line 1086)

`row.as_dict()` returns all schema fields and decoded values; it eagerly accesses every cell.

### `test_read_row_features` (line 1098)

Rows support `'name' in row`, items(), keys(), values(), and iteration over column names. items()/values() decode values.

### `test_alternative_indices` (line 1120)

Tests all four keyless/preserve_order flag combinations on normal, reversed, and all_rows splits. Ordered keyed splits retain supplied keys/order; keyless splits expose integer positions; comparable-key mode may sort. Duplicate split creation and string rows fail. Replacing all_rows can discard the original keyed view.

### `test_add_column` (line 1209)

Historical helper test supplies all keys, adds price, checks a keyed evens split and metadata, and rejects an already-existing column name. It does not cover keyless split preservation. DO NOT USE add_column(); use the cache-append rewrite and explicit split preservation instead.

### `test_load_keys` (line 1242)

Convert a keyless all_rows view back to named keys with `load_keys(lambda row: row.name)`. This is the first definition of test_load_keys; Python shadows it with the later definition, so normal unittest discovery does not execute it.

### `test_filter` (line 1264)

Starting from keyless all_rows, `r.filter(lambda row: (row.area//10)%2 == 0)` retains the matching rows in memory. It does not physically delete source data or persist a split.

### `test_filter_and_load_keys` (line 1287)

`load_keys(row_to_key=lambda row: row.name, filter_function=predicate)` filters and restores meaningful keys in one traversal.

### `test_filter_string` (line 1309)

`RFDD(path + '^all_rows$(r.area//10)%2==0')` builds the same filtered view via expression syntax. Use only trusted expressions.

### `test_load_keys` (line 1326)

The second definition actually tests combined files: `RFDD(a+','+b)` merges disjoint keyed files; `a^keyless,b^keyless` presents a contiguous positional range across files; mixing keyed and keyless files permits both string keys and integer positions. The two files contain alternate even/odd records. No key restoration is performed by this test despite its name.

### `test_dataloader_integration` (line 1369)

A Dataset keeps RFDD open and a list of keyed row IDs; __getitem__(integer) resolves through that list. The test iterates 100,000 column-less records with batch_size=10, num_workers=8, shuffle=True, then closes the reader.

### `test_dataloader_integration_with_columns` (line 1398)

Same multiworker Dataset pattern, returning area and price fields. Default collation yields two tensors of shape (10,). Keyless splits can avoid the explicit key list when no original-key lookup is needed.

## test_efficient_index.py

The test uses a bare efficient_index import; adapt sys.path when running it independently.

### `test_fdd_int_list_getitem` (line 12)

Packed integer indexing and out-of-range IndexError. The fixture supplies
5-byte values; the installed copy constructs the default 6-byte reader and
fails. The local checkout's test passes `byte_width=5`.

### `test_fdd_index_keyless_setitem_getitem` (line 19)

Append offset tuple [1,2,3] at position 0; read its components; missing position 1 raises IndexError.

### `test_fdd_index_comparable_key_getitem` (line 27)

Map sorted comparable keys 10 and 20 to offset tuples; missing 30 raises KeyError.

### `test_fdd_index_general_setitem_getitem` (line 37)

Map arbitrary key "a" to a three-offset tuple; missing "b" raises KeyError; a two-offset replacement raises ValueError.

## test_freeze_dried_data_old.py

Historical FDD API only. The old test imports FDD from the modern module, which no longer defines that class; these are not modern RFDD/WFDD invocation recipes. The base cases are repeated for zlib, bz2, gzip, none, and a custom trailing-space codec.

### `test_basic_operations` (line 20)

Legacy FDD writes/reads string and integer rows using write_or_overwrite and compression.

### `test_multiple_datatypes` (line 29)

Round-trips ints, floats, strings, lists, and dicts.

### `test_overwrite_existing_file` (line 44)

Replaces an existing file; prior keys raise KeyError.

### `test_update_operation` (line 54)

Legacy update(dict) inserts several keyed values.

### `test_large_data_handling` (line 64)

Legacy update writes 1,000 records and verifies length and values.

### `test_empty_file` (line 76)

Empty file has length zero; absent keys raise KeyError.

### `test_nonexistent_key` (line 86)

Absent key raises KeyError in a populated file.

### `test_custom_attributes` (line 95)

Set/replace metadata, then read metadata and iterate rows using read_only=True.

### `test_delete_custom_attributes` (line 119)

Deleting writer metadata persists; access later raises AttributeError.

### `test_custom_properties_read_only_mode` (line 137)

Legacy read_only mode rejects setting existing or new metadata with ValueError; do not assume this behavior describes modern RFDD local caches.

### `test_dataloader` (line 157)

A Dataset opens legacy FDD read_only, caches its index keys, and feeds batches of 10 from 100,000 records to eight workers with shuffling.

## Reopening to reinterpret the schema

The column-reopen test also illustrates this fast operation, without rewriting
row bytes:

```python
# Existing file has one str column whose values are 'name0', 'name1', ...
def suffix_codec():
    def encode(value):
        return ('name' + str(value)).encode('utf-8')
    def decode(data):
        return int(data.decode('utf-8')[4:])
    return encode, decode

import dill
previous = dill.settings['recurse']
try:
    dill.settings['recurse'] = True
    with WFDD(path, reopen=True, columns={'number': suffix_codec()}) as writer:
        pass
finally:
    dill.settings['recurse'] = previous
# Existing column bytes now decode through the new function; indices unchanged.
```

Supply the same column count and order and codecs compatible with existing bytes.
This can rename columns or change their interpretation; it is not an arbitrary
schema migration or an added-column operation. The upstream test uses a
read-focused identity serializer; the example above supplies a byte-producing
serializer that can also encode new rows.

## Additional skill-validation examples

These checks were run on miniature temporary files during this skill update:

- Same-schema filtering: retain keys 101/309, remove 205; preserve a keyless
  cluster's reversed order, a keyed split, an empty-after-filter split, metadata,
  and old cell bytes. Documented in rewrite-preserving-splits.md.
- Fast trailing-column append: source deserializer deliberately raises if called;
  uncached payloads copy byte-for-byte, new score 0.75 round-trips, missing score
  uses an empty appended source span and reads None.
- In-place modification: attribute-assigned string cat→dog and int64 1→999
  persist with unchanged file size and split membership. A longer string raises;
  reopening verifies the prior disk value. Bracket assignment was observed to
  remain cache-only and is not documented as a disk-edit method.
- Writer reopen: modify an existing fixed-width cell, append another row,
  replace metadata and add a split; original split membership survives.
- Codec portability: imported top-level functions fail without their author
  module even with recursive dill; nested functions and captured helpers succeed
  with recurse=True. Default recurse=False was also checked and failed the
  module-independent load. See portable-codecs.md.
- Inspection helper: run it against a miniature filtered FDD and check columns,
  split names, metadata names, and sampled row output.
