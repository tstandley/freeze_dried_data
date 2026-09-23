# Rewrite while preserving splits and clusters

Verified against the installed freeze_dried_data 2.6.4 source. Use a separate
output file: `filter()` changes an in-memory index, and replacing split indices
does not physically remove discarded row bytes. Do not use `add_column()`;
add columns via an explicit rewrite as well.

For an unchanged copy, use a filesystem byte copy; it retains every split and
all data without needing to reconstruct the keyspace. The guidance below is
for rewrites that filter rows or change columns.

## Identity and order

A keyless split contains byte offsets, not original keys. Its `.keys()` is
`range(len(split))`. Never feed those positions into a writer keyed by image
hashes. Recover keys using a stable column such as `img_hash`. Without a stable
column, build a mapping from source row-offset tuples to original keys from the
original keyed `all_rows` index. Raw offsets are meaningful only within the
same source file. Verify that the keyed index covers every referenced row and
that its identity mapping is unambiguous. A split named `all_rows` can have been
replaced by a subset, so its name is not evidence of coverage. If all surviving
indices are keyless and no column stores the original key, original keys cannot
be recovered from positions alone. Stop and explain the missing identity rather
than silently dropping splits or assigning substitute keys. Synthetic positions
can preserve membership in a deliberately rekeyed output, but that is not a
faithful preservation of the original keyspace.

Preserve each split's name, keyless/keyed behavior, and order of surviving rows.
For keyless splits, positions necessarily shift after deletion. Preserve empty
splits too. Do not recluster or sort the membership list. With `make_split`,
`preserve_order=False` can sort comparable keys; use `True` when order matters.

## Same-schema filtering recipe

Preconditions: `all_rows` is keyed; `key_column` uniquely matches those keys;
all rows referenced by splits are present in `all_rows`. This example targets
column-based datasets. Keep the original files until output validation and any
application reference updates are complete.

```python
from freeze_dried_data import RFDD, WFDD
from freeze_dried_data.efficient_index import FDDOnDiskIndex, FDDIndexKeyless


def rewrite_filtered(source, destination, excluded, key_column='img_hash'):
    with RFDD(str(source)) as src:
        if isinstance(src.index, (FDDOnDiskIndex, FDDIndexKeyless)):
            raise ValueError('This recipe requires keyed all_rows')
        with WFDD(str(destination), columns=src.column_def) as dst:
            for key, row in src.items():
                if key not in excluded:
                    dst[key] = row  # same schema, lazy raw-copy path
            for name in src.custom_properties:
                setattr(dst, name, getattr(src, name))
            for split in src.get_available_splits():
                if split == 'all_rows':
                    continue  # already follows source iteration order
                src.load_new_split(split)
                keyless = isinstance(src.index, (FDDOnDiskIndex, FDDIndexKeyless))
                def retained_keys():
                    for position_or_key, row in src.items():
                        key = row[key_column] if keyless else position_or_key
                        if key not in excluded:
                            yield key
                dst.make_split(split, retained_keys(), keyless=keyless,
                               preserve_order=True)
```

## Fast append of trailing columns

In 2.6.4, the writer examines `row._fdd_row_cache[i]` first. A non-None
cached value is serialized with the **destination** column serializer; otherwise
it copies the source byte span. Therefore append the new cell to the cache and
write the row directly. Existing uncached cells stay raw, with no decoding.
Keep the original columns, order, and serializers as an unchanged prefix.

```python
with RFDD(str(source)) as src:
    schema = {**src.column_def, 'score': 'float'}  # score must be a NEW name
    with WFDD(str(destination), columns=schema) as dst:
        for key, row in src.items():
            value = scores.get(key)
            row._fdd_row_cache.append(value)
            # None is the uncached sentinel. Give the new cell an empty source
            # span so missing values copy zero bytes instead of indexing past
            # the original offsets. Optional when every new value is non-None.
            row._fdd_row_index = (*row._fdd_row_index, row._fdd_row_index[-1])
            dst[key] = row
        # Copy custom properties and rebuild splits using the recipe above.
```

Extend the cache and offsets once per appended column, in destination order.
Do not repeatedly append to the same cached row object. Access new columns on
the output reader; the source reader's column-name map is unchanged. Merely
reading existing cells populates their cache and causes reserialization, so
avoid accessing heavy columns before copying.

This uses private internals: recheck the writer on version changes. It has been
tested with a source deserializer that raises if called, byte-for-byte checks of
old cells, and both non-null and missing new values. Reordering/subsetting old
columns is a different operation: use an explicit dict or a separately tested
mapping of caches and source offsets.

## Validate before replacing a source

Check output schema/serializers, row identities, missing cells, and properties.
Compare each output split against the source's original key sequence filtered
by the exclusion set, including empty splits and both cluster families. Compare
raw cell bytes where byte preservation matters. Use a fresh process to verify
custom serializers can be imported.

Properties copy verbatim, so derived row counts, centroids, or statistics can
become stale; decide which to recompute based on their meaning. Downstream
files referencing removed hashes, and sampler state storing split positions,
need explicit handling before swapping in a filtered pool. Existing open readers
must be reopened after replacement.

Inspect a source only to the extent needed; do not load image bytes to recover
membership when a small stable hash column or index offsets suffice.
