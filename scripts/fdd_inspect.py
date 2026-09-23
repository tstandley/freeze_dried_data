#!/usr/bin/env python3
"""Inspect a freeze_dried_data (.fdd) file: columns, types, splits, properties, sample row.

Usage:
    python fdd_inspect.py <path.fdd> [--split NAME] [--rows N] [--no-sample]
                          [--import-dir DIR]

If the column_def uses custom (serializer, deserializer) functions, the module
that defined them must be importable (dill stores them by module reference).
By default the .fdd's own directory is added to sys.path; use --import-dir to
add more.
"""
import argparse
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")


def describe(v, maxlen=120):
    t = type(v).__name__
    shape = getattr(v, "shape", None)
    dtype = getattr(v, "dtype", None)
    if shape is not None:
        return f"{t} shape={tuple(shape)} dtype={dtype}"
    if isinstance(v, (bytes, bytearray)):
        return f"{t} len={len(v)} head={v[:8]!r}"
    if isinstance(v, str):
        s = v if len(v) <= maxlen else v[:maxlen] + "..."
        return f"str len={len(v)} {s!r}"
    if isinstance(v, dict):
        inner = {k: describe(x, 40) for k, x in list(v.items())[:6]}
        more = "" if len(v) <= 6 else f" ... (+{len(v) - 6} more)"
        return f"dict len={len(v)} {inner}{more}"
    if isinstance(v, (list, tuple)):
        return f"{t} len={len(v)} first={describe(v[0], 40) if v else None}"
    r = repr(v)
    return f"{t} {r if len(r) <= maxlen else r[:maxlen] + '...'}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--split", default="all_rows")
    ap.add_argument("--rows", type=int, default=1, help="sample rows to print")
    ap.add_argument("--no-sample", action="store_true")
    ap.add_argument("--import-dir", action="append", default=[])
    args = ap.parse_args()

    fdd_path = args.path.split("^")[0].split("$")[0].split(",")[0]
    for d in [os.path.dirname(os.path.abspath(fdd_path))] + args.import_dir:
        if d not in sys.path:
            sys.path.insert(0, d)

    from freeze_dried_data import RFDD

    t = time.time()
    try:
        f = RFDD(args.path, split=args.split)
    except ModuleNotFoundError as e:
        print(f"ERROR: {e}\nThe column_def references custom serializers from a module that "
              f"isn't importable. Locate the module and pass --import-dir <dir>.")
        sys.exit(1)
    print(f"file: {fdd_path}  ({os.path.getsize(fdd_path) / 1e9:.2f} GB)")
    print(f"opened in {time.time() - t:.2f}s; split={args.split!r}; index={type(f.index).__name__}")
    print(f"rows in split: {len(f):,}")

    if f.columns is None:
        print("columns: NONE (column-less file; values are arbitrary pickled objects)")
    else:
        print("columns:")
        for name in f.columns:
            cd = f.column_def[name] if getattr(f, "column_def", None) else "?"
            if isinstance(cd, tuple):
                ser, de = cd
                cd = f"custom ({getattr(ser, '__module__', '?')}.{getattr(ser, '__name__', '?')} / " \
                     f"{getattr(de, '__module__', '?')}.{getattr(de, '__name__', '?')})"
            print(f"  {name}: {cd}")

    splits = f.get_available_splits()
    print(f"splits ({len(splits)}): {splits if len(splits) <= 12 else splits[:12] + ['...']}")
    props = list(f.custom_properties.keys())
    print(f"custom properties ({len(props)}): {props}")

    if args.no_sample or len(f) == 0:
        f.close()
        return
    print()
    for i, (k, row) in enumerate(f.items()):
        if i >= args.rows:
            break
        print(f"--- row key={k!r} ({type(k).__name__})")
        if f.columns is None:
            print("  value:", describe(row))
        else:
            for name in f.columns:
                try:
                    print(f"  {name}: {describe(row[name])}")
                except Exception as e:
                    print(f"  {name}: <error reading: {e}>")

    f.close()


if __name__ == "__main__":
    main()
