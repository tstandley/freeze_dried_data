# Portable custom codecs and dill

Verified with freeze_dried_data 2.6.4 and dill 0.4.0 in isolated subprocesses.

## What FDD actually serializes

`WFDD.close()` detects any tuple-valued column definition and calls
`dill.dumps(self.column_def)` with no explicit dill options. Thus dill's global
settings **at close time** control codec serialization. Without custom tuples,
it uses `system_serialize` for the column definition. RFDD tries its system
loader, then falls back to `dill.loads` for the column definition.

Dill is not automatically self-contained. Importable module-level functions are
normally stored by reference (module + qualified name), even when `byref=False`
and `recurse=True`. The reader then needs that module installed and importable;
not explicitly importing it in the reader script is not the same as eliminating
the dependency. FDD must load both encode and decode functions to open the file.

## Make the project's codec code travel with the file

Use nested functions (or genuinely by-value functions) and enable `recurse=True`
while the writer closes. Capture project helper functions in the same nested
scope so their code is included too. Import standard/third-party runtime
libraries inside the functions; those libraries must still be installed.

```python
import dill
from freeze_dried_data import WFDD


def portable_codec():
    scale = 10
    def adjust(value):
        return value * scale
    def encode(value):
        import struct
        return struct.pack('<q', adjust(value))
    def decode(data):
        import struct
        return struct.unpack('<q', data)[0] // scale
    return encode, decode


previous = dill.settings['recurse']
try:
    dill.settings['recurse'] = True
    with WFDD(output_path, columns={'value': portable_codec()}) as out:
        out[1] = {'value': 42}
    # close() has happened while recurse=True is still active
finally:
    dill.settings['recurse'] = previous
```

This recipe was tested with `portable_codec` defined in a separate author module,
then that module made unavailable before reading via `python -I` in a fresh
process. The file still read correctly. Nested functions with the default
`recurse=False` failed that test because the module's globals remained a dependency.
A direct tuple of importable top-level encode/decode functions still failed with
`recurse=True`; the option does not force such functions to be stored by value.

A wrapper that calls an imported project function can still capture a module
reference; it does not necessarily embed the function's implementation or its
helpers. Local `import my_project` likewise preserves that dependency. Put the
small required implementation/helper chain into by-value closures or explicitly
package the dependency, then test the actual resulting file. Do not claim that
dill bundles NumPy, Torch, binary extensions, or arbitrary entire libraries.

Dill settings are process-global. For concurrent writers, isolate file creation
in a process or coordinate the setting; do not race other writes. Reopening and
closing a writer reserializes the schema, so test portability after that too.
Dill bytecode may depend on Python/library versions; embedding code is not a
promise of universal cross-version compatibility.

## Validate portability, not just same-process readability

1. Write a miniature file through the same codec and writer path as production.
2. Start a fresh isolated Python process outside the author module's import path.
3. Make that module unavailable; merely omitting an import statement is inadequate.
4. Open the FDD, read and verify values; separately test round-trip writes if
   portability of the serializer is needed for later rewrites.
5. Check any remaining dependencies against the intended reader environment.

Module-referenced codecs remain appropriate when deployment intentionally
includes their module. For old files that already need it, supply the module on
sys.path to read them; changing dill settings on the reader cannot replace
missing referenced code. A conversion requires opening with the old dependency
available and writing a new schema with portable codecs.
