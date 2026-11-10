# Reproducible offline environment

Upstream: https://github.com/fastavro/fastavro
Upstream SHA: 2b321cce027f702351dab0f148012547880b04b3
Runtime: 3.13.14 (tags/v3.13.14:fd17997, Jun 10 2026, 13:03:48) [MSC v.1944 64 bit (AMD64)]
Platform: win32

Create an isolated virtual environment, then run from this checkout:
```
python -m pip install -r requirements-domain.lock
```
Set `PYTHONUTF8=1` and `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` before tests.
```
python -m pytest -q -o addopts= -o log_cli=false tests/test_schema.py tests/test_fastavro.py tests/test_json.py
python domain_baseline.py
```
Measured selected pytest process: 1.383 seconds.
Use the Python backend from the checkout. The wheel in the lock is for distribution metadata; run from this directory so local source takes precedence. Do not run pip install -e . (which would build C extensions). The upstream binary test module is skipped because its codec-parametrization skips collection without optional codecs; domain_baseline.py covers binary IO, blocks and schema resolution directly.

Tests use synthetic in-memory or temporary-file inputs under the upstream license.
No account, cloud service, browser or external database is needed. Recreate the
environment from this immutable commit to reset the starting state.
