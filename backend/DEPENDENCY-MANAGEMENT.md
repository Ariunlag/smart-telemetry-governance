# Backend dependency management

R0B uses a two-level, fully pinned dependency strategy for Python 3.12.

- `requirements.txt` contains the reviewed, direct runtime dependencies.
- `requirements-dev.txt` adds the reviewed, direct development dependencies, including the SQLite async test driver, and includes the runtime list.
- `requirements.lock` is the fully resolved validation and CI environment. CI installs this file exactly.

When a direct dependency must change, update its reviewed pin in the appropriate direct-requirements file, create a fresh Python 3.12 environment, resolve and test it, then replace `requirements.lock` with the resulting reviewed resolution. Do not make unreviewed transitive upgrades separately from that change.
