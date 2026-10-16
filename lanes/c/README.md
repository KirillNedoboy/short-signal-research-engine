# Lane C source snapshot

`run_live.py` is kept at `lanes/c/run_live.py` and its relative source root remains `lanes/c/release/sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a/app`. This preserves the launcher's relative layout.

The upstream C release contains a vendored Python tree. The vendor tree is intentionally excluded from this export. `requirements.lock` records the 35 distribution name/version pairs read from the release's `.dist-info/METADATA` files; no vendored Python files are tracked.
