
## YYYY-MM-DD - Fix memory leak in JSON Parsing
**Learning:** Using `ijson` to parse large JSON files effectively resolves memory leak issues. However, `ijson` has limitations: it may fail to handle JSON Lines (JSONL) files correctly if you fallback using `ijson.items(f, 'item')`. When reading JSONL, fallback to `ijson.items(f, "", multiple_values=True)`.
**Action:** When implementing iterative JSON parsing, manually check for the `[` character to identify an array, and use `multiple_values=True` in `ijson.items()` to support JSONL safely without crashing.
