#!/usr/bin/env python3
"""Normalise the AnyStore Postman collection for newman.

The shipped collection stores each URL as an object with `host: ["{{baseUrl}}"]`
and no `path`, so newman resolves `{{baseUrl}}/objects` to `{{baseUrl}}` and
every request 404s. Postman's own client falls back to `raw`; newman does not.
This rewrites each URL to its `raw` string, which newman parses correctly.

The original collection is never modified.
"""

import json
import sys
from pathlib import Path


def normalise(node):
    for item in node:
        if "item" in item:
            normalise(item["item"])
            continue
        request = item.get("request")
        if isinstance(request, dict):
            url = request.get("url")
            if isinstance(url, dict) and "raw" in url:
                request["url"] = url["raw"]


def main() -> int:
    source = Path(sys.argv[1])
    target = Path(sys.argv[2])
    collection = json.loads(source.read_text())
    normalise(collection["item"])
    target.write_text(json.dumps(collection, indent=2))
    print(f"normalised {source} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
