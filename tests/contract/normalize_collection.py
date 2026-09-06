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
import uuid
from pathlib import Path


def normalise(node, run_id):
    for item in node:
        if "item" in item:
            normalise(item["item"], run_id)
            continue
        request = item.get("request")
        if isinstance(request, dict):
            for header in request.get("header", []):
                if header.get("key", "").lower() == "idempotency-key":
                    header["value"] = f"{header.get('value', '')}-{run_id}"

            url = request.get("url")
            if isinstance(url, dict) and "raw" in url:
                request["url"] = url["raw"]

            body = request.get("body", {})
            if body.get("mode") == "raw" and isinstance(body.get("raw"), str):
                body["raw"] = body["raw"].replace(
                    '"name": "postman-folder"',
                    f'"name": "postman-folder-{run_id}"',
                )


def add_bearer_auth(collection):
    collection["auth"] = {
        "type": "bearer",
        "bearer": [
            {
                "key": "token",
                "value": "{{authToken}}",
                "type": "string",
            }
        ],
    }
    variables = collection.setdefault("variable", [])
    if not any(variable.get("key") == "authToken" for variable in variables):
        variables.append(
            {
                "key": "authToken",
                "value": "",
                "type": "string",
            }
        )


def add_cleanup(collection, run_id):
    collection["item"].append(
        {
            "name": "Cleanup",
            "item": [
                {
                    "name": "Abort Upload Session",
                    "request": {
                        "method": "DELETE",
                        "header": [
                            {
                                "key": "Idempotency-Key",
                                "value": f"cleanup-upload-{run_id}",
                                "type": "text",
                            }
                        ],
                        "url": "{{baseUrl}}/uploads/{{uploadId}}",
                    },
                    "event": [
                        {
                            "listen": "test",
                            "script": {
                                "type": "text/javascript",
                                "exec": [
                                    'pm.test("upload cleanup",()=>pm.response.to.have.status(204));'
                                ],
                            },
                        }
                    ],
                    "response": [],
                },
                {
                    "name": "Delete Test Folder",
                    "request": {
                        "method": "DELETE",
                        "header": [
                            {
                                "key": "If-Match",
                                "value": "{{folderRevision}}",
                                "type": "text",
                            },
                            {
                                "key": "Idempotency-Key",
                                "value": f"cleanup-folder-{run_id}",
                                "type": "text",
                            },
                        ],
                        "url": "{{baseUrl}}/objects/{{folderId}}?recursive=true",
                    },
                    "event": [
                        {
                            "listen": "test",
                            "script": {
                                "type": "text/javascript",
                                "exec": [
                                    'pm.test("folder cleanup",()=>pm.response.to.have.status(204));'
                                ],
                            },
                        }
                    ],
                    "response": [],
                },
            ],
        }
    )


def main() -> int:
    source = Path(sys.argv[1])
    target = Path(sys.argv[2])
    collection = json.loads(source.read_text())
    run_id = uuid.uuid4().hex[:8]
    normalise(collection["item"], run_id)
    add_bearer_auth(collection)
    add_cleanup(collection, run_id)
    target.write_text(json.dumps(collection, indent=2))
    print(f"normalised {source} -> {target} (run {run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
