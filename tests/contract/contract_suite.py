#!/usr/bin/env python3
"""AnyStore v1 HTTP contract suite.

Implements every case in AnyStore_Postman_Test_Cases.md (T01-T37) plus the
acceptance boundary checks, using only the public API. Run against a live
server:

    python3 tests/contract/contract_suite.py http://127.0.0.1:8088/api/v1
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from typing import Any

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8088/api/v1"
RUN = uuid.uuid4().hex[:8]

# Must exceed the deployment's multipart threshold. Defaults to just over the
# documented 64 MiB default; override when the target uses a smaller threshold.
MULTIPART_SIZE = int(os.environ.get("ANYSTORE_TEST_MULTIPART_SIZE", 64 * 1024 * 1024 + 1024))

PASSED: list[str] = []
FAILED: list[tuple[str, str]] = []


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes):
        self.status = status
        # HTTP header names are case-insensitive on the wire.
        self.headers = {k.lower(): v for k, v in headers.items()}
        self.body = body

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")

    def json(self) -> Any:
        return json.loads(self.body)


def call(
    method: str,
    path: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    absolute: bool = False,
    raw_body: bytes | None = None,
    follow: bool = False,
) -> Response:
    url = path if absolute else f"{BASE}{path}"
    data = raw_body
    send_headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        send_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(url, data=data, method=method)
    for key, value in send_headers.items():
        request.add_header(key, value)

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = urllib.request.build_opener(
        *([] if follow else [NoRedirect])
    )
    try:
        with opener.open(request) as response:
            return Response(
                response.status, dict(response.headers), response.read()
            )
    except urllib.error.HTTPError as error:
        return Response(error.code, dict(error.headers), error.read())


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
    else:
        FAILED.append((name, detail))
        print(f"  FAIL {name}: {detail}")


def all_changes() -> list[dict]:
    """Reads the whole retained Changes feed."""
    items: list[dict] = []
    page = call("GET", "/changes?limit=1000").json()
    items.extend(page["items"])
    while page["has_more"]:
        page = call("GET", f"/changes?cursor={page['next_cursor']}&limit=1000").json()
        items.extend(page["items"])
    return items


def changes_for(object_id: str) -> list[dict]:
    return [c for c in all_changes() if c["object_id"] == object_id]


def key(name: str) -> str:
    return f"{RUN}-{name}"


# ---------------------------------------------------------------------------
# Object / Idempotency / Revision
# ---------------------------------------------------------------------------

print(f"AnyStore contract suite against {BASE} (run {RUN})")

folder_name = f"postman-folder-{RUN}"
t01_body = {
    "kind": "folder",
    "name": folder_name,
    "parent_id": "root",
    "metadata": {"suite": "postman"},
}
r = call("POST", "/objects", t01_body, {"Idempotency-Key": key("create-folder")})
check("T01 create folder status", r.status == 201, f"got {r.status} {r.text}")
folder = r.json()
check("T01 revision is 1", folder["revision"] == 1, str(folder))
check("T01 kind is folder", folder["kind"] == "folder")
check("T01 path is derived", folder["path"] == f"/{folder_name}", folder["path"])
check(
    "T01 folder omits content fields",
    all(k not in folder for k in ("content_type", "size", "sha256", "content_state")),
    str(folder),
)
folder_id = folder["id"]
folder_revision = folder["revision"]
t01_text = r.text

r = call("POST", "/objects", t01_body, {"Idempotency-Key": key("create-folder")})
check("T02 retry status", r.status == 201, str(r.status))
check("T02 retry body is byte-identical", r.text == t01_text, r.text)
check(
    "T02 retry created no second object",
    len([c for c in all_changes() if c["object_id"] == folder_id]) == 1,
)

r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": f"different-{RUN}", "parent_id": "root", "metadata": {}},
    {"Idempotency-Key": key("create-folder")},
)
check("T03 reused key status", r.status == 409, str(r.status))
check("T03 reused key error", r.json()["error"] == "idempotency_key_reused", r.text)

t04_body = {
    "kind": "file",
    "name": "example.txt",
    "parent_id": folder_id,
    "content_type": "text/plain",
    "metadata": {"suite": "postman", "version": 1},
}
r = call("POST", "/objects", t04_body, {"Idempotency-Key": key("create-file")})
check("T04 create file status", r.status == 201, f"{r.status} {r.text}")
file_object = r.json()
check("T04 revision is 1", file_object["revision"] == 1)
check("T04 content_state is none", file_object["content_state"] == "none", r.text)
file_id = file_object["id"]
file_revision = file_object["revision"]

r = call("POST", "/objects", t04_body, {"Idempotency-Key": key("create-file-dup")})
check("T05 duplicate name status", r.status == 409, str(r.status))
check("T05 duplicate name error", r.json()["error"] == "name_conflict", r.text)

r = call("GET", f"/objects/{file_id}")
check("T06 get object", r.status == 200 and r.json()["id"] == file_id, r.text)

old_revision = file_revision
t07_body = {"metadata": {"set": {"version": 2, "new_key": True}, "remove": []}}
t07_headers = {
    "If-Match": str(file_revision),
    "Idempotency-Key": key("patch-metadata"),
}
before = len(changes_for(file_id))
r = call("PATCH", f"/objects/{file_id}", t07_body, t07_headers)
check("T07 patch status", r.status == 200, f"{r.status} {r.text}")
patched = r.json()
check(
    "T07 revision incremented exactly once",
    patched["revision"] == old_revision + 1,
    str(patched["revision"]),
)
check("T07 metadata applied", patched["metadata"]["version"] == 2, r.text)
after = changes_for(file_id)
check("T07 emitted one change", len(after) == before + 1, str(len(after)))
check("T07 change action", after[-1]["action"] == "metadata_updated", str(after[-1]))
file_revision = patched["revision"]
t07_text = r.text

r = call(
    "PATCH",
    f"/objects/{file_id}",
    {"metadata": {"set": {"should_not_write": True}, "remove": []}},
    {"If-Match": str(old_revision), "Idempotency-Key": key("patch-stale")},
)
check("T08 stale revision status", r.status == 412, f"{r.status} {r.text}")
check("T08 stale revision error", r.json()["error"] == "revision_conflict", r.text)
check(
    "T08 reports current revision",
    r.json()["current_revision"] == file_revision,
    r.text,
)
current = call("GET", f"/objects/{file_id}").json()
check("T08 object unchanged", "should_not_write" not in current["metadata"], str(current))
check("T08 emitted no change", len(changes_for(file_id)) == len(after))

r = call("PATCH", f"/objects/{file_id}", t07_body, t07_headers)
check("T09 replay is the original success", r.status == 200, f"{r.status} {r.text}")
check("T09 replay body is byte-identical", r.text == t07_text, r.text)
check(
    "T09 replay did not bump the revision",
    call("GET", f"/objects/{file_id}").json()["revision"] == file_revision,
)
check("T09 replay emitted no change", len(changes_for(file_id)) == len(after))

folder2_name = f"postman-folder2-{RUN}"
r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": folder2_name, "parent_id": "root", "metadata": {}},
    {"Idempotency-Key": key("create-folder2")},
)
folder2_id = r.json()["id"]

before = len(changes_for(file_id))
r = call(
    "PATCH",
    f"/objects/{file_id}",
    {
        "name": "renamed.txt",
        "parent_id": folder2_id,
        "metadata": {"set": {"combined": True}, "remove": []},
    },
    {"If-Match": str(file_revision), "Idempotency-Key": key("patch-combined")},
)
check("T10 combined patch status", r.status == 200, f"{r.status} {r.text}")
combined = r.json()
check(
    "T10 revision incremented exactly once",
    combined["revision"] == file_revision + 1,
    str(combined["revision"]),
)
check("T10 name changed", combined["name"] == "renamed.txt")
check("T10 parent changed", combined["parent_id"] == folder2_id)
check("T10 metadata changed", combined["metadata"]["combined"] is True)
check("T10 id is unchanged", combined["id"] == file_id)
file_revision = combined["revision"]

emitted = changes_for(file_id)[before:]
check("T26 emitted exactly three changes", len(emitted) == 3, str(emitted))
check(
    "T26 change order is renamed, moved, metadata_updated",
    [c["action"] for c in emitted] == ["renamed", "moved", "metadata_updated"],
    str([c["action"] for c in emitted]),
)
check(
    "T26 all share the resulting revision",
    all(c["revision"] == file_revision for c in emitted),
)
check(
    "T26 all share one request id",
    len({c["request_id"] for c in emitted}) == 1,
)
check(
    "T26 all carry the idempotency key",
    all(c["idempotency_key"] == key("patch-combined") for c in emitted),
    str(emitted),
)

before = len(changes_for(file_id))
r = call(
    "PATCH",
    f"/objects/{file_id}",
    {"name": "renamed.txt", "parent_id": folder2_id},
    {"If-Match": str(file_revision), "Idempotency-Key": key("patch-noop")},
)
check("T11 no-op status", r.status == 200, f"{r.status} {r.text}")
check("T11 no-op keeps the revision", r.json()["revision"] == file_revision, r.text)
check("T11 no-op emitted no change", len(changes_for(file_id)) == before)

r = call("GET", f"/resolve?path=/{folder2_name}/renamed.txt")
check("T12 resolve status", r.status == 200, f"{r.status} {r.text}")
check("T12 resolve returns the object", r.json()["id"] == file_id, r.text)

r = call("GET", f"/objects/{folder2_id}/children?limit=100&order_by=name&order=asc")
children = r.json()["items"]
check("T13 list children status", r.status == 200)
check(
    "T13 file appears exactly once",
    len([c for c in children if c["id"] == file_id]) == 1,
    str(children),
)
check(
    "T13 only direct children are returned",
    all(c["parent_id"] == folder2_id for c in children),
    str(children),
)

r = call(
    "POST",
    "/query",
    {
        "kind": "file",
        "metadata": {"combined": {"eq": True}, "new_key": {"exists": True}},
        "limit": 100,
        "cursor": None,
    },
)
matches = [i for i in r.json()["items"] if i["id"] == file_id]
check("T14 metadata query returns the file once", len(matches) == 1, r.text)

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

CONTENT = b"hello world"

before_revision = call("GET", f"/objects/{file_id}").json()["revision"]
t15_body = {
    "object_id": file_id,
    "size": len(CONTENT),
    "content_type": "text/plain",
    "sha256": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
}
r = call("POST", "/uploads", t15_body, {"Idempotency-Key": key("upload-create")})
check("T15 create session status", r.status in (200, 201), f"{r.status} {r.text}")
session = r.json()
check("T15 returns an upload id", bool(session.get("id")), r.text)
check(
    "T15 target revision is unchanged",
    call("GET", f"/objects/{file_id}").json()["revision"] == before_revision,
)
check(
    "T15 response exposes no storage internals",
    not any(k in r.text for k in ("blob_ref", "blob_backend", "bucket")),
    r.text,
)
upload_id = session["id"]
t15_text = r.text

r = call("POST", "/uploads", t15_body, {"Idempotency-Key": key("upload-create")})
check("T16 session retry status", r.status in (200, 201))
check("T16 session retry is byte-identical", r.text == t15_text, r.text)

put = call("PUT", session["upload"]["url"], raw_body=CONTENT, absolute=True)
check("upload bytes to the signed URL", put.status == 200, str(put.status))

before = len(changes_for(file_id))
t18_headers = {
    "If-Match": str(before_revision),
    "Idempotency-Key": key("upload-complete"),
}
r = call("POST", f"/uploads/{upload_id}/complete", {}, t18_headers)
check("T18 complete status", r.status == 200, f"{r.status} {r.text}")
completed = r.json()
check(
    "T18 revision incremented once",
    completed["revision"] == before_revision + 1,
    r.text,
)
check("T18 content_state is ready", completed["content_state"] == "ready", r.text)
check("T18 size is recorded", completed["size"] == len(CONTENT), r.text)
emitted = changes_for(file_id)[before:]
check("T18 emitted one content_ready", [c["action"] for c in emitted] == ["content_ready"], str(emitted))
file_revision = completed["revision"]
t18_text = r.text

r = call("POST", f"/uploads/{upload_id}/complete", {}, t18_headers)
check("T19 complete replay status", r.status == 200, f"{r.status} {r.text}")
check("T19 complete replay is byte-identical", r.text == t18_text, r.text)
check(
    "T19 replay did not bump the revision",
    call("GET", f"/objects/{file_id}").json()["revision"] == file_revision,
)
check(
    "T19 replay emitted no duplicate change",
    len(changes_for(file_id)) == before + 1,
)

# T20: stale revision during replacement.
REPLACEMENT = b"replaced!!!"
r = call(
    "POST",
    "/uploads",
    {"object_id": file_id, "size": len(REPLACEMENT), "content_type": "text/plain"},
    {"Idempotency-Key": key("upload-replace-stale")},
)
stale_session = r.json()
call("PUT", stale_session["upload"]["url"], raw_body=REPLACEMENT, absolute=True)

stale_revision = file_revision
call(
    "PATCH",
    f"/objects/{file_id}",
    {"metadata": {"set": {"bump": 1}, "remove": []}},
    {"Idempotency-Key": key("bump-before-replace")},
)
file_revision = call("GET", f"/objects/{file_id}").json()["revision"]

before = len(changes_for(file_id))
r = call(
    "POST",
    f"/uploads/{stale_session['id']}/complete",
    {},
    {"If-Match": str(stale_revision), "Idempotency-Key": key("complete-stale")},
)
check("T20 stale completion status", r.status == 412, f"{r.status} {r.text}")
check("T20 stale completion error", r.json()["error"] == "revision_conflict", r.text)
check(
    "T20 object revision unchanged by the failure",
    call("GET", f"/objects/{file_id}").json()["revision"] == file_revision,
)
check("T20 emitted no change", len(changes_for(file_id)) == before)

redirect = call("GET", f"/objects/{file_id}/content")
check("T20 old content still readable", redirect.status == 307, str(redirect.status))
current_bytes = call("GET", redirect.header("location"), absolute=True).body
check("T20 old content is unchanged", current_bytes == CONTENT, str(current_bytes))

# T21: successful atomic replacement.
r = call(
    "POST",
    "/uploads",
    {"object_id": file_id, "size": len(REPLACEMENT), "content_type": "text/plain"},
    {"Idempotency-Key": key("upload-replace-ok")},
)
replace_session = r.json()
call("PUT", replace_session["upload"]["url"], raw_body=REPLACEMENT, absolute=True)

pre = call("GET", f"/objects/{file_id}").json()
pre_redirect = call("GET", f"/objects/{file_id}/content")
pre_bytes = call("GET", pre_redirect.header("location"), absolute=True).body
check("T21 old content readable before completion", pre_bytes == CONTENT)

before = len(changes_for(file_id))
r = call(
    "POST",
    f"/uploads/{replace_session['id']}/complete",
    {},
    {"If-Match": str(file_revision), "Idempotency-Key": key("complete-replace")},
)
check("T21 replacement status", r.status == 200, f"{r.status} {r.text}")
check("T21 revision incremented once", r.json()["revision"] == file_revision + 1, r.text)
emitted = changes_for(file_id)[before:]
check(
    "T21 emitted content_replaced",
    [c["action"] for c in emitted] == ["content_replaced"],
    str(emitted),
)
post = call("GET", f"/objects/{file_id}").json()
check("T21 id unchanged", post["id"] == pre["id"])
check("T21 name unchanged", post["name"] == pre["name"])
check("T21 metadata unchanged", post["metadata"] == pre["metadata"], str(post["metadata"]))
file_revision = post["revision"]

r = call("HEAD", f"/objects/{file_id}/content")
check("T22 head status", r.status == 200, str(r.status))
check(
    "T22 content-length equals object size",
    r.header("content-length") == str(len(REPLACEMENT)),
    str(r.headers),
)
check(
    "T22 sha256 header matches the object",
    r.header("x-anystore-sha256") == post["sha256"],
    str(r.headers),
)

r = call("GET", f"/objects/{file_id}/content")
check("T23 get content is a redirect", r.status == 307, str(r.status))
download = call("GET", r.header("location"), absolute=True)
check("T23 downloaded bytes are current", download.body == REPLACEMENT)
check(
    "T23 download filename is the current object name",
    post["name"] in download.header("content-disposition") or "",
    str(download.header("content-disposition")),
)

# T17: multipart part allocation idempotency.
r = call(
    "POST",
    "/objects",
    {"kind": "file", "name": f"big-{RUN}.bin", "parent_id": folder2_id},
    {"Idempotency-Key": key("create-big")},
)
big_id = r.json()["id"]
big_revision = r.json()["revision"]
part_b = b"B" * 1024
part_a = b"A" * (MULTIPART_SIZE - len(part_b))
r = call(
    "POST",
    "/uploads",
    {"object_id": big_id, "size": len(part_a) + len(part_b)},
    {"Idempotency-Key": key("upload-big")},
)
big_session = r.json()
check("T17 large upload uses multipart", big_session["mode"] == "multipart", r.text)
check("T17 multipart reports a part size", "part_size" in big_session, r.text)

r = call(
    "POST",
    f"/uploads/{big_session['id']}/parts",
    {"part_numbers": [1, 2]},
    {"Idempotency-Key": key("upload-parts")},
)
check("T17 part allocation status", r.status == 200, f"{r.status} {r.text}")
parts_text = r.text
parts = r.json()["parts"]
r = call(
    "POST",
    f"/uploads/{big_session['id']}/parts",
    {"part_numbers": [1, 2]},
    {"Idempotency-Key": key("upload-parts")},
)
check("T17 part allocation replay is byte-identical", r.text == parts_text, r.text)

for part, payload in zip(sorted(parts, key=lambda p: p["part_number"]), [part_a, part_b]):
    call("PUT", part["url"], raw_body=payload, absolute=True)

r = call(
    "POST",
    f"/uploads/{big_session['id']}/complete",
    {"parts": [{"part_number": 1, "etag": "a"}, {"part_number": 2, "etag": "b"}]},
    {"If-Match": str(big_revision), "Idempotency-Key": key("complete-big")},
)
check("T17 multipart completion", r.status == 200, f"{r.status} {r.text}")
check(
    "T17 multipart size is the sum of parts",
    r.json()["size"] == len(part_a) + len(part_b),
    r.text,
)

# ---------------------------------------------------------------------------
# Changes
# ---------------------------------------------------------------------------

r = call("GET", "/changes?limit=500")
check("T24 initial changes status", r.status == 200, r.text)
page = r.json()
check("T24 returns items", isinstance(page["items"], list))
check("T24 always returns a next cursor", bool(page["next_cursor"]), r.text)
ids = [c["change_id"] for c in page["items"]]
check("T24 change ids are unique", len(ids) == len(set(ids)))
check("T24 change ids are monotonic", ids == sorted(ids), str(ids[:5]))
check(
    "T24 changes carry no metadata or bytes",
    all(
        set(c.keys())
        <= {
            "change_id",
            "object_id",
            "revision",
            "action",
            "changed_at",
            "request_id",
            "idempotency_key",
            "tombstone",
        }
        for c in page["items"]
    ),
    str(page["items"][:1]),
)
changes_cursor = page["next_cursor"]

first = call("GET", f"/changes?cursor={changes_cursor}&limit=10")
check("T27 first read status", first.status == 200, first.text)
call(
    "POST",
    "/objects",
    {"kind": "folder", "name": f"cursor-probe-{RUN}", "parent_id": "root"},
    {"Idempotency-Key": key("cursor-probe")},
)
repeat = call("GET", f"/changes?cursor={changes_cursor}&limit=10")
check("T27 cursor page is stable", repeat.text == first.text, repeat.text[:200])

check(
    "T27 a different limit on a bound cursor is rejected",
    call("GET", f"/changes?cursor={changes_cursor}&limit=11").status == 400,
)

seen_before = {c["change_id"] for c in first.json()["items"]}
continuation = call(
    "GET", f"/changes?cursor={first.json()['next_cursor']}&limit=10"
).json()
check(
    "T28 continuation does not replay",
    not ({c["change_id"] for c in continuation["items"]} & seen_before),
    str(continuation["items"][:1]),
)

r = call("GET", "/changes?cursor=chgcur_00000000000000000000000000&limit=10")
check("T37 unknown cursor status", r.status == 410, f"{r.status} {r.text}")
check("T37 unknown cursor error", r.json()["error"] == "changes_cursor_expired", r.text)

# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

file_revision = call("GET", f"/objects/{file_id}").json()["revision"]
before = len(changes_for(file_id))
r = call(
    "DELETE",
    f"/objects/{file_id}",
    headers={"If-Match": "1", "Idempotency-Key": key("delete-stale")},
)
check("T29 stale delete status", r.status == 412, f"{r.status} {r.text}")
check("T29 object still exists", call("GET", f"/objects/{file_id}").status == 200)
check("T29 no tombstone emitted", len(changes_for(file_id)) == before)

r = call(
    "DELETE",
    f"/objects/{file_id}",
    headers={
        "If-Match": str(file_revision),
        "Idempotency-Key": key("delete-file"),
    },
)
check("T30 delete status", r.status == 204, f"{r.status} {r.text}")
check("T30 get returns 404", call("GET", f"/objects/{file_id}").status == 404)
check(
    "T30 resolve excludes the object",
    call("GET", f"/resolve?path=/{folder2_name}/renamed.txt").status == 404,
)
check(
    "T30 query excludes the object",
    not [
        i
        for i in call(
            "POST", "/query", {"metadata": {"combined": {"eq": True}}, "limit": 100}
        ).json()["items"]
        if i["id"] == file_id
    ],
)
tombstones = [c for c in changes_for(file_id) if c["action"] == "deleted"]
check("T30 exactly one tombstone", len(tombstones) == 1, str(tombstones))
check("T30 tombstone flag is set", tombstones[0].get("tombstone") is True, str(tombstones))
check(
    "T30 tombstone revision is previous + 1",
    tombstones[0]["revision"] == file_revision + 1,
    str(tombstones[0]),
)

r = call(
    "DELETE",
    f"/objects/{file_id}",
    headers={
        "If-Match": str(file_revision),
        "Idempotency-Key": key("delete-file"),
    },
)
check("T31 delete retry status", r.status == 204, f"{r.status} {r.text}")
check(
    "T31 no second tombstone",
    len([c for c in changes_for(file_id) if c["action"] == "deleted"]) == 1,
)

r = call(
    "DELETE",
    f"/objects/{folder2_id}",
    headers={"Idempotency-Key": key("delete-file")},
)
check("T32 key reuse on another target", r.status == 409, f"{r.status} {r.text}")
check("T32 key reuse error", r.json()["error"] == "idempotency_key_reused", r.text)

# T33/T34: non-empty and recursive folder delete.
r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": f"tree-{RUN}", "parent_id": "root"},
    {"Idempotency-Key": key("tree-root")},
)
tree_root = r.json()["id"]
tree_root_revision = r.json()["revision"]
r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": "level1", "parent_id": tree_root},
    {"Idempotency-Key": key("tree-l1")},
)
tree_l1 = r.json()["id"]
r = call(
    "POST",
    "/objects",
    {"kind": "file", "name": "leaf.txt", "parent_id": tree_l1},
    {"Idempotency-Key": key("tree-leaf")},
)
tree_leaf = r.json()["id"]

before = len(all_changes())
r = call(
    "DELETE",
    f"/objects/{tree_root}",
    headers={
        "If-Match": str(tree_root_revision),
        "Idempotency-Key": key("delete-nonempty"),
    },
)
check("T33 non-empty delete status", r.status == 409, f"{r.status} {r.text}")
check("T33 non-empty delete error", r.json()["error"] == "folder_not_empty", r.text)
check(
    "T33 revision unchanged",
    call("GET", f"/objects/{tree_root}").json()["revision"] == tree_root_revision,
)
check("T33 emitted no change", len(all_changes()) == before)

r = call(
    "DELETE",
    f"/objects/{tree_root}?recursive=true",
    headers={
        "If-Match": str(tree_root_revision),
        "Idempotency-Key": key("delete-recursive"),
    },
)
check("T34 recursive delete status", r.status == 204, f"{r.status} {r.text}")
for name, target in (("root", tree_root), ("level1", tree_l1), ("leaf", tree_leaf)):
    check(f"T34 {name} is gone", call("GET", f"/objects/{target}").status == 404)
    stones = [c for c in changes_for(target) if c["action"] == "deleted"]
    check(f"T34 one tombstone for {name}", len(stones) == 1, str(stones))

# ---------------------------------------------------------------------------
# Invalid move
# ---------------------------------------------------------------------------

r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": f"move-a-{RUN}", "parent_id": "root"},
    {"Idempotency-Key": key("move-a")},
)
move_a = r.json()["id"]
move_a_revision = r.json()["revision"]
r = call(
    "POST",
    "/objects",
    {"kind": "folder", "name": "b", "parent_id": move_a},
    {"Idempotency-Key": key("move-b")},
)
move_b = r.json()["id"]

before = len(changes_for(move_a))
r = call(
    "PATCH",
    f"/objects/{move_a}",
    {"parent_id": move_a},
    {"Idempotency-Key": key("move-self")},
)
check("T35 move into itself status", r.status == 409, f"{r.status} {r.text}")
check("T35 move into itself error", r.json()["error"] == "invalid_move", r.text)

r = call(
    "PATCH",
    f"/objects/{move_a}",
    {"parent_id": move_b},
    {"Idempotency-Key": key("move-descendant")},
)
check("T36 move into descendant status", r.status == 409, f"{r.status} {r.text}")
check("T36 move into descendant error", r.json()["error"] == "invalid_move", r.text)
check(
    "T35/T36 revision unchanged",
    call("GET", f"/objects/{move_a}").json()["revision"] == move_a_revision,
)
check("T35/T36 emitted no change", len(changes_for(move_a)) == before)

# ---------------------------------------------------------------------------
# Root protection and validation
# ---------------------------------------------------------------------------

check(
    "root cannot be deleted",
    call("DELETE", "/objects/root", headers={"Idempotency-Key": key("del-root")}).status
    == 400,
)
check(
    "root cannot be renamed",
    call(
        "PATCH", "/objects/root", {"name": "x"}, {"Idempotency-Key": key("ren-root")}
    ).status
    == 400,
)
check(
    "invalid names are rejected",
    call(
        "POST",
        "/objects",
        {"kind": "folder", "name": "a/b", "parent_id": "root"},
        {"Idempotency-Key": key("bad-name")},
    ).json()["error"]
    == "invalid_name",
)
check(
    "folders reject content requests",
    call("GET", f"/objects/{folder_id}/content").json()["error"] == "not_a_file",
)
check(
    "files without content report content_not_ready",
    call(
        "GET",
        "/objects/"
        + call(
            "POST",
            "/objects",
            {"kind": "file", "name": f"empty-{RUN}.txt", "parent_id": "root"},
            {"Idempotency-Key": key("empty-file")},
        ).json()["id"]
        + "/content",
    ).json()["error"]
    == "content_not_ready",
)
check("root resolves to /", call("GET", "/resolve?path=/").json()["id"] == "root")

# ---------------------------------------------------------------------------

print()
print(f"passed: {len(PASSED)}  failed: {len(FAILED)}")
if FAILED:
    print("\nfailures:")
    for name, detail in FAILED:
        print(f"  - {name}: {detail}")
    sys.exit(1)
print("ALL CONTRACT CHECKS PASSED")
