#!/usr/bin/env python3
"""Small, self-cleaning smoke test for a deployed AnyStore service."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from typing import Any

BASE = os.environ.get("ANYSTORE_TEST_BASE_URL", "").rstrip("/")
ORIGIN = BASE.removesuffix("/api/v1")
TOKEN = os.environ.get("ANYSTORE_TEST_AUTH_TOKEN", "").strip()
RUN = uuid.uuid4().hex[:10]


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes):
        self.status = status
        self.headers = {key.lower(): value for key, value in headers.items()}
        self.body = body

    def json(self) -> Any:
        return json.loads(self.body)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def request(
    method: str,
    url: str,
    *,
    body: Any = None,
    raw_body: bytes | None = None,
    headers: dict[str, str] | None = None,
    authenticated: bool = True,
    follow: bool = False,
) -> Response:
    request_headers = dict(headers or {})
    data = raw_body
    if body is not None:
        data = json.dumps(body).encode()
        request_headers.setdefault("Content-Type", "application/json")
    if authenticated:
        request_headers.setdefault("Authorization", f"Bearer {TOKEN}")

    req = urllib.request.Request(url, method=method, data=data, headers=request_headers)
    opener = urllib.request.build_opener(*([] if follow else [NoRedirect]))
    try:
        with opener.open(req, timeout=30) as response:
            return Response(response.status, dict(response.headers), response.read())
    except urllib.error.HTTPError as error:
        return Response(error.code, dict(error.headers), error.read())


def api(method: str, path: str, **kwargs: Any) -> Response:
    return request(method, f"{BASE}{path}", **kwargs)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    if not BASE:
        print("ANYSTORE_TEST_BASE_URL is required", file=sys.stderr)
        return 2
    if not TOKEN:
        print("ANYSTORE_TEST_AUTH_TOKEN is required", file=sys.stderr)
        return 2

    payload = f"AnyStore remote smoke {RUN}\n".encode()
    digest = hashlib.sha256(payload).hexdigest()
    folder_id: str | None = None
    file_id: str | None = None
    upload_id: str | None = None
    file_revision: int | None = None
    completed = False

    try:
        health = request("GET", f"{ORIGIN}/healthz", authenticated=False)
        require(health.status == 200 and health.body == b"ok", "healthz failed")
        require(
            api("GET", "/objects/root", authenticated=False).status == 401,
            "unauthenticated request was not rejected",
        )
        root = api("GET", "/objects/root")
        require(root.status == 200 and root.json()["id"] == "root", "root read failed")

        folder_key = f"smoke-folder-{RUN}"
        folder_body = {
            "kind": "folder",
            "name": f"remote-smoke-{RUN}",
            "parent_id": "root",
            "metadata": {"suite": "remote-smoke"},
        }
        folder = api(
            "POST",
            "/objects",
            body=folder_body,
            headers={"Idempotency-Key": folder_key},
        )
        require(folder.status == 201, f"folder create failed: {folder.body!r}")
        folder_id = folder.json()["id"]
        replay = api(
            "POST",
            "/objects",
            body=folder_body,
            headers={"Idempotency-Key": folder_key},
        )
        require(replay.body == folder.body, "folder replay was not byte-identical")

        file = api(
            "POST",
            "/objects",
            body={
                "kind": "file",
                "name": "payload.bin",
                "parent_id": folder_id,
                "content_type": "application/octet-stream",
            },
            headers={"Idempotency-Key": f"smoke-file-{RUN}"},
        )
        require(file.status == 201, f"file create failed: {file.body!r}")
        file_id = file.json()["id"]
        file_revision = file.json()["revision"]

        upload = api(
            "POST",
            "/uploads",
            body={
                "object_id": file_id,
                "size": len(payload),
                "content_type": "application/octet-stream",
                "sha256": digest,
            },
            headers={"Idempotency-Key": f"smoke-upload-{RUN}"},
        )
        require(upload.status == 201, f"upload create failed: {upload.body!r}")
        upload_data = upload.json()
        upload_id = upload_data["id"]
        put = request(
            "PUT",
            upload_data["upload"]["url"],
            raw_body=payload,
            authenticated=False,
        )
        require(200 <= put.status < 300, f"signed upload failed: {put.status}")

        commit = api(
            "POST",
            f"/uploads/{upload_id}/complete",
            body={},
            headers={
                "If-Match": str(file_revision),
                "Idempotency-Key": f"smoke-complete-{RUN}",
            },
        )
        require(commit.status == 200, f"upload complete failed: {commit.body!r}")
        require(commit.json()["sha256"] == digest, "committed digest differs")
        file_revision = commit.json()["revision"]
        completed = True

        redirect = api("GET", f"/objects/{file_id}/content")
        require(redirect.status == 307, "content endpoint did not redirect")
        location = redirect.headers.get("location")
        require(bool(location), "content redirect has no location")
        download = request("GET", location or "", authenticated=False, follow=True)
        require(download.status == 200 and download.body == payload, "download mismatch")

        changes = api("GET", "/changes?limit=1000")
        require(changes.status == 200, "Changes read failed")
        actions = [
            change["action"]
            for change in changes.json()["items"]
            if change["object_id"] == file_id
        ]
        require(actions == ["created", "content_ready"], f"unexpected Changes: {actions}")

        print(
            json.dumps(
                {
                    "ok": True,
                    "run": RUN,
                    "health": "ok",
                    "auth": "ok",
                    "idempotency": "byte-identical",
                    "postgresql": "ok",
                    "cos_upload": "ok",
                    "cos_download": "ok",
                    "changes": actions,
                }
            )
        )
        return 0
    finally:
        if upload_id and not completed:
            api(
                "DELETE",
                f"/uploads/{upload_id}",
                headers={"Idempotency-Key": f"cleanup-upload-{RUN}"},
            )
        if file_id:
            headers = {"Idempotency-Key": f"cleanup-file-{RUN}"}
            if file_revision is not None:
                headers["If-Match"] = str(file_revision)
            api("DELETE", f"/objects/{file_id}", headers=headers)
        if folder_id:
            api(
                "DELETE",
                f"/objects/{folder_id}?recursive=true",
                headers={"Idempotency-Key": f"cleanup-folder-{RUN}"},
            )


if __name__ == "__main__":
    raise SystemExit(main())
