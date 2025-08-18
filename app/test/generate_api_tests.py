#!/usr/bin/env python3
"""
Generate JSON test responses for all GET endpoints discovered via OpenAPI.
Saves files under app/test/test-docs/<group>/<sanitized-name>.json

Assumptions:
- Backend exposed at http://localhost:30888 with prefix /api/v1
- OpenAPI available at /openapi.json
"""

import json
import os
import re
import sys
import time
from urllib import request, parse, error


BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:30888")
API_PREFIX = "/api/v1"
OPENAPI_URL = f"{BASE_URL}/openapi.json"
OUTPUT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "test-docs"))


# Sample values for common path/query params
SAMPLE_VALUES = {
    "symbol": "AAPL",
    "id": "1",
    "news_id": "1",
    "post_id": "1",
    "trend_id": "1",
    "username": "elonmusk",
    "category": "crypto",
    "topic": "AI",
    "batch_id": "demo-batch",
    "year": "2023",
    "month": "05",
    "period_date": "2024-03-31",
}


def http_get(url: str, timeout: int = 20):
    req = request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            content_type = resp.headers.get("Content-Type", "")
            data = resp.read()
            if b"application/json" in content_type.encode() or data.startswith(b"{") or data.startswith(b"["):
                try:
                    return json.loads(data.decode("utf-8")), None
                except Exception as e:
                    return {"raw": data.decode("utf-8", "ignore")}, None
            return {"raw": data.decode("utf-8", "ignore")}, None
    except error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = str(e)
        return None, {"status": e.code, "reason": e.reason, "body": body}
    except Exception as e:
        return None, {"error": str(e)}


def sanitize_filename(s: str) -> str:
    s = s.strip("/")
    s = s.replace("/", "_")
    s = re.sub(r"[^a-zA-Z0-9._-]", "-", s)
    return s or "root"


def infer_group_from_path(path: str) -> str:
    # path like /api/v1/<group>/...
    parts = path.strip("/").split("/")
    if len(parts) >= 3 and parts[0] == "api" and parts[1] == "v1":
        return parts[2]
    if len(parts) >= 1:
        return parts[0]
    return "misc"


def fill_path_params(path: str, params: list) -> str:
    # params from openapi: list of dict with name, in
    filled = path
    for p in params:
        if p.get("in") == "path":
            name = p.get("name")
            sample = SAMPLE_VALUES.get(name, "sample")
            filled = re.sub(r"\{" + re.escape(name) + r"\}", parse.quote(sample), filled)
    return filled


def build_query(params: list) -> str:
    query = {}
    for p in params:
        if p.get("in") == "query":
            name = p.get("name")
            required = p.get("required", False)
            if required:
                sample = SAMPLE_VALUES.get(name, None)
                if sample is None:
                    # generic sensible defaults
                    if name in ("limit", "page", "size"):
                        sample = "10"
                    elif name in ("order", "sort"):
                        sample = "desc"
                    else:
                        sample = "sample"
                query[name] = sample
    if not query:
        return ""
    return "?" + parse.urlencode(query)


def main():
    print(f"Loading OpenAPI from {OPENAPI_URL}")
    spec, err = http_get(OPENAPI_URL)
    if err:
        print("Failed to load OpenAPI:", err, file=sys.stderr)
        sys.exit(1)

    paths = spec.get("paths", {})
    total = 0
    ok = 0
    fail = 0
    for path, methods in paths.items():
        get_op = methods.get("get")
        if not get_op:
            continue
        # Only API v1
        if not path.startswith(API_PREFIX):
            continue

        params = get_op.get("parameters", []) + methods.get("parameters", [])
        full_path = fill_path_params(path, params)
        query = build_query(params)
        url = f"{BASE_URL}{full_path}{query}"

        group = infer_group_from_path(path)
        out_dir = os.path.join(OUTPUT_ROOT, f"{group}-api")
        os.makedirs(out_dir, exist_ok=True)
        fname = sanitize_filename(full_path[len(API_PREFIX):] + (query if query else ""))
        out_file = os.path.join(out_dir, f"{fname}.json")

        print(f"GET {url}")
        total += 1
        data, error_info = http_get(url)
        if error_info:
            fail += 1
            payload = {"request_url": url, "error": error_info}
        else:
            ok += 1
            payload = {"request_url": url, "data": data}

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        time.sleep(0.05)

    print(f"Done. total={total}, ok={ok}, fail={fail}")


if __name__ == "__main__":
    main()


