"""
@file data_grabber.py
@brief Parallel fetch of PCPartPicker data with Excel or HTTP output modes.

@details
  - Fetches all component types in parallel via ProcessPoolExecutor.
  - Converts library objects (e.g., EthernetCard) into JSON-safe dicts.
  - Output modes:
      * Excel: one sheet per component type (flattened columns).
      * HTTP: POST each item to a database endpoint you provide.

@usage
  # 1) Excel output
  python data_grabber.py --mode excel --excel-path pcpp_dump.xlsx

  # 2) HTTP output (POST each item)
  python data_grabber.py --mode http --base-url http://127.0.0.1:8000 \
      --endpoint-template "/pcparts/{type}" --concurrency 8

  # 3) Just to JSON (optional file)
  python data_grabber.py --mode json --json-path pcpp_dump.json

@notes
  - Requires: pcpartpicker, requests, pandas, openpyxl (for Excel)
  - The endpoint template may include "{type}" which will be replaced
    by the component type (e.g., "cpu", "video-card").
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, Tuple

import dataclasses
import inspect
from dataclasses import asdict, is_dataclass
from typing import Any

# --------------------------
# Configuration
# --------------------------

supported_types: List[str] = [
    "cpu",
    "speakers",
    "memory",
    "case",
    "wired-network-card",
    "keyboard",
    "wireless-network-card",
    "cpu-cooler",
    "mouse",
    "video-card",
    "monitor",
    "ups",
    "power-supply",
    "external-hard-drive",
    "fan-controller",
    "internal-hard-drive",
    "optical-drive",
    "sound-card",
    "case-fan",
    "headphones",
    "motherboard",
    "thermal-paste",
]


# --------------------------
# Utilities
# --------------------------

def _is_dataclass_instance(obj: Any) -> bool:
    """True only for dataclass *instances* (not classes)."""
    return is_dataclass(obj) and not inspect.isclass(obj)

def _to_jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(x) for x in obj]

    # ✅ Only instances go to asdict()
    if _is_dataclass_instance(obj):
        return _to_jsonable(asdict(obj))

    # If it's a dataclass *type*, choose how to represent it
    if is_dataclass(obj) and inspect.isclass(obj):
        # Keep it simple: use the class name (or return a schema if you prefer)
        return getattr(obj, "__name__", "DataclassType")

    from enum import Enum
    if isinstance(obj, Enum):
        return getattr(obj, "value", obj.name)

    from datetime import date, datetime
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    from decimal import Decimal
    if isinstance(obj, Decimal):
        return str(obj)

    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            return _to_jsonable(to_dict())
        except Exception:
            pass

    try:
        attrs = {
            k: v
            for k, v in vars(obj).items()
            if not k.startswith("_") and not callable(v)
        }
        if attrs:
            return _to_jsonable(attrs)
    except Exception:
        pass

    return str(obj)


def _flatten_for_excel(rows: List[dict]) -> List[dict]:
    """
    @brief Flatten nested dicts for Excel output.
    @details Uses pandas.json_normalize if available; otherwise a shallow fallback.

    @param rows List of JSON-safe dict rows.
    @return Flattened list of dicts.
    """
    if not rows:
        return rows

    try:
        import pandas as pd  # type: ignore
        df = pd.json_normalize(rows)
        return df.to_dict(orient="records")
    except Exception:
        # Fallback: keep as-is (Excel writer will still handle simple dicts).
        return rows


# --------------------------
# Fetching (parallel)
# --------------------------

def _worker_fetch(component_type: str) -> Tuple[str, List[dict]]:
    """
    @brief Worker process: fetch a single component type and convert items.

    @param component_type PCPartPicker slug (e.g., "cpu").
    @return (component_type, JSON-safe list of item dicts)
    """
    from pcpartpicker import API  # local import to isolate in child process

    api = API()
    raw = api.retrieve(component_type)
    items = raw[component_type]
    json_safe_items = [_to_jsonable(x) for x in items]
    return component_type, json_safe_items


def fetch_all(max_workers: int | None = None) -> Dict[str, Any]:
    """
    @brief Fetch all supported types in parallel.

    @param max_workers Process pool size (defaults to os.cpu_count()).
    @return Mapping: type -> list[dict] OR {"error": "..."} on failure.
    """
    results: Dict[str, Any] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker_fetch, t): t for t in supported_types}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                k, data = fut.result()
                results[k] = data
            except Exception as e:
                results[t] = {"error": str(e)}
    return results


# --------------------------
# Output: JSON
# --------------------------

def write_json(results: Dict[str, Any], out_path: str) -> None:
    """
    @brief Write results to a JSON file.

    @param results Mapping type -> list[dict] or {"error": "..."}.
    @param out_path Output file path.
    """
    json_ready = _to_jsonable(results)
    tmp = f"{out_path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(json_ready, f, indent=2, ensure_ascii=False)
    os.replace(tmp, out_path)


# --------------------------
# Output: Excel
# --------------------------

def write_excel(results: Dict[str, Any], excel_path: str) -> None:
    """
    @brief Write results to an Excel workbook, one sheet per component type.

    @param results Mapping type -> list[dict] (errors are skipped but sheet noted).
    @param excel_path Path to the .xlsx file.
    """
    import pandas as pd  # type: ignore

    # Create/overwrite the workbook.
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for comp_type, value in results.items():
            if isinstance(value, dict) and "error" in value:
                # Write a 1-row sheet noting the error
                df = pd.DataFrame([{"error": value["error"]}])
                df.to_excel(writer, sheet_name=_safe_sheet_name(comp_type), index=False)
                continue

            if not isinstance(value, list):
                df = pd.DataFrame([{"error": f"Unexpected type: {type(value).__name__}"}])
                df.to_excel(writer, sheet_name=_safe_sheet_name(comp_type), index=False)
                continue

            flat_rows = _flatten_for_excel(value)
            df = pd.DataFrame(flat_rows)
            # If the list is empty, ensure we still create a sheet
            if df.empty:
                df = pd.DataFrame([{}])
            df.to_excel(writer, sheet_name=_safe_sheet_name(comp_type), index=False)


def _safe_sheet_name(name: str) -> str:
    """
    @brief Sanitize Excel sheet name (<=31 chars, no invalid characters).

    @param name Proposed sheet name.
    @return Safe sheet name.
    """
    invalid = set(r'[]:*?/\\')
    sanitized = "".join(ch for ch in name if ch not in invalid)
    return (sanitized or "Sheet")[:31]


# --------------------------
# Output: HTTP POST
# --------------------------

def post_results(
    results: Dict[str, Any],
    base_url: str,
    endpoint_template: str = "/pcparts/{type}",
    concurrency: int = 8,
    timeout: float = 10.0,
    headers: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    """
    @brief POST each item of each component type to your database via HTTP.

    @details
      - Builds endpoint as: base_url.rstrip('/') + endpoint_template.format(type=comp_type)
      - Sends JSON bodies (one POST per item).
      - Uses a thread pool for I/O concurrency.

    @param results Mapping type -> list[dict] or error dicts.
    @param base_url Base URL, e.g. "http://127.0.0.1:8000".
    @param endpoint_template Path template; may contain "{type}" placeholder.
    @param concurrency Number of concurrent POSTs.
    @param timeout Per-request timeout (seconds).
    @param headers Optional HTTP headers (defaults to JSON content).
    @return Summary dict: {"posted": int, "failed": int, "errors": [..]}
    """
    import requests  # type: ignore

    sess = requests.Session()
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)

    jobs: List[Tuple[str, str, dict]] = []  # (type, url, payload)

    for comp_type, value in results.items():
        if isinstance(value, dict) and "error" in value:
            # Skip fetching error types; record as a "failed group"
            continue
        if not isinstance(value, list):
            continue

        url = base_url.rstrip("/") + endpoint_template.format(type=comp_type)
        for item in value:
            payload = _to_jsonable(item) if not _is_json_safe(item) else item
            jobs.append((comp_type, url, payload))

    summary = {"posted": 0, "failed": 0, "errors": []}

    def _post(job: Tuple[str, str, dict]) -> Tuple[bool, str | None]:
        comp_type, url, payload = job
        try:
            resp = sess.post(url, json=payload, timeout=timeout, headers=hdrs)
            if 200 <= resp.status_code < 300:
                return True, None
            return False, f"{comp_type} {resp.status_code} {resp.text[:200]}"
        except Exception as e:
            return False, f"{comp_type} EXC {e}"

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(_post, j) for j in jobs]
        for fut in as_completed(futures):
            ok, err = fut.result()
            if ok:
                summary["posted"] += 1
            else:
                summary["failed"] += 1
                if err:
                    summary["errors"].append(err)

    return summary


def _is_json_safe(x: Any) -> bool:
    """
    @brief Quick check if value is already built-in JSON-safe.
    """
    if x is None or isinstance(x, (str, int, float, bool)):
        return True
    if isinstance(x, list):
        return all(_is_json_safe(i) for i in x)
    if isinstance(x, dict):
        return all(isinstance(k, str) and _is_json_safe(v) for k, v in x.items())
    return False


# --------------------------
# CLI
# --------------------------

def parse_args() -> argparse.Namespace:
    """
    @brief Parse command-line arguments.
    """
    p = argparse.ArgumentParser(description="PCPartPicker parallel fetch with Excel/HTTP output.")
    p.add_argument("--mode", choices=["excel", "http", "json"], default="excel",
                   help="Output mode: 'excel' writes a workbook; 'http' posts to an API; 'json' writes a JSON dump.")
    p.add_argument("--excel-path", default="pcpp_dump.xlsx", help="Path for Excel output.")
    p.add_argument("--json-path", default="pcpp_dump.json", help="Path for JSON output (mode=json).")
    p.add_argument("--base-url", default="", help="Base URL for HTTP mode (e.g., http://127.0.0.1:8000).")
    p.add_argument("--endpoint-template", default="/pcparts/{type}",
                   help="Endpoint template for HTTP mode, may include '{type}'.")
    p.add_argument("--concurrency", type=int, default=8, help="HTTP POST concurrency (threads).")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout seconds.")
    p.add_argument("--max-workers", type=int, default=None,
                   help="Process pool size for fetching (default: cpu count).")
    return p.parse_args()


def main() -> None:
    """
    @brief Entry point: fetch data then write to Excel/JSON or POST to HTTP.
    """
    args = parse_args()

    results = fetch_all(max_workers=args.max_workers)

    if args.mode == "excel":
        write_excel(results, args.excel_path)
        print(f"Wrote Excel: {args.excel_path}")

    elif args.mode == "json":
        write_json(results, args.json_path)
        print(f"Wrote JSON: {args.json_path}")

    elif args.mode == "http":
        if not args.base_url:
            raise SystemExit("--base-url is required for --mode http")
        summary = post_results(
            results=results,
            base_url=args.base_url,
            endpoint_template=args.endpoint_template,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
        print(f"HTTP POST summary: posted={summary['posted']} failed={summary['failed']}")
        if summary["errors"]:
            print("Errors (first 10):")
            for e in summary["errors"][:10]:
                print("  -", e)


if __name__ == "__main__":
    main()
