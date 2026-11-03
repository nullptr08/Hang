import csv
import random
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# --- Configuration ---
URL = "https://daleel.admission.gov.sd/result2026/Result_2026.aspx"
START_ID = 11263693

CATEGORIES = {
    "1": "قبول عام",
    "3": "النفقة الخاصة",
    "5": "أبناء العاملين",
}

HIGH_JUMP = (10000, 15000)
SCAN_WORKERS = 8  # per category; total concurrency = 3 * SCAN_WORKERS
OUTPUT_CSV = "admission_boundaries.csv"
ERROR_LOG = "errors.log"
SHOW_TRACEBACKS = False  # True = full traceback on every exception

_local = threading.local()
_lock = threading.Lock()
_status = {}  # category_code -> status text
_error_count = 0


# --- Progress / logging ---------------------------------------------------
def _render():
    """Redraw the single progress line. Caller must hold _lock."""
    line = " | ".join(f"[C{c}] {s}" for c, s in sorted(_status.items()))
    sys.stdout.write("\r" + line + " " * 10)
    sys.stdout.flush()


def set_status(code, text):
    with _lock:
        _status[code] = text
        _render()


def log_msg(text):
    """Print a permanent line above the progress line."""
    with _lock:
        sys.stdout.write("\r" + " " * 150 + "\r" + text + "\n")
        _render()


def log_error(code, student_id, where, exc=None, note=""):
    global _error_count
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"{ts} [C{code}] ID {student_id} {where}"
    if exc is not None:
        msg += f": {type(exc).__name__}: {exc}"
    if note:
        msg += f" ({note})"
    tb = traceback.format_exc() if (exc is not None and SHOW_TRACEBACKS) else ""
    with _lock:
        _error_count += 1
        sys.stdout.write("\r" + " " * 150 + "\r" + msg + "\n")
        if tb:
            sys.stdout.write(tb)
        _render()
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n" + tb)


# --- HTTP -----------------------------------------------------------------
def _parse_tokens(html):
    soup = BeautifulSoup(html, "html.parser")

    def get_val(el_id):
        el = soup.find("input", {"id": el_id})
        return el.get("value", "") if el else ""

    captcha_el = soup.find("span", {"id": "CaptchaDiv"})
    captcha = captcha_el.get_text(strip=True) if captcha_el else ""
    return get_val("__VIEWSTATE"), get_val("__EVENTVALIDATION"), captcha, soup


def _state():
    """Per-thread session + cached form tokens."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.tokens = None
    return _local


def _fresh_tokens(st):
    r = st.session.get(URL, timeout=15)
    r.raise_for_status()
    vs, ev, cap, _ = _parse_tokens(r.text)
    return (vs, ev, cap) if vs and cap else None


def test_id(category, student_id_val):
    """One POST per ID; tokens are reused from the previous response."""
    st = _state()
    for attempt in range(1, 4):
        try:
            if not st.tokens:
                st.tokens = _fresh_tokens(st)
                if not st.tokens:
                    log_error(category, student_id_val, "GET",
                              note=f"no VIEWSTATE/CAPTCHA found, attempt {attempt}/3")
                    continue
            vs, ev, cap = st.tokens

            payload = {
                "__VIEWSTATE": vs,
                "__EVENTVALIDATION": ev,
                "DropDownList1": category,
                "TextBox1": str(student_id_val),
                "CaptchaInput": cap,
                "Button1": "عرض النتيجة",
            }
            r = st.session.post(URL, data=payload, headers={"Referer": URL}, timeout=15)
            r.raise_for_status()

            nvs, nev, ncap, soup = _parse_tokens(r.text)
            st.tokens = (nvs, nev, ncap) if nvs and ncap else None

            name_el = soup.find(id="StudentNameLabel")
            fac_el = soup.find(id="FacultyNameLabel")
            name = name_el.get_text(" ", strip=True) if name_el else ""
            faculty = fac_el.get_text(" ", strip=True) if fac_el else ""

            ok = bool(name and "NOT FOUND" not in name and "خطأ" not in name)
            return ok, name, faculty
        except Exception as e:  # requests errors AND parsing bugs
            st.tokens = None  # force a fresh GET on retry
            log_error(category, student_id_val, "request", e,
                      note=f"attempt {attempt}/3")
    log_error(category, student_id_val, "GAVE UP", note="treated as invalid")
    return False, "", ""


# --- Search ---------------------------------------------------------------
def run_category_search(code, name):
    # --- Phase 1: coarse search for ANY valid ID (upper bound for the lowest) ---
    current_id = START_ID + 1
    probe = 0
    while True:  # runs until a valid ID is found
        probe += 1
        current_id += random.randint(*HIGH_JUMP)
        set_status(code, f"P1 probe {probe} ID {current_id}")
        ok, _, _ = test_id(code, current_id)
        if ok:
            first_valid = current_id
            log_msg(f"[{name}] Phase 1 found valid ID: {first_valid}")
            break

    # --- Phase 2: parallel ascending scan of START_ID+1 .. first_valid ---
    # first_valid is itself valid, so the lowest valid ID is <= first_valid.
    first_id = START_ID + 1
    total = first_valid - first_id + 1
    lowest, s_name, faculty = first_valid, "", ""
    found = False

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        for batch_start in range(first_id, first_valid + 1, SCAN_WORKERS):
            batch = range(batch_start, min(batch_start + SCAN_WORKERS, first_valid + 1))
            done = batch_start - first_id
            set_status(code, f"P2 ID {batch_start} ({done}/{total}, {done * 100 // total}%)")

            results = list(pool.map(lambda i: test_id(code, i), batch))
            for sid, (ok, n, f) in zip(batch, results):
                if ok:
                    lowest, s_name, faculty, found = sid, n, f, True
                    break
            if found:
                break

    set_status(code, f"DONE {lowest}")
    log_msg(f"[{name}] Lowest valid ID: {lowest}")
    return {"category_code": code, "category_name": name,
            "lowest_valid_id": lowest, "student_name": s_name, "faculty": faculty}


def main():
    results = []
    print("=== Admission ID Boundary Finder ===")
    print(f"Errors are logged to {ERROR_LOG}\n")
    for code in CATEGORIES:
        _status[code] = "waiting"

    with ThreadPoolExecutor(max_workers=len(CATEGORIES)) as executor:
        futures = {executor.submit(run_category_search, c, n): n
                   for c, n in CATEGORIES.items()}
        for f in as_completed(futures):
            try:
                results.append(f.result())
            except Exception:
                log_msg(f"[{futures[f]}] CRASHED:\n{traceback.format_exc()}")

    print("\n" + "=" * 50)
    for r in results:
        print(f"[{r['category_name']}] Lowest valid ID: {r['lowest_valid_id']}")
    print(f"Total errors logged: {_error_count}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Category", "Lowest Valid ID", "Student Name", "Faculty"])
        for r in results:
            w.writerow([r["category_name"], r["lowest_valid_id"],
                        r["student_name"], r["faculty"]])
    print(f"Saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
    
