import csv
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# --- Configuration ---
URL = "https://daleel.admission.gov.sd/result2026/Result_2026.aspx"
KNOWN_VALID_ID = 22001925   # confirmed valid; the search starts here and goes DOWN
STOP_AFTER_INVALID = 3000   # stop after this many consecutive invalid IDs below the last valid one

CATEGORIES = {
    "1": "قبول عام",
    "3": "النفقة الخاصة",
    "5": "أبناء العاملين",
}

SCAN_WORKERS = 8  # per category; total concurrency = 3 * SCAN_WORKERS
OUTPUT_CSV = "admission_boundaries.csv"
ERROR_LOG = "errors.log"
SHOW_TRACEBACKS = False
REUSE_TOKENS = True  # auto-disabled at startup if the reuse check fails

_local = threading.local()
_lock = threading.Lock()
_status = {}
_error_count = 0


# --- Progress / logging ---------------------------------------------------
def _render():
    line = " | ".join(f"[C{c}] {s}" for c, s in sorted(_status.items()))
    sys.stdout.write("\r" + line + " " * 10)
    sys.stdout.flush()


def set_status(code, text):
    with _lock:
        _status[code] = text
        _render()


def log_msg(text):
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
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.tokens = None
    return _local


def _fresh_tokens(st):
    r = st.session.get(URL, timeout=15)
    r.raise_for_status()
    vs, ev, cap, _ = _parse_tokens(r.text)
    return (vs, ev, cap) if vs and cap else None


def test_id(category, student_id_val, reuse=None, dump_path=None):
    """reuse=None -> use the global REUSE_TOKENS. dump_path saves the raw response HTML."""
    st = _state()
    if reuse is None:
        reuse = REUSE_TOKENS
    for attempt in range(1, 4):
        try:
            if not reuse:
                st.tokens = None
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
            if dump_path:
                with open(dump_path, "w", encoding="utf-8") as f:
                    f.write(r.text)

            nvs, nev, ncap, soup = _parse_tokens(r.text)
            st.tokens = (nvs, nev, ncap) if nvs and ncap else None

            name_el = soup.find(id="StudentNameLabel")
            fac_el = soup.find(id="FacultyNameLabel")
            name = name_el.get_text(" ", strip=True) if name_el else ""
            faculty = fac_el.get_text(" ", strip=True) if fac_el else ""

            ok = bool(name and "NOT FOUND" not in name and "خطأ" not in name)
            return ok, name, faculty
        except Exception as e:
            st.tokens = None
            log_error(category, student_id_val, "request", e, note=f"attempt {attempt}/3")
    log_error(category, student_id_val, "GAVE UP", note="treated as invalid")
    return False, "", ""


# --- Search ---------------------------------------------------------------
def _result(code, name, lowest, s_name="", faculty=""):
    return {"category_code": code, "category_name": name,
            "lowest_valid_id": lowest, "student_name": s_name, "faculty": faculty}


def run_category_search(code, name):
    global REUSE_TOKENS

    # --- Step 1: verify the known ID, fresh tokens, then reused tokens ---
    set_status(code, f"verifying {KNOWN_VALID_ID}")
    ok, s_name, faculty = test_id(code, KNOWN_VALID_ID, reuse=False,
                                  dump_path=f"debug_C{code}_fresh.html")
    log_msg(f"[{name}] Known ID {KNOWN_VALID_ID} (fresh tokens): "
            f"{'VALID' if ok else 'INVALID'} | name='{s_name}' faculty='{faculty}'")
    if not ok:
        log_msg(f"[{name}] Not valid in this category. See debug_C{code}_fresh.html")
        set_status(code, "known ID not valid here")
        return _result(code, name, "NOT FOUND")

    # Token reuse check: this POST uses the tokens from the previous POST response
    ok2, _, _ = test_id(code, KNOWN_VALID_ID, reuse=True,
                        dump_path=f"debug_C{code}_reused.html")
    log_msg(f"[{name}] Token reuse check: {'OK' if ok2 else 'FAILED, disabling reuse'}")
    if not ok2:
        with _lock:
            REUSE_TOKENS = False

    # --- Step 2: scan DOWN from the known ID, tracking the lowest valid ---
    lowest, lowest_name, lowest_fac = KNOWN_VALID_ID, s_name, faculty
    valid_found = 1
    invalid_run = 0
    cur = KNOWN_VALID_ID - 1

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        while cur >= 1 and invalid_run < STOP_AFTER_INVALID:
            batch = list(range(cur, max(cur - SCAN_WORKERS, 0), -1))  # descending
            set_status(code, f"DOWN ID {cur} | lowest {lowest} | valid {valid_found} | "
                             f"invalid run {invalid_run}/{STOP_AFTER_INVALID}")
            results = list(pool.map(lambda i: test_id(code, i), batch))
            for sid, (ok, n, f) in zip(batch, results):
                if ok:
                    lowest, lowest_name, lowest_fac = sid, n, f
                    valid_found += 1
                    invalid_run = 0
                else:
                    invalid_run += 1
            cur -= SCAN_WORKERS

    set_status(code, f"DONE {lowest}")
    log_msg(f"[{name}] Lowest valid ID: {lowest} | {lowest_name} | {lowest_fac} "
            f"({valid_found} valid IDs found)")
    return _result(code, name, lowest, lowest_name, lowest_fac)


def main():
    results = []
    print("=== Admission ID Boundary Finder ===")
    print(f"Known valid ID: {KNOWN_VALID_ID} | errors logged to {ERROR_LOG}\n")
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