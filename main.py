import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests

# --- Configuration ---
URL = os.getenv("TARGET_URL", "https://daleel.admission.gov.sd/result2026/Result_2026.aspx")

# ID Range configuration (8-digit IDs from 20000000 to 21000000)
RANGE_START = 20000000
RANGE_END = 21000000

# Number of concurrent worker threads
MAX_WORKERS = 6

CATEGORIES = {
    "1": "قبول عام",
    "3": "قبول على النفقة الخاصة",
    "5": "قبول أبناء العاملين",
}

OUTPUT_CSV = "found_students.csv"


def get_value(soup, element_id):
  element = soup.find("input", {"id": element_id})
  return element.get("value", "") if element else ""


def test_single_id(session, category_code, category_name, student_id):
  """Fetches a fresh form (Viewstate & Captcha) and tests a single ID."""
  max_retries = 3
  for attempt in range(max_retries):
    try:
      # GET fresh form
      response = session.get(URL, timeout=10)
      response.raise_for_status()

      soup = BeautifulSoup(response.text, "html.parser")
      captcha_element = soup.find("span", {"id": "CaptchaDiv"})

      if not captcha_element:
        return None

      captcha = captcha_element.get_text(strip=True)

      # Build POST payload
      payload = {
          "__VIEWSTATE": get_value(soup, "__VIEWSTATE"),
          "__EVENTVALIDATION": get_value(soup, "__EVENTVALIDATION"),
          "DropDownList1": category_code,
          "TextBox1": str(student_id),
          "CaptchaInput": captcha,
          "Button1": "عرض النتيجة",
      }

      # Submit result query
      result = session.post(
          URL, data=payload, headers={"Referer": URL}, timeout=10
      )
      result.raise_for_status()

      result_soup = BeautifulSoup(result.text, "html.parser")

      # Extract student details
      name_el = result_soup.find("span", {"id": "StudentNameLabel"})
      faculty_el = result_soup.find("span", {"id": "FacultyNameLabel"})

      student_name = name_el.get_text(" ", strip=True) if name_el else ""
      faculty_name = faculty_el.get_text(" ", strip=True) if faculty_el else ""

      # Check if valid result is returned
      if (
          student_name
          and "NOT FOUND" not in student_name
          and "رقم الاستمارة" not in student_name
          and len(student_name) > 2
      ):
        return {
            "id": student_id,
            "category_code": category_code,
            "category_name": category_name,
            "student_name": student_name,
            "faculty": faculty_name,
        }

      return False  # Checked, but invalid ID

    except (requests.RequestException, Exception):
      if attempt == max_retries - 1:
        return None
      time.sleep(1)  # Brief backoff before retry

  return None


def worker_scan_chunk(chunk_id, start_id, end_id):
  """Worker thread scanning a specific partition of IDs across all categories."""
  session = requests.Session()
  local_found = []
  print(
      f"[Thread-{chunk_id}] Starting scan for range {start_id} to {end_id}..."
  )

  for student_id in range(start_id, end_id + 1):
    # Optional progress log every 500 IDs per thread
    if student_id % 500 == 0:
      print(f"[Thread-{chunk_id}] Progress: checked up to ID {student_id}")

    for cat_code, cat_name in CATEGORIES.items():
      result = test_single_id(session, cat_code, cat_name, student_id)
      if result is True:
        # Invalid ID, move to next category or ID
        continue
      elif result and isinstance(result, dict):
        # Valid student found!
        print(
            f"\n[FOUND!] ID: {student_id} | Category: {cat_name} | Name:"
            f" {result['student_name']}\n"
        )
        local_found.append(result)
        # Once a valid record is found for this ID in a category, we can break or continue

  print(f"[Thread-{chunk_id}] Completed range {start_id} to {end_id}.")
  return local_found


def main():
  print("=" * 60)
  print("Starting Multi-Threaded Admission ID Scanner")
  print(f"Range: {RANGE_START} to {RANGE_END}")
  print(f"Threads: {MAX_WORKERS}")
  print("=" * 60)

  # Partition the ID range evenly across MAX_WORKERS threads
  total_ids = (RANGE_END - RANGE_START) + 1
  chunk_size = total_ids // MAX_WORKERS

  ranges = []
  for i in range(MAX_WORKERS):
    start = RANGE_START + (i * chunk_size)
    end = RANGE_START + ((i + 1) * chunk_size) - 1 if i < MAX_WORKERS - 1 else RANGE_END
    ranges.append((i + 1, start, end))

  all_found_records = []

  # Execute threads using ThreadPoolExecutor
  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [
        executor.submit(worker_scan_chunk, chunk_id, start, end)
        for chunk_id, start, end in ranges
    ]

    for future in as_completed(futures):
      try:
        data = future.result()
        if data:
          all_found_records.extend(data)
      except Exception as exc:
        print(f"[ERROR] Thread generated an exception: {exc}")

  # Save results to CSV
  print("\n" + "=" * 60)
  print(f"Scan complete. Saving {len(all_found_records)} records to {OUTPUT_CSV}")
  print("=" * 60)

  with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8-sig") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow([
        "Student ID",
        "Category Code",
        "Category Name",
        "Student Name",
        "Faculty / Program",
    ])
    for rec in all_found_records:
      writer.writerow([
          rec["id"],
          rec["category_code"],
          rec["category_name"],
          rec["student_name"],
          rec["faculty"],
      ])

  print(f"Results successfully exported to {OUTPUT_CSV}")


if __name__ == "__main__":
  main()
