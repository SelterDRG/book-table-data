import json
import time
import re
from typing import Optional, Tuple, Dict, List
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BookRatingsBot/1.0; +https://github.com/SelterDRG/book-table-data)"
}

# Two JSON files to update
BOOKS_FILES = [
    "books(full-list).json",
    "books(book-club).json",
]

# ---------- Helpers ----------

def _clean_int(text: str) -> int:
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        raise ValueError(f"Could not parse int from: {text!r}")
    return int(digits)

def _clean_float(text: str) -> float:
    t = text.strip().replace(",", ".")
    return float(t)

def pick_source_url(book: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (source, url) where source is 'goodreads' or 'royalroad' or None.
    Rules:
      - if book['url'] is not None -> use it
      - else if vendors.rr.url exists -> use RoyalRoad URL
      - else -> None
    """
    primary = book.get("url")
    if primary:
        return "goodreads", primary

    vendors = book.get("vendors") or {}
    rr = vendors.get("rr") or {}
    rr_url = rr.get("url")
    if rr_url:
        return "royalroad", rr_url

    return None, None

def _format_rating(rating: float) -> str:
    return f"{rating:.2f}".rstrip("0").rstrip(".")

# ---------- Goodreads scraping ----------

def scrape_goodreads(url: str) -> Tuple[float, int]:
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rating_div = soup.find("div", class_="RatingStatistics__rating")
    if not rating_div:
        rating_div = soup.find("div", class_=lambda x: x and "RatingStatistics__rating" in x)
    if not rating_div:
        raise ValueError("Goodreads: rating element not found")

    rating_value = _clean_float(rating_div.get_text(strip=True))

    count_span = soup.find("span", {"data-testid": "ratingsCount"})
    if not count_span:
        raise ValueError("Goodreads: ratingsCount element not found")

    count_text = count_span.get_text(" ", strip=True)
    rating_count = _clean_int(count_text)

    return rating_value, rating_count

# ---------- RoyalRoad scraping ----------

def scrape_royalroad(url: str) -> Tuple[float, int]:
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    score_span = soup.select_one('span[aria-label*="stars"]')
    if not score_span:
        raise ValueError("RoyalRoad: score span with aria-label not found")

    aria = (score_span.get("aria-label") or "").strip()
    data_content = (score_span.get("data-content") or "").strip()

    score_value: Optional[float] = None

    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*stars", aria, flags=re.IGNORECASE)
    if m:
        score_value = _clean_float(m.group(1))
    else:
        m2 = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*/\s*5", data_content)
        if m2:
            score_value = _clean_float(m2.group(1))

    if score_value is None:
        raise ValueError(
            f"RoyalRoad: could not parse score from aria-label={aria!r} data-content={data_content!r}"
        )

    # Ratings count: find "Ratings :" then use the next <li>
    lis = soup.select("ul.list-unstyled li")
    if not lis:
        lis = soup.find_all("li")

    rating_count: Optional[int] = None
    for i, li in enumerate(lis):
        key = li.get_text(" ", strip=True)
        key_norm = re.sub(r"\s+", " ", key).strip().lower()

        if key_norm in ("ratings :", "ratings:", "ratings"):
            if i + 1 < len(lis):
                val_text = lis[i + 1].get_text(" ", strip=True)
                rating_count = _clean_int(val_text)
                break

    if rating_count is None:
        raise ValueError("RoyalRoad: could not locate 'Ratings' count in stats list")

    return score_value, rating_count

# ---------- Core logic ----------

def load_json(path: str) -> List[dict]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, data: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def build_targets(all_books: List[dict]) -> Dict[str, str]:
    """
    Returns dict url -> source ("goodreads"/"royalroad") for all scrapeable entries,
    deduped across both files.
    """
    targets: Dict[str, str] = {}
    for book in all_books:
        source, url = pick_source_url(book)
        if not source or not url:
            continue
        # If the same URL somehow appears with different sources, keep the first seen.
        targets.setdefault(url, source)
    return targets

def scrape_targets(targets: Dict[str, str], delay_seconds: int = 2) -> Dict[str, Tuple[str, str]]:
    cache: Dict[str, Tuple[str, str]] = {}
    failures = 0
    total = len(targets)

    for idx, (url, source) in enumerate(targets.items(), start=1):
        try:
            if source == "goodreads":
                rating, count = scrape_goodreads(url)
            else:
                rating, count = scrape_royalroad(url)

            cache[url] = (_format_rating(rating), str(count))

        except Exception as e:
            failures += 1
            print(f"    ERROR: {e}")

        time.sleep(delay_seconds)

    if total > 0 and failures > total * 0.3:
        print("\nWARNING: Many failures — site HTML may have changed or requests were blocked.")

    return cache

def apply_cache(books: List[dict], cache: Dict[str, Tuple[str, str]]) -> bool:
    changed = False
    for book in books:
        source, url = pick_source_url(book)
        if not url or url not in cache:
            continue

        rating_str, count_str = cache[url]
        if book.get("rating", "") != rating_str or book.get("r_count", "") != count_str:
            book["rating"] = rating_str
            book["r_count"] = count_str
            changed = True
    return changed

def main():
    # Load both lists
    data_by_file: Dict[str, List[dict]] = {}
    for path in BOOKS_FILES:
        print(f"Loading {path}...")
        data_by_file[path] = load_json(path)

    # Build a combined list of all books for deduping
    combined = []
    for books in data_by_file.values():
        combined.extend(books)

    targets = build_targets(combined)
    if not targets:
        print("No scrapeable entries found across both files.")
        return

    # Scrape each unique URL once
    cache = scrape_targets(targets, delay_seconds=2)

    # Apply results back to each file, write only if changed
    any_written = False
    for path, books in data_by_file.items():
        changed = apply_cache(books, cache)
        if changed:
            write_json(path, books)
            any_written = True
            print(f"Wrote updates to {path}")
        else:
            print(f"No changes for {path}")

    if any_written:
        print("\nDone: updates written.")
    else:
        print("\nDone: no updates needed.")

if __name__ == "__main__":
    main()
