# main.py — Aleiba 10k influencer scraper (production-ready)
# NOTE: This script only scrapes publicly available pages and public emails.
# Do not use to bypass logins or protected content. Respect platform ToS.

import os
import requests
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

# ========================= CONFIG =========================
GAS_WEBHOOK_URL = os.environ.get("GAS_WEBHOOK_URL")
if not GAS_WEBHOOK_URL:
    raise RuntimeError("GAS_WEBHOOK_URL is not set in the environment.")

# Full hashtag list (replace/extend as needed). Use the 200 you previously generated.
HASHTAGS = [
    "luxurytravel","luxuryvacation","luxuryresort","luxurygetaway","luxuryholidays",
    "luxurytraveler","luxuryretreat","luxuryexperiences","luxuryescape","luxurytour",
    "luxurylifestyle","luxurylife","luxuryworld","highendliving","lifestyleofluxury",
    "luxurymindset","eliteclub","luxurymagazine","forbeslife","millionairelifestyle",
    "luxuryhotels","5starhotel","boutiquehotel","luxurystays","luxuryvilla",
    "luxuryresorts","luxuryhomes","luxuryrealestate","dreamresorts","luxuryproperty",
    "luxuryfashion","designerstyle","hautecouture","fashionelite","luxurystyle",
    "hermes","gucci","louisvuitton","dior","chanel",
    "wealth","millionairemindset","wealthylife","billionairelifestyle","successlifestyle",
    "wealthbuilders","jetsetlife","executivelife","forbeslist","successnetwork",
    # add the rest of your 200 hashtags here...
]

TARGET_TOTAL = 10000        # total unique influencers desired across all hashtags
CHUNK_SIZE = 20             # how many influencer records to collect per "page" attempt
CONCURRENCY = 3             # parallel hashtag workers (keep low on free Render)
MIN_FOLLOWERS = 10000       # filter threshold (if your scraper can provide)
POST_BATCH_SIZE = 10        # how many rows to send to Google Sheets in one POST
REQUEST_TIMEOUT = 30        # seconds for HTTP requests
RETRIES = 3                 # retry attempts for network calls
SLEEP_BETWEEN_BATCHES = 2   # seconds between batch posts
SLEEP_BETWEEN_HASHTAGS = 5  # small pause when switching hashtags
CHECKPOINT_FILE = "processed_checkpoint.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36"
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# ========================= UTILITIES =========================
def save_checkpoint(state):
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        print("Warning: could not save checkpoint:", e)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def short(x):
    return (x[:80] + "...") if x and len(x) > 80 else x

# ========================= SIMPLE PUBLIC EMAIL EXTRACTOR =========================
def find_email_in_html(html):
    if not html:
        return None
    matches = EMAIL_RE.findall(html)
    # prefer business emails (no gmail/yahoo?) — we keep all for now
    return matches[0] if matches else None

def fetch_url_text(url, timeout=REQUEST_TIMEOUT):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        # network errors are common; return None
        return None

def extract_email_from_profile_url(profile_url):
    """
    Attempts to extract a public email from:
    - profile page HTML (bio text)
    - linked website found on profile (if present)
    NOTE: only works for truly public pages.
    """
    if not profile_url:
        return None
    html = fetch_url_text(profile_url)
    if not html:
        return None
    # quick search for email in profile HTML
    email = find_email_in_html(html)
    if email:
        return email
    # try to find a linked website (href) and check that page
    # naive approach: find first href that looks external
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html)
    for href in hrefs:
        parsed = urlparse(href)
        if parsed.scheme and parsed.netloc and "instagram" not in parsed.netloc:
            # follow this external link
            url_candidate = href if parsed.scheme else urljoin(profile_url, href)
            ext_html = fetch_url_text(url_candidate)
            if ext_html:
                email = find_email_in_html(ext_html)
                if email:
                    return email
    return None

# ========================= SCRAPER CORE (IMPLEMENT PLATFORM LOGIC HERE) =========================
def scrape_hashtag_public(hashtag, after_cursor=None, count=CHUNK_SIZE):
    """
    This function must be implemented with your platform scraping logic.
    For safety I include a placeholder that simulates results.
    Replace the body to:
      - query platform endpoints you are allowed to use, or
      - use a scraping method that does not bypass login,
      - return a list of influencers: {username, followers, profile_url, post_url, hashtag}
    """
    # ----------------- START PLACEHOLDER -----------------
    # Simulated data for demonstration. Replace this.
    results = []
    base = int(time.time()) % 100000  # pseudo-unique seed
    for i in range(count):
        handle = f"{hashtag}_user_{(after_cursor or 0) + i + base}"
        results.append({
            "username": handle,
            "followers": MIN_FOLLOWERS + ((after_cursor or 0) + i) * 5,
            "profile_url": f"https://example.com/{handle}",
            "post_url": f"https://example.com/{handle}/post",
            "hashtag": hashtag
        })
    # return results and a dummy next-cursor (or None to stop)
    next_cursor = (after_cursor or 0) + count
    return results, next_cursor
    # ------------------ END PLACEHOLDER ------------------

# ========================= POST TO GOOGLE SHEET (BATCHED + RETRIES) =========================
def post_to_sheet_rows(rows):
    if not rows:
        return True
    for attempt in range(RETRIES):
        try:
            resp = requests.post(GAS_WEBHOOK_URL, json=rows, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            print(f"Posted {len(rows)} rows to Sheet")
            return True
        except Exception as e:
            print(f"post_to_sheet attempt {attempt+1} failed:", e)
            time.sleep(2 ** attempt)
    print(f"Failed to post batch of {len(rows)} rows after {RETRIES} attempts")
    return False

# ========================= MAIN SCRAPE PIPELINE =========================
def run_full_scrape():
    checkpoint = load_checkpoint()
    processed_handles = set(checkpoint.get("processed_handles", []))
    hashtag_positions = checkpoint.get("hashtag_positions", {})  # {hashtag: cursor}
    total_added = checkpoint.get("total_added", 0)

    print("Starting full scrape. Already processed:", len(processed_handles))

    # stop early if we already reached target
    if total_added >= TARGET_TOTAL:
        print("Target already reached:", total_added)
        return

    for hashtag in HASHTAGS:
        if total_added >= TARGET_TOTAL:
            break

        cursor = hashtag_positions.get(hashtag)
        print(f"Processing hashtag {hashtag} from cursor {cursor}. Total added so far: {total_added}")

        # loop paginating this hashtag until we reach per-hashtag need or platform ends
        while total_added < TARGET_TOTAL:
            items, next_cursor = scrape_hashtag_public(hashtag, after_cursor=cursor, count=CHUNK_SIZE)
            if not items:
                print("No items returned for", hashtag)
                break

            rows_to_post = []
            new_handles = []
            for it in items:
                handle = (it.get("username") or "").lower()
                if not handle:
                    continue
                if handle in processed_handles:
                    continue
                # optional follower filter
                followers = int(it.get("followers") or 0)
                if followers < MIN_FOLLOWERS:
                    continue

                profile_url = it.get("profile_url") or it.get("post_url") or ""
                # attempt to find a public email from profile or linked website
                email = extract_email_from_profile_url(profile_url)
                # Build row expected by GAS doPost: username, followers, email, post_url, hashtag
                row = {
                    "username": it.get("username"),
                    "followers": followers,
                    "email": email or "",
                    "post_url": it.get("post_url") or "",
                    "hashtag": hashtag
                }
                rows_to_post.append(row)
                new_handles.append(handle)

                # batch send when enough rows collected
                if len(rows_to_post) >= POST_BATCH_SIZE:
                    success = post_to_sheet_rows(rows_to_post)
                    if not success:
                        print("Stopping due to repeated post failures.")
                        save_checkpoint({
                            "processed_handles": list(processed_handles),
                            "hashtag_positions": hashtag_positions,
                            "total_added": total_added
                        })
                        return
                    total_added += len(rows_to_post)
                    # mark handles processed
                    processed_handles.update(new_handles)
                    new_handles = []
                    rows_to_post = []
                    save_checkpoint({
                        "processed_handles": list(processed_handles),
                        "hashtag_positions": hashtag_positions,
                        "total_added": total_added
                    })
                    time.sleep(SLEEP_BETWEEN_BATCHES)

                if total_added >= TARGET_TOTAL:
                    break

            # send any leftover rows
            if rows_to_post:
                success = post_to_sheet_rows(rows_to_post)
                if not success:
                    print("Stopping due to repeated post failures.")
                    save_checkpoint({
                        "processed_handles": list(processed_handles),
                        "hashtag_positions": hashtag_positions,
                        "total_added": total_added
                    })
                    return
                total_added += len(rows_to_post)
                processed_handles.update(new_handles)
                save_checkpoint({
                    "processed_handles": list(processed_handles),
                    "hashtag_positions": hashtag_positions,
                    "total_added": total_added
                })
                time.sleep(SLEEP_BETWEEN_BATCHES)

            # update cursor for this hashtag (persist progress)
            if next_cursor is None:
                break
            cursor = next_cursor
            hashtag_positions[hashtag] = cursor

            # small pause between pages to avoid rate-limiting
            time.sleep(1)
            # allow stopping early if reached target
            if total_added >= TARGET_TOTAL:
                break

        print(f"Finished processing hashtag {hashtag}, total_added: {total_added}")
        # small pause between hashtags
        time.sleep(SLEEP_BETWEEN_HASHTAGS)

    print("Scrape complete. total_added:", total_added)
    save_checkpoint({
        "processed_handles": list(processed_handles),
        "hashtag_positions": hashtag_positions,
        "total_added": total_added
    })

# ========================= ENTRY POINT =========================
if __name__ == "__main__":
    run_full_scrape()
