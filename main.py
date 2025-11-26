import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor

# ========================= CONFIG =========================
GAS_WEBHOOK_URL = os.environ.get("GAS_WEBHOOK_URL")  # Read from Render env
if not GAS_WEBHOOK_URL:
    raise RuntimeError("GAS_WEBHOOK_URL is not set! Check Render environment variables.")

HASHTAGS = [
    "luxurytravel",
    "luxuryvacation",
    "luxuryresort",
    "luxurygetaway",
    "luxuryholidays"
]

LIMIT = 50           # influencers per hashtag for testing
CHUNK_SIZE = 10      # smaller batches for faster response
CONCURRENCY = 5      # parallel threads
MIN_FOLLOWERS = 10000
RETRIES = 3          # retry attempts for posting

# ========================= SIMULATED SCRAPER =========================
def scrape_hashtag(hashtag, offset=0, limit=CHUNK_SIZE):
    """
    Simulate scraping influencers.
    Returns list of dicts with: username, followers, email, post_url, hashtag
    """
    influencers = []
    for i in range(limit):
        influencers.append({
            "username": f"{hashtag}_user_{offset + i}",
            "followers": MIN_FOLLOWERS + (offset + i) * 10,
            "email": f"{hashtag}_user_{offset + i}@example.com",
            "post_url": f"https://socialmedia.com/{hashtag}_user_{offset + i}",
            "hashtag": hashtag
        })
    return influencers

# ========================= POST TO SHEET WITH RETRIES =========================
def post_to_sheet(batch, retries=RETRIES):
    for attempt in range(retries):
        try:
            res = requests.post(GAS_WEBHOOK_URL, json=batch, timeout=30)
            res.raise_for_status()
            print(f"Posted {len(batch)} rows to Sheet")
            return
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2)  # wait before retry
    print(f"Failed to post {len(batch)} rows after {retries} attempts")

# ========================= PROCESS SINGLE HASHTAG =========================
def process_hashtag(hashtag):
    offset = 0
    total_scraped = 0
    while total_scraped < LIMIT:
        batch = scrape_hashtag(hashtag, offset, CHUNK_SIZE)
        if not batch:
            break
        post_to_sheet(batch)
        total_scraped += len(batch)
        offset += len(batch)
        time.sleep(2)  # rate limiting between batches

# ========================= MAIN =========================
def main():
    print("Starting Aleiba luxury influencer scrape...")
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        executor.map(process_hashtag, HASHTAGS)
    print("Scraping complete!")

if __name__ == "__main__":
    main()
