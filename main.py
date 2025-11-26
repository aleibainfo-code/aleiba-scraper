import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor

# ========================= CONFIG =========================
GAS_WEBHOOK_URL = os.environ.get("GAS_WEBHOOK_URL")  # Read from Render env
HASHTAGS = [
    "luxurytravel","luxuryvacation","luxuryresort","luxurygetaway","luxuryholidays",
    "luxurytraveler","luxuryretreat","luxuryexperiences","luxuryescape","luxurytour",
    "luxurylifestyle","luxurylife","luxuryworld","highendliving","lifestyleofluxury",
    "luxurymindset","eliteclub","luxurymagazine","forbeslife","millionairelifestyle",
    # ... add remaining hashtags up to 200
]
LIMIT = 10000             # Total influencers to scrape
CHUNK_SIZE = 50           # Posts per request
CONCURRENCY = 5           # Number of parallel threads
MIN_FOLLOWERS = 10000

# ========================= SIMULATED SCRAPER =========================
def scrape_hashtag(hashtag, offset=0, limit=CHUNK_SIZE):
    """
    Replace this function with real scraping logic (Instagram/TikTok)
    Returns list of dicts with: username, followers, email, post_url, hashtag
    """
    influencers = []
    for i in range(limit):
        influencers.append({
            "username": f"{hashtag}_user_{offset + i}",
            "followers": MIN_FOLLOWERS + (offset + i)*10,
            "email": f"{hashtag}_user_{offset + i}@example.com",
            "post_url": f"https://socialmedia.com/{hashtag}_user_{offset + i}",
            "hashtag": hashtag
        })
    return influencers

# ========================= POST TO SHEET =========================
def post_to_sheet(batch):
    try:
        res = requests.post(GAS_WEBHOOK_URL, json=batch, timeout=30)
        res.raise_for_status()
        print(f"Posted {len(batch)} rows to Sheet")
    except Exception as e:
        print(f"Error posting to Sheet: {e}")

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
        time.sleep(1)  # Rate limit

# ========================= MAIN =========================
def main():
    print("Starting Aleiba luxury influencer scrape...")
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        executor.map(process_hashtag, HASHTAGS)
    print("Scraping complete!")

if __name__ == "__main__":
    main()

