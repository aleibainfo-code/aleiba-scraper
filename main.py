# =============================
# ALEIBA Multi-Platform Influencer Scraper (Instagram, TikTok, YouTube, Facebook)
# =============================

import gspread
import re, requests, time
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import dns.resolver
from validate_email import validate_email
import os, json

# STEP 1: Load Google service account from Render secret
GOOGLE_JSON = os.environ.get("GOOGLE_SERVICE_JSON")
if not GOOGLE_JSON:
    raise Exception("Please set GOOGLE_SERVICE_JSON as an environment variable in Render")

service_account_info = json.loads(GOOGLE_JSON)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)

# STEP 2: Open Google Sheets
HASHTAG_SHEET = "Aleiba_Hashtags"
INFLUENCERS_SHEET = "Aleiba_Influencers"

sheet_hashtags = client.open(HASHTAG_SHEET).sheet1
sheet_influencers = client.open(INFLUENCERS_SHEET).sheet1

print("✅ Connected to Google Sheets")

# STEP 3: Helper functions
def verify_email_address(email):
    try:
        domain = email.split('@')[1]
        dns.resolver.resolve(domain, 'MX')
        return True
    except:
        return False

def scrape_emails_from_url(url):
    emails_found = set()
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            possible_emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            for email in possible_emails:
                if verify_email_address(email):
                    emails_found.add(email)
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
    return list(emails_found)

def existing_emails():
    try:
        emails = sheet_influencers.col_values(2)
        return set(e.strip().lower() for e in emails if e)
    except:
        return set()

# STEP 4: Build profile URLs for each platform
def build_platform_urls(hashtag):
    hashtag_clean = hashtag.strip("#")
    return {
        "Instagram": f"https://www.instagram.com/explore/tags/{hashtag_clean}",
        "TikTok": f"https://www.tiktok.com/tag/{hashtag_clean}",
        "YouTube": f"https://www.youtube.com/results?search_query={hashtag_clean}",
        "Facebook": f"https://www.facebook.com/hashtag/{hashtag_clean}"
    }

# STEP 5: Main scraper
def run_scraper(min_followers=5000):
    hashtags = sheet_hashtags.col_values(1)[1:]
    print(f"🔍 Found {len(hashtags)} hashtags/URLs to process.")
    old_emails = existing_emails()
    new_count = 0

    for hashtag in hashtags:
        platform_urls = build_platform_urls(hashtag)
        for platform, url in platform_urls.items():
            print(f"⏳ Scraping {platform}: {url}")
            emails = scrape_emails_from_url(url)
            for email in emails:
                if email.lower() not in old_emails:
                    sheet_influencers.append_row([f"{platform} - {hashtag}", email, "Verified ✅"])
                    old_emails.add(email.lower())
                    new_count += 1
                    print(f"✅ Added new verified email: {email}")
                else:
                    print(f"⚠️ Skipped duplicate: {email}")
            time.sleep(2)  # polite delay

    print(f"\n🎉 Done! Added {new_count} new verified influencer emails.")

# STEP 6: Run scraper
run_scraper()
