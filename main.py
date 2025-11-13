# ======================================
# ALEIBA Influencer Scraper v10.0 (Render-ready)
# Instagram + TikTok + Email verification
# ======================================

import gspread, re, requests, time, random, os, dns.resolver
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from validate_email import validate_email

# ======================================
# STEP 1 — Google Sheets Connection
# ======================================
print("🔗 Connecting to Google Sheets...")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

HASHTAG_SHEET_KEY = "1xrtllFoCpcFVbFQuixwbGBMGrCHEon-TtKFA9sc2D3w"      # Aleiba_Hashtags
INFLUENCER_SHEET_KEY = "1WNflviC1Sts3gDIOOo2WU71ih5jKpq47UDIcobKPHeo"   # Aleiba_Influencers

sheet_hashtags = client.open_by_key(HASHTAG_SHEET_KEY).sheet1
sheet_influencers = client.open_by_key(INFLUENCER_SHEET_KEY).sheet1

print("✅ Connected successfully!")

# ======================================
# STEP 2 — Helper functions
# ======================================

def verify_email_address(email):
    """Check if email domain is valid"""
    try:
        domain = email.split('@')[1]
        dns.resolver.resolve(domain, 'MX')
        return True
    except:
        return False

def existing_emails():
    """Fetch all existing emails to avoid duplicates"""
    try:
        emails = sheet_influencers.col_values(2)  # assuming column B has emails
        return set(e.strip().lower() for e in emails if e)
    except:
        return set()

def scrape_emails_from_url(url):
    """Scrape emails from a given URL"""
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

def find_social_profiles(hashtag):
    """Simulate finding influencer profiles for a hashtag"""
    base_urls = [
        f"https://www.instagram.com/explore/tags/{hashtag.strip('#')}/",
        f"https://www.tiktok.com/tag/{hashtag.strip('#')}/"
    ]
    return base_urls

# ======================================
# STEP 3 — Main scraper
# ======================================
def run_scraper():
    hashtags = sheet_hashtags.col_values(1)[1:]  # skip header
    if not hashtags:
        print("⚠️ No hashtags found. Please add some to Aleiba_Hashtags sheet.")
        return

    print(f"🔍 Found {len(hashtags)} hashtags to process.")
    old_emails = existing_emails()
    new_count = 0
    MAX_PER_RUN = 800  # adjust for Render free tier

    for tag in hashtags:
        print(f"\n► Processing: {tag}")
        profiles = find_social_profiles(tag)
        if not profiles:
            print("  -> No profiles found.")
            continue

        for profile in profiles:
            if new_count >= MAX_PER_RUN:
                print(f"⚠️ Reached daily limit ({MAX_PER_RUN}). Stopping.")
                return

            print(f"  -> Scraping profile: {profile}")
            emails = scrape_emails_from_url(profile)
            for email in emails:
                if email.lower() not in old_emails:
                    sheet_influencers.append_row([profile, email, "Verified ✅"])
                    old_emails.add(email.lower())
                    new_count += 1
                    print(f"     ✅ Added new verified email: {email}")
                else:
                    print(f"     ⚠️ Duplicate skipped: {email}")

            time.sleep(random.uniform(2, 5))  # small delay

    print(f"\n🎉 Done! Added {new_count} new verified influencer emails.")

# ======================================
# STEP 4 — Run the scraper
# ======================================
if __name__ == "__main__":
    run_scraper()
