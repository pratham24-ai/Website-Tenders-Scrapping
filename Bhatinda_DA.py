import ssl
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util.retry import Retry
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------- MongoDB Config ----------------
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_DB_COLLECTION = "bathinda_dev_auth_tender"

# ---------------- Target URLs ----------------
URL = "https://bdabathinda.in/en/tenders"

# ---------------- Legacy SSL Adapter ----------------
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

# ---------------- Session with Retry ----------------
def get_session():
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = LegacySSLAdapter()
    adapter.max_retries = retries

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": URL,
    })
    return session

# ---------------- Scraper with Pagination ----------------
def BDA():
    session = get_session()
    all_tenders = []
    visited_urls = set()

    next_url = URL 

    while next_url:
        print(f"Fetching: {next_url}")

        try:
            res = session.get(next_url, timeout=(10, 30))
            res.raise_for_status()
        except Exception as e:
            print(f"Request failed: {e}")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.select_one("table.views-table")

        if not table:
            print("No table found. Stopping.")
            break

        rows = table.select("tbody tr")
        print("Rows found on this page:", len(rows))

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            tender = {
                "sr_no": cols[0].get_text(strip=True),
                "description": cols[1].get_text(" ", strip=True),
                "documents": urljoin(URL, cols[2].find("a")["href"]) if cols[2].find("a") else None,
                "last_date_of_submission": cols[3].get_text(strip=True),
                "scraped_at": datetime.utcnow()
            }

            all_tenders.append(tender)

        # Find Next page
        next_link = soup.select_one("li.pager__item--next a")
        if not next_link:
            print("No next page. Pagination finished.")
            break

        next_url = urljoin(URL, next_link["href"])

        if next_url in visited_urls:
            print("Pagination loop detected. Stopping.")
            break

        visited_urls.add(next_url)

    print(f"Total tenders scraped: {len(all_tenders)}")
    return all_tenders
# ---------------- MongoDB Store ----------------
def store_in_mongo(data):
    if not data:
        print("No data found to insert")
        return

    client = MongoClient(MONGO_DB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_DB_COLLECTION]

    collection.delete_many({})
    collection.insert_many(data)

    print(f"Inserted {len(data)} records into MongoDB")

# ---------------- MAIN ----------------
def main():
    print("Scraping Bathinda Development Authority tenders with pagination...")
    tenders = BDA()
    print(f"Fetched {len(tenders)} tenders")
    store_in_mongo(tenders)

if __name__ == "__main__":
    main()
