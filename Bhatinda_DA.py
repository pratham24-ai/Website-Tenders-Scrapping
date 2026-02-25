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
        "Connection": "close",
    })
    return session

# ---------------- Scraper ----------------
def BDA():
    session = get_session()
    all_tenders = []
    page = 0
    last_first_srno = None

    while True:
        # paginated_url = f"{URL}?page={page}"
        # print(f"Fetching page {page}: {paginated_url}")

        # try:
        #     res = session.get(paginated_url, timeout=(10, 30))
        #     res.raise_for_status()
        # except Exception as e:
        #     print(f"Request failed: {e}")
        #     break

        soup = BeautifulSoup(URL.text, "html.parser")
        table = soup.select_one("table.views-table")

        if not table:
            print("No table found. Stopping pagination.")
            break

        rows = table.find_all("tbody tr")
        if not rows:
            print("No rows found. Stopping pagination.")
            break

        # first_srno = rows[0].find_all("td")[0].get_text(strip=True)
        # if first_srno == last_first_srno:
        #     print("Pagination loop detected (same data again). Stopping.")
        #     break

        # last_first_srno = first_srno

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            all_tenders.append({
                "Sr_No": cols[0].get_text(strip=True),
                "description": cols[1].get_text(" ", strip=True),
                "documents":cols[2].get(""),
                "last_date_of_submission": cols[4].get_text(strip=True),
                "scraped_at": datetime.utcnow()
            })

        page += 1

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
    print("Scraping hafed tenders...")
    tenders = BDA()
    print(f"Fetched {len(tenders)} tenders")
    store_in_mongo(tenders)

if __name__ == "__main__":
    main()