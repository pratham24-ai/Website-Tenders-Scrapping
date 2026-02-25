#Website-https://aurangabad.bih.nic.in/notice_category/tenders/
#title-Chief Minister Relife Fund
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
MONGO_DB_COLLECTION = "cmrf_tender_data"

# ---------------- Target URLs ----------------
URL = "https://aurangabad.bih.nic.in/notice_category/tenders/"

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
        "Connection": "close",
    })
    return session

# ---------------- Scraper ----------------
def scrape_tenders():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("tbody")
    if not table:
        print("Table not found")
        return []

    tenders = []

    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all(["td"])

        if len(cols) < 5:
            continue

        title = cols[0].get_text(strip=True)
        description = cols[1].get_text(strip=True)
        start_date=cols[2].get_text(strip=True)
        end_date=cols[3].get_text(strip=True)
        file = cols[4].find("a")
        pdf_link = file["href"] if file else ""

        tender_json = {
            "title": title,
            "description": description,
            "start_date":start_date,
            "end_date":end_date,
            "pdf_link": pdf_link
        }

        tenders.append(tender_json)

    return tenders

# ---------------- MongoDB Store ----------------
def store_in_mongo(data):
    if not data:
        print("No data found to insert")
        return

    client = MongoClient(MONGO_DB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_DB_COLLECTION]

    collection.delete_many({})  # Optional: clear old data
    collection.insert_many(data)

    print(f"Inserted {len(data)} records into MongoDB")

# ---------------- MAIN ----------------
def main():
    print("Scraping ASCL tenders...")

    tenders = scrape_tenders()
    print(f"Fetched {len(tenders)} tenders")

    store_in_mongo(tenders)

if __name__ == "__main__":
    main()