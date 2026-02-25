#website => https://ayurveda.hp.gov.in/Tenders.aspx
import ssl
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import os
from dotenv import load_dotenv

load_dotenv()
# ---------------- MongoDB Config ----------------
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_DB_COLLECTION = "ayurveda_tenders"

# ---------------- Target URLs ----------------
URL = "https://ayurveda.hp.gov.in/Tenders.aspx"

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

def get_session():
    session = requests.Session()
    session.mount("https://", LegacySSLAdapter())
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Connection": "close",
    })
    return session

# ---------------- Scraper ----------------
def Ayurved():
    session = get_session()
    response = session.get(URL, timeout=(10, 20))
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    tenders = []           
    seen = set()

    # 🔥 STRICT selector based on your DOM
    links = soup.select("div.bs-docs-example table.table tbody tr td a[href]")

    print("Total <a> tags found:", len(links))

    for a in links:
        title = a.get_text(strip=True)
        href = a.get("href")

        if not title or not href:
            continue

        tender_url = urljoin(URL  , href)
        key = (title, tender_url)

        if key in seen:
            continue

        seen.add(key)

        tenders.append({
            "tender_title": title,
            "tender_url": tender_url,
            "scraped_at": datetime.utcnow()
        })

    print("Valid tenders captured:", len(tenders))
    return tenders


# ---------------- MongoDB Store ----------------
def store_in_mongo(data):
    if not data:
        print(" No data found to insert")
        return

    client = MongoClient(MONGO_DB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_DB_COLLECTION]

    collection.delete_many({})  # optional: clear previous data
    collection.insert_many(data)

    print(f"Inserted {len(data)} records into MongoDB")

# ---------------- MAIN ----------------
def main():
    print("Scraping APL tenders...")

    tenders = Ayurved()
    print(f"Fetched {len(tenders)} tenders")

    store_in_mongo(tenders)

if __name__ == "__main__":
    main()