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
MONGO_DB_COLLECTION = "central_uni_rajasthan_tenders"

# ---------------- Target URLs ----------------
URL = "http://14.139.244.219/tenders"
BASE_URL = "http://14.139.244.219/"

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
def cur():
    session = get_session()
    try:
        response = session.get(URL, timeout=(30, 60))
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(" Failed to fetch tenders:", e)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    container = soup.find("div", class_="innerpage")

    tenders = []

    if not container:
        print("innerpage not found")
        return tenders

    rows = container.find_all("div", class_="views-row")

    for row in rows:
        title = None
        doc_title = None
        pdf_url = None

        #  Tender title (h2)
        h2 = row.find("h2")
        if h2:
            title = h2.get_text(strip=True)

        # PDF link & document title
        link = row.find("a", href=True)
        if link:
            doc_title = link.get_text(strip=True)
            pdf_url = urljoin(BASE_URL, link["href"])

        if title or doc_title or pdf_url:
            tenders.append({
                "title": title,
                "documents": {
                    "doc_title": doc_title,
                    "pdf_url": pdf_url
                },
                "scraped_at": datetime.utcnow()
            })

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
    print("Scraping CUR tenders...")

    tenders = cur()
    print(f"Fetched {len(tenders)} tenders")

    store_in_mongo(tenders)

if __name__ == "__main__":
    main()