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
MONGO_DB_COLLECTION = "assam_petrochemical_tenders"

# ---------------- Target URLs ----------------
URL = "https://assampetrochemicals.co.in/tenders.php"

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
def APL():
    session = get_session()
    response = session.get(URL, timeout=(10, 20))
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    tenders = []

    table = soup.find("table",class_="TextMain")
    if not table:
        print(" Tender table not found")
        return tenders

    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        link_tag = cols[0].find("a", href=True)

        # Preserve multi-line text like "Corrigendum ..."
        title = cols[0].get_text(separator="\n").strip()

        link_url = urljoin(URL, link_tag["href"]) if link_tag else None

        tenders.append({
            "title": title,
            "link_url": link_url,
            "scraped_at": datetime.utcnow()
        })

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

    tenders = APL()
    print(f"Fetched {len(tenders)} tenders")

    store_in_mongo(tenders)

if __name__ == "__main__":
    main()