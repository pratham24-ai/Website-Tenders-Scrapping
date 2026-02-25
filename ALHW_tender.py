#Website-http://andssw1.and.nic.in/alhw/alhw-tender.php
#title-Andaman Lakshdweep Harbour Work

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
MONGO_DB_COLLECTION = "alhw_tenders"

# ---------------- Target URLs ----------------
URL = "http://andssw1.and.nic.in/alhw/alhw-tender.php"

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

    table = soup.find("div", class_="col-md-12")
    if not table:
        print("Table not found")
        return []

    tenders = []

    rows = table.find_all("tr")

    for row in rows[1:]:  # skip header row
        cols = row.find_all(["td"])

        if len(cols) < 32:
            continue

        inviting_officer = cols[1].get_text(strip=True)
        sector = cols[2].get_text(strip=True)
        state = cols[3].get_text(strip=True)
        currency = cols[4].get_text(strip=True)
        per_qual = cols[5].get_text(strip=True)
        pincode = cols[6].get_text(strip=True)
        id = cols[7].get_text(strip=True)
        prebid_date = cols[8].get_text(strip=True)
        ref_no = cols[9].get_text(strip=True)
        tender_title = cols[10].get_text(strip=True)
        description = cols[11].get_text(strip=True)
        location = cols[12].get_text(strip=True)
        inviting_off_address = cols[13].get_text(strip=True)
        fee = cols[14].get_text(strip=True)
        value = cols[15].get_text(strip=True)
        emd = cols[16].get_text(strip=True)
        public_date = cols[17].get_text(strip=True)
        doc_start_date = cols[18].get_text(strip=True)
        doc_end_date = cols[19].get_text(strip=True)
        bidsub_start_date = cols[20].get_text(strip=True)
        bidsub_end_date = cols[21].get_text(strip=True)
        bid_open_date = cols[22].get_text(strip=True)
        form_contract = cols[23].get_text(strip=True)
        prod_cat = cols[24].get_text(strip=True)
        prod_sub_cat = cols[25].get_text(strip=True)
        tender_type = cols[26].get_text(strip=True)
        tender_category = cols[27].get_text(strip=True)
        return_url = cols[28].find("a")
        link_tag=return_url["href"]if return_url else""

        remark = cols[29].get_text(strip=True)

        column_31 = cols[30].find("a")
        column31=column_31["href"]if column_31 else""

        column_32 = cols[31].get_text(strip=True)

        column_33 = cols[32].find("a")
        column33=column_33["href"]if column_33 else""

        tender_json = {
            "inviting_officer":inviting_officer,
            "sector": sector,
            "state": state,
            "currency": currency,
            "per_qual": per_qual,
            "pincode": pincode,
            "id": id,
            "prebid_date": prebid_date,
            "ref_no": ref_no,
            "tender_title": tender_title,
            "description": description,
            "location": location,
            "inviting_off_address": inviting_off_address,
            "fee": fee,
            "value": value,
            "emd": emd,
            "public_date": public_date,
            "doc_start_date": doc_start_date,
            "doc_end_date": doc_end_date,
            "bidsub_start_date": bidsub_start_date,
            "bidsub_end_date": bidsub_end_date,
            "bid_open_date": bid_open_date,
            "form_contract": form_contract,
            "prod_cat": prod_cat,
            "prod_sub_cat": prod_sub_cat,
            "tender_type": tender_type,
            "tender_category": tender_category,
            "link_tag": link_tag,
            "remark":remark,
            "column31":column31,
            "column_32":column_32,
            "column33":column33
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
    print("Scraping alhw tenders...")

    tenders = scrape_tenders()
    print(f"Fetched {len(tenders)} tenders")

    store_in_mongo(tenders)

if __name__ == "__main__":
    main()