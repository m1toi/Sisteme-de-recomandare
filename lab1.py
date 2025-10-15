from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *
import pandas as pd
import math
import re

client = RecombeeClient(
    'm1toi-adidas-sales-db',
    'Cky2nfVve4garUbQ0EKKf5S6WCc5oaekLHrq4AVBRYx34Fc6wbMXkuVWDD1YjpX1',
    region=Region.EU_WEST
)


df = pd.read_csv("Adidas_Sales_US_DS.csv")

print(f"✅ Fișier încărcat: {len(df)} rânduri.\n")


requests = [
    AddItemProperty("Retailer", "string"),
    AddItemProperty("RetailerID", "int"),
    AddItemProperty("InvoiceDate", "string"),
    AddItemProperty("Region", "string"),
    AddItemProperty("State", "string"),
    AddItemProperty("City", "string"),
    AddItemProperty("Product", "string"),
    AddItemProperty("PricePerUnit", "double"),
    AddItemProperty("UnitsSold", "double"),
    AddItemProperty("TotalSales", "double"),
    AddItemProperty("OperatingProfit", "double"),
    AddItemProperty("OperatingMargin", "double"),
    AddItemProperty("SalesMethod", "string"),
]

client.send(Batch(requests))
print(" Proprietățile au fost definite în Recombee.\n")


def to_float(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.replace('$', '').replace(',', '').strip()
    try:
        return float(value)
    except ValueError:
        return None


requests = []

for index, row in df.iterrows():
    item_id = f"sale_{index}"

    requests.append(AddItem(item_id))

    values = {}

    if isinstance(row.get("Retailer"), str):
        values["Retailer"] = row["Retailer"].strip()

    if pd.notna(row.get("RetailerID")):
        try:
            values["RetailerID"] = int(row["RetailerID"])
        except:
            pass

    if isinstance(row.get("InvoiceDate"), str):
        values["InvoiceDate"] = row["InvoiceDate"].strip()

    for field in ["Region", "State", "City", "Product", "SalesMethod"]:
        if isinstance(row.get(field), str):
            values[field] = row[field].strip()

    for field in ["PricePerUnit", "UnitsSold", "TotalSales", "OperatingProfit", "OperatingMargin"]:
        val = to_float(row.get(field))
        if val is not None:
            values[field] = val

    if values:
        requests.append(SetItemValues(item_id, values, cascade_create=True))


CHUNK = 500
total_batches = math.ceil(len(requests) / CHUNK)

for i in range(0, len(requests), CHUNK):
    batch = requests[i:i + CHUNK]
    try:
        client.send(Batch(batch))
        print(f" Batch {i//CHUNK + 1}/{total_batches} trimis ({len(batch)} requesturi)")
    except APIException as e:
        print(f"️ Eroare la batch {i//CHUNK + 1}: {e}")

print(f"\n Încărcare completă: {len(df)} itemi + proprietăți trimise în Recombee!")
