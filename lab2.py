import pandas as pd
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import (
    AddUser,
    SetUserValues,
    AddUserProperty,
    DeleteUser,
    ListUsers,
    Batch
)

# === Configurare client Recombee ===
client = RecombeeClient(
    'm1toi-adidas-sales-db',
    'Cky2nfVve4garUbQ0EKKf5S6WCc5oaekLHrq4AVBRYx34Fc6wbMXkuVWDD1YjpX1',
    region=Region.EU_WEST
)

df = pd.read_csv("people.csv").fillna('')
df.columns = [col.strip() for col in df.columns]
for col in df.columns:
    df[col] = df[col].astype(str).str.strip()


try:
    users = client.send(ListUsers())
    delete_requests = [DeleteUser(u) for u in users]
    if delete_requests:
        client.send(Batch(delete_requests))
except Exception as e:
    print("Eroare la ștergere useri:", e)

properties = {
    "SalesPerson": "string",
    "Team": "string",
    "Location": "string"
}

for prop, prop_type in properties.items():
    try:
        client.send(AddUserProperty(prop, prop_type))
    except Exception as e:
        print(f"Proprietatea '{prop}' exista ({e})")

requests = []
for _, row in df.iterrows():
    user_id = row["SP ID"]
    requests.append(AddUser(user_id))

client.send(Batch(requests))

requests = []
for _, row in df.iterrows():
    user_id = row["SP ID"]
    props = {
        "SalesPerson": row["Sales person"],
        "Team": row["Team"],
        "Location": row["Location"]
    }
    props = {k: v for k, v in props.items() if v and v != 'nan'}
    requests.append(SetUserValues(user_id, props, cascade_create=False))

client.send(Batch(requests))
