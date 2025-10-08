from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import AddItemProperty
from recombee_api_client.exceptions import ResponseException


client = RecombeeClient(
  'm1toi-adidas-sales-db',
  'Cky2nfVve4garUbQ0EKKf5S6WCc5oaekLHrq4AVBRYx34Fc6wbMXkuVWDD1YjpX1',
  region=Region.EU_WEST
)
def add_property(name, typ):
    print(f"Adaug '{name}' ({typ}) ...")
    try:
        client.send(AddItemProperty(name, typ))
        print(f"'{name}' adaugata cu succes.")
    except ResponseException as e:
        print(f" Eroare la '{name}': {e}")

add_property("Retailer", "string")
add_property("RetailerID", "int")
add_property("InvoiceDate", "string")
add_property("Region", "string")
add_property("State", "string")
add_property("City", "string")
add_property("Product", "string")
add_property("PricePerUnit", "double")
add_property("UnitsSold", "double")
add_property("TotalSales", "double")
add_property("OperatingProfit", "double")
add_property("OperatingMargin", "double")
add_property("SalesMethod", "string")

print("Toate proprietățile au fost procesate!")
