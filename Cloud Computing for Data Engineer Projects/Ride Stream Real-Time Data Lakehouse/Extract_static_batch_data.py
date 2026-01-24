import requests
import pandas as pd

# -------- DIM: Zone Lookup --------
url_zone = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
df_zone = pd.read_csv(url_zone)

# -------- DIM: Rate Codes --------
rate_codes = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}
df_rate = pd.DataFrame(list(rate_codes.items()), columns=["RateCodeID", "RateDescription"])

# -------- DIM: Payment Types --------
payment_types = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}
df_payment = pd.DataFrame(list(payment_types.items()), columns=["PaymentTypeID", "PaymentDescription"])

# -------- DIM: Vendors --------
vendors = {
    1: "Creative Mobile Technologies",
    2: "VeriFone"
}
df_vendor = pd.DataFrame(list(vendors.items()), columns=["VendorID", "VendorName"])

# Save locally
df_zone.to_json("nyc_dim_zone.json", orient="records", indent=2)
df_rate.to_json("nyc_dim_rate.json", orient="records", indent=2)
df_payment.to_json("nyc_dim_payment.json", orient="records", indent=2)
df_vendor.to_json("nyc_dim_vendor.json", orient="records", indent=2)

print("Dimension data locally as JSON")
