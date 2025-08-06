# %%
# 03_export_for_neo4j.ipynb
# Split enriched transactions into nodes and relationships

import pandas as pd

# Load enriched transaction data
df = pd.read_csv("../data/processed/enriched_transactions.csv")

# Strip leading/trailing whitespace just in case
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 🔹 1. PERSON nodes
senders = df[["nameOrig", "sender_name", "sender_email"]].rename(columns={
    "nameOrig": "user_id", "sender_name": "name", "sender_email": "email"
})

receivers = df[["nameDest", "receiver_name", "receiver_email"]].rename(columns={
    "nameDest": "user_id", "receiver_name": "name", "receiver_email": "email"
})

persons = pd.concat([senders, receivers]).drop_duplicates(subset="user_id")
persons.to_csv("../data/processed/nodes_person.csv", index=False)

# 🔹 2. PHONE nodes
phones = pd.concat([
    df[["sender_phone"]].rename(columns={"sender_phone": "phone"}),
    df[["receiver_phone"]].rename(columns={"receiver_phone": "phone"})
]).drop_duplicates()
phones.to_csv("../data/processed/nodes_phone.csv", index=False)

# 🔹 3. IP nodes
ips = pd.concat([
    df[["sender_ip"]].rename(columns={"sender_ip": "ip"}),
    df[["receiver_ip"]].rename(columns={"receiver_ip": "ip"})
]).drop_duplicates()
ips.to_csv("../data/processed/nodes_ip.csv", index=False)

# 🔹 4. Company nodes
companies = pd.concat([
    df[["sender_company"]].rename(columns={"sender_company": "company"}),
    df[["receiver_company"]].rename(columns={"receiver_company": "company"})
]).drop_duplicates()
companies.to_csv("../data/processed/nodes_company.csv", index=False)

# 🔹 5. SENT relationships
sent_rels = df[["nameOrig", "nameDest", "amount", "type", "isFraud"]].rename(columns={
    "nameOrig": "from",
    "nameDest": "to"
})
sent_rels.to_csv("../data/processed/relationships_sent.csv", index=False)

# 🔹 6. USES_PHONE
uses_phone = pd.concat([
    df[["nameOrig", "sender_phone"]].rename(columns={"nameOrig": "user_id", "sender_phone": "phone"}),
    df[["nameDest", "receiver_phone"]].rename(columns={"nameDest": "user_id", "receiver_phone": "phone"})
]).drop_duplicates()
uses_phone.to_csv("../data/processed/relationships_uses_phone.csv", index=False)

# 🔹 7. LOGGED_FROM (IP)
logged_from = pd.concat([
    df[["nameOrig", "sender_ip"]].rename(columns={"nameOrig": "user_id", "sender_ip": "ip"}),
    df[["nameDest", "receiver_ip"]].rename(columns={"nameDest": "user_id", "receiver_ip": "ip"})
]).drop_duplicates()
logged_from.to_csv("../data/processed/relationships_logged_from.csv", index=False)

# 🔹 8. WORKS_FOR (company)
works_for = pd.concat([
    df[["nameOrig", "sender_company"]].rename(columns={"nameOrig": "user_id", "sender_company": "company"}),
    df[["nameDest", "receiver_company"]].rename(columns={"nameDest": "user_id", "receiver_company": "company"})
]).drop_duplicates()
works_for.to_csv("../data/processed/relationships_works_for.csv", index=False)

print(" All node and relationship CSVs exported to 'data/processed/'")



