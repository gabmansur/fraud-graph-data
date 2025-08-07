# %%
# 04_analyze_graph.ipynb
# Sanity check before graph import – See if injected fraud is detectable

import pandas as pd

# Load enriched data
df = pd.read_csv("../data/processed/enriched_transactions.csv")

# Count people sharing the same phone
phone_counts = pd.concat([
    df[["nameOrig", "sender_phone"]].rename(columns={"nameOrig": "user_id", "sender_phone": "phone"}),
    df[["nameDest", "receiver_phone"]].rename(columns={"nameDest": "user_id", "receiver_phone": "phone"})
])
phone_counts = phone_counts.drop_duplicates()
phone_group = phone_counts.groupby("phone").count()
shared_phones = phone_group[phone_group["user_id"] > 1]
print(f"Phones shared by >1 user: {len(shared_phones)}")

# Count people sharing same IP
ip_counts = pd.concat([
    df[["nameOrig", "sender_ip"]].rename(columns={"nameOrig": "user_id", "sender_ip": "ip"}),
    df[["nameDest", "receiver_ip"]].rename(columns={"nameDest": "user_id", "receiver_ip": "ip"})
])
ip_counts = ip_counts.drop_duplicates()
ip_group = ip_counts.groupby("ip").count()
shared_ips = ip_group[ip_group["user_id"] > 1]
print(f"IPs shared by >1 user: {len(shared_ips)}")

# Cross-check: Who shares the same phone AND IP?
merged = pd.merge(phone_counts, ip_counts, on="user_id")
cross_group = merged.groupby(["phone", "ip"]).count()
suspicious_pairs = cross_group[cross_group["user_id"] > 1]
print(f"People sharing both phone AND IP: {len(suspicious_pairs)}")

# Show suspicious users (Optional preview)
if len(suspicious_pairs):
    print("\nSample suspicious overlap:")
    print(merged[(merged["phone"].isin(suspicious_pairs.index.get_level_values(0))) &
                 (merged["ip"].isin(suspicious_pairs.index.get_level_values(1)))])



