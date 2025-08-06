# %%
# 01_generate_metadata.ipynb
# Step 1 of Fraud Graph Demo – Enrich users with fake metadata

import pandas as pd
import numpy as np
from faker import Faker

# Set up Faker
fake = Faker()
np.random.seed(42)

# Load the PaySim dataset (adjust path if needed)
df = pd.read_csv("../data/raw/PS_20174392719_1491204439457_log.csv")

# reduce data size for testing
df = df.sample(10000, random_state=42)

# Combine all unique users (sender + receiver)
orig_users = df["nameOrig"].unique()
dest_users = df["nameDest"].unique()
all_users = pd.Series(orig_users.tolist() + dest_users.tolist()).unique()

print(f"Total unique users: {len(all_users)}")

# Generate fake metadata for each user
user_meta = {
    "user_id": [],
    "name": [],
    "email": [],
    "phone": [],
    "ip": [],
    "company": [],
}

for user in all_users:
    user_meta["user_id"].append(user)
    user_meta["name"].append(fake.name())
    user_meta["email"].append(fake.email())
    user_meta["phone"].append(fake.phone_number())
    user_meta["ip"].append(fake.ipv4_public())
    user_meta["company"].append(fake.company())

meta_df = pd.DataFrame(user_meta)

# Inject Suspicious Patterns: Shared phones and IPs
# Pick 10 random phones and assign to 50 users (fraud ring)
suspicious_phones = meta_df["phone"].sample(10).values
fraud_indices = meta_df.sample(50).index

for i, idx in enumerate(fraud_indices):
    meta_df.at[idx, "phone"] = suspicious_phones[i % 10]

# Same for IPs
suspicious_ips = meta_df["ip"].sample(5).values
ip_fraud_indices = meta_df.sample(30).index

for i, idx in enumerate(ip_fraud_indices):
    meta_df.at[idx, "ip"] = suspicious_ips[i % 5]

# Done! Preview
meta_df.head(10)

# Save to processed/ folder for the next notebook
meta_df.to_csv("../data/processed/users_metadata.csv", index=False)

print("Enriched user metadata saved to 'data/processed/users_metadata.csv'")



