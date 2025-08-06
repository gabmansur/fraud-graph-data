# %%
#  02_inject_fraud_patterns.ipynb
# Merge metadata with transactions and prep for graph import

import pandas as pd

# Load PaySim sample
df = pd.read_csv("../data/raw/PS_20174392719_1491204439457_log.csv")
df = df.sample(10000, random_state=42)

# Load enriched metadata
meta_df = pd.read_csv("../data/processed/users_metadata.csv")

# Merge sender metadata
df = df.merge(meta_df, how='left', left_on='nameOrig', right_on='user_id')
df = df.rename(columns={
    'name': 'sender_name',
    'email': 'sender_email',
    'phone': 'sender_phone',
    'ip': 'sender_ip',
    'company': 'sender_company'
}).drop(columns=['user_id'])

# Merge receiver metadata
df = df.merge(meta_df, how='left', left_on='nameDest', right_on='user_id')
df = df.rename(columns={
    'name': 'receiver_name',
    'email': 'receiver_email',
    'phone': 'receiver_phone',
    'ip': 'receiver_ip',
    'company': 'receiver_company'
}).drop(columns=['user_id'])

# Preview result
df.head()

df.to_csv("../data/processed/enriched_transactions.csv", index=False)
print("Enriched transactions saved to 'data/processed/enriched_transactions.csv'")


