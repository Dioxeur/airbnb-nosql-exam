import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://admin:admin@mongo:27017/")
db = client.airbnb
collection = db.listings

print("📊 Starting migration...")

# Load cleaned data
df = pd.read_csv("/data/listings_clean.csv")
print(f"Loaded: {len(df)} rows")

# Transform to MongoDB documents
documents = []
for _, row in df.iterrows():
    doc = {
        "listing_id": int(row['id']),
        "name": row['name'],
        "property_type": row['property_type'],
        "host": {
            "host_id": int(row['host_id']),
            "host_name": str(row['host_name']),
            "is_superhost": bool(row['host_is_superhost'])
        },
        "location": {
            "city": "Paris",
            "neighbourhood": str(row['neighbourhood_cleansed']),
            "latitude": float(row['latitude']) if pd.notna(row['latitude']) else None,
            "longitude": float(row['longitude']) if pd.notna(row['longitude']) else None
        },
        "reviews": {
            "count": int(row['number_of_reviews'])
        },
        "instant_bookable": bool(row['instant_bookable']),
        "last_scraped": row['last_scraped']
    }
    documents.append(doc)

# Insert into MongoDB
collection.delete_many({})  # Clear existing data
result = collection.insert_many(documents)
print(f"✅ Inserted {len(result.inserted_ids)} documents")

# Create indexes
collection.create_index("listing_id", unique=True)
collection.create_index("property_type")
collection.create_index("last_scraped")
collection.create_index([("reviews.count", -1)])
collection.create_index("host.host_id")

print("✅ Migration completed!")