
import pandas as pd

# Load and clean data
df = pd.read_csv("./data/listings_Paris.csv")
print(f"Loaded: {len(df)} rows")

# Convert booleans
df['instant_bookable'] = df['instant_bookable'].map({'t': True, 'f': False})
df['host_is_superhost'] = df['host_is_superhost'].map({'t': True, 'f': False})

# Convert dates
df['last_scraped'] = pd.to_datetime(df['last_scraped'])

# Keep only necessary columns for exam
exam_columns = ['id', 'name', 'property_type', 'host_id', 'host_name', 
               'number_of_reviews', 'instant_bookable', 'host_is_superhost', 
               'last_scraped', 'neighbourhood_cleansed', 'latitude', 'longitude']
df = df[exam_columns]
print(f"After cleaning: {len(df)} rows, {len(df.columns)} columns")


df.to_csv("listings_clean.csv", index=False)
print("✅ Cleaned data saved!")