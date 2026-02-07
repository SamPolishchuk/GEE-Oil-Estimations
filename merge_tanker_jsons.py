import geojson
import json

FILE_PATH_1 = "data/oil_tanks_1.geojson"
FILE_PATH_2 = "data/oil_tanks_2.geojson"

# Load both files
with open(FILE_PATH_1, "r") as f:
    data1 = geojson.load(f)

with open(FILE_PATH_2, "r") as f:
    data2 = geojson.load(f)

# Combine features
all_features = data1['features'] + data2['features']

# Remove duplicates based on tank_id
seen_ids = set()
unique_features = []

for feature in all_features:
    tank_id = feature['properties'].get('tank_id')
    if tank_id not in seen_ids:
        seen_ids.add(tank_id)
        unique_features.append(feature)

# Create merged collection
merged = geojson.FeatureCollection(unique_features)

# Save merged file
with open("data/oil_tanks.geojson", "w") as f:
    geojson.dump(merged, f, indent=2)

print(f"File 1: {len(data1['features'])} tanks")
print(f"File 2: {len(data2['features'])} tanks")
print(f"Total: {len(all_features)} tanks")
print(f"Unique: {len(unique_features)} tanks")
print(f"Duplicates removed: {len(all_features) - len(unique_features)}")
print(f"\n✓ Merged file saved as oil_tanks_merged.geojson")