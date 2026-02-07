import requests
import json
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import geojson
import time
import os

# -------------------------------------------------
# 1. Major oil storage locations worldwide
# -------------------------------------------------
LOCATIONS = {
    "Fujairah, UAE": "25.15,56.30,25.25,56.40",
    "Rotterdam, Netherlands": "51.85,3.90,51.99,4.50",
    "Jurong Island, Singapore": "1.22,103.65,1.30,103.75",
    "Houston Ship Channel, USA": "29.70,-95.30,29.80,-94.90",
    "Saldanha Bay, South Africa": "-33.05,17.85,-32.95,18.05",
    "Zhoushan, China": "29.85,121.90,30.10,122.30",
    "Cushing, OK": "35.95,-97.45,36.15,-96.95"
}

# -------------------------------------------------
# 2. Simplified query (avoiding duplicate searches)
# -------------------------------------------------
def build_query(bbox):
    """Simplified query to reduce server load."""
    return f"""
[out:json][timeout:180];
(
  way["man_made"="storage_tank"]({bbox});
  relation["man_made"="storage_tank"]({bbox});
);
out body;
>;
out skel qt;
"""

# -------------------------------------------------
# 3. Alternative Overpass servers
# -------------------------------------------------
OVERPASS_SERVERS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter"
]

# -------------------------------------------------
# 4. Fetch with retry logic and multiple servers
# -------------------------------------------------
def fetch_tanks(location_name, bbox, max_retries=3):
    """Fetch tank data with retry logic across multiple servers."""
    
    for attempt in range(max_retries):
        server = OVERPASS_SERVERS[attempt % len(OVERPASS_SERVERS)]
        
        try:
            print(f"\n{'Retrying' if attempt > 0 else 'Fetching'} data for {location_name}...")
            if attempt > 0:
                print(f"  Attempt {attempt + 1}/{max_retries} using {server}")
            
            query = build_query(bbox)
            
            # Add delay between requests to be nice to the server
            if attempt > 0:
                wait_time = 5 * attempt
                print(f"  Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            
            response = requests.post(
                server, 
                data={"data": query}, 
                timeout=200  # Increased timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Check for Overpass API errors
            if 'remark' in data:
                print(f"  ⚠️  API remark: {data['remark']}")
            
            elements = data.get('elements', [])
            nodes = {el['id']: (el['lon'], el['lat']) 
                    for el in elements if el['type'] == 'node'}

            features = []
            
            for el in elements:
                if el['type'] == 'way' and 'nodes' in el:
                    coords = [nodes[n] for n in el['nodes'] if n in nodes]
                    
                    if len(coords) < 3:
                        continue

                    # Close polygon if not closed
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])

                    try:
                        poly = Polygon(coords)
                        
                        if poly.is_valid and poly.area > 0:
                            properties = {
                                "tank_id": el['id'],
                                "location": location_name
                            }

                            # Preserve oil/fuel tags if present
                            if 'tags' in el:
                                if 'content' in el['tags']:
                                    properties['content'] = el['tags']['content']
                                if 'substance' in el['tags']:
                                    properties['substance'] = el['tags']['substance']
                            
                            features.append(geojson.Feature(
                                geometry=mapping(poly),
                                properties=properties
                            ))
                    except Exception as e:
                        continue

            print(f"  ✓ Found {len(features)} valid tanks")
            
            # Small delay between successful requests
            time.sleep(2)
            return features
            
        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout on attempt {attempt + 1}")
            if attempt == max_retries - 1:
                print(f"  ✗ All attempts failed for {location_name}")
                return []
            continue
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Network error: {e}")
            if attempt == max_retries - 1:
                print(f"  ✗ All attempts failed for {location_name}")
                return []
            continue
            
        except json.JSONDecodeError:
            print(f"  ✗ Invalid JSON response")
            if attempt == max_retries - 1:
                return []
            continue
            
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            if attempt == max_retries - 1:
                return []
            continue
    
    return []

# -------------------------------------------------
# 5. Create output directory if it doesn't exist
# -------------------------------------------------
output_dir = "data/regions"
os.makedirs(output_dir, exist_ok=True)

# -------------------------------------------------
# 6. Fetch and save each location separately
# -------------------------------------------------
total_tanks = 0
successful_regions = 0

for location_name, bbox in LOCATIONS.items():
    features = fetch_tanks(location_name, bbox)
    
    if features:
        # Create safe filename
        safe_name = location_name.lower()\
            .replace(' ', '_')\
            .replace(',', '')\
            .replace('.', '')
        
        filename = f"{output_dir}/{safe_name}.geojson"
        
        try:
            fc = geojson.FeatureCollection(features)
            
            with open(filename, "w") as f:
                geojson.dump(fc, f, indent=2)
            
            file_size = os.path.getsize(filename) / 1024  # KB
            print(f"  ✓ Saved to {filename} ({file_size:.1f} KB)")
            
            total_tanks += len(features)
            successful_regions += 1
            
        except Exception as e:
            print(f"  ✗ Error saving {filename}: {e}")

# -------------------------------------------------
# 7. Summary
# -------------------------------------------------
print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")
print(f"Regions processed: {successful_regions}/{len(LOCATIONS)}")
print(f"Total tanks saved: {total_tanks}")
print(f"Output directory: {output_dir}/")
print(f"\nFiles created:")
for filename in sorted(os.listdir(output_dir)):
    if filename.endswith('.geojson'):
        filepath = os.path.join(output_dir, filename)
        size_kb = os.path.getsize(filepath) / 1024
        print(f"  - {filename} ({size_kb:.1f} KB)")
print(f"{'='*60}")