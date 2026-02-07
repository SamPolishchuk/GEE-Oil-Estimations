import ee
import sys
import json
import os
import glob

# Configuration
CLOUD_PROJECT = 'oil-tankers' 
USERNAME = "2samenpoli"
REGIONS_DIR = "data/regions"

def validate_geojson(filepath):
    """Validate GeoJSON structure and check for common issues."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check type
        if data.get('type') != 'FeatureCollection':
            print(f"  ✗ Invalid: type is '{data.get('type')}', expected 'FeatureCollection'")
            return False
        
        # Check features
        features = data.get('features', [])
        if not features:
            print(f"  ✗ No features found")
            return False
        
        print(f"  ✓ Valid FeatureCollection with {len(features)} features")
        return True
        
    except json.JSONDecodeError as e:
        print(f"  ✗ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Validation error: {e}")
        return False

def upload_region(filepath):
    """Upload a single region GeoJSON file to GEE."""
    
    # Get region name from filename
    filename = os.path.basename(filepath)
    region_name = os.path.splitext(filename)[0]
    asset_id = f"users/{USERNAME}/oil-tankers/{region_name}"
    
    print(f"\n{'='*60}")
    print(f"Processing: {filename}")
    print(f"{'='*60}")
    
    # Validate
    print("Validating GeoJSON...")
    if not validate_geojson(filepath):
        print(f"✗ Validation failed for {filename}")
        return False
    
    # Load GeoJSON
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
    except Exception as e:
        print(f"  ✗ Failed to load file: {e}")
        return False
    
    # Convert to Earth Engine FeatureCollection
    print("Converting to Earth Engine format...")
    try:
        features = []
        for feat in geojson_data['features']:
            ee_feature = ee.Feature(feat)
            features.append(ee_feature)
        
        fc = ee.FeatureCollection(features)
        print(f"  ✓ Created FeatureCollection with {len(features)} features")
        
    except Exception as e:
        print(f"  ✗ Conversion failed: {e}")
        return False
    
    # Upload to Earth Engine
    print(f"Starting export to {asset_id}...")
    try:
        task = ee.batch.Export.table.toAsset(
            collection=fc,
            description=f'upload_{region_name}',
            assetId=asset_id
        )
        
        task.start()
        
        print(f"  ✓ Task started: {task.status()['description']}")
        print(f"  State: {task.status()['state']}")
        print(f"  Asset ID: {asset_id}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Export failed: {e}")
        return False

def upload_all_regions():
    """Upload all region files to GEE."""
    
    # Initialize EE
    try:
        ee.Initialize(project=CLOUD_PROJECT)
        print(f"✓ Initialized GEE with project: {CLOUD_PROJECT}\n")
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        print("\nRun: earthengine authenticate --force")
        return False
    
    # Find all GeoJSON files in regions directory
    pattern = os.path.join(REGIONS_DIR, "*.geojson")
    geojson_files = glob.glob(pattern)
    
    if not geojson_files:
        print(f"✗ No GeoJSON files found in {REGIONS_DIR}/")
        return False
    
    print(f"Found {len(geojson_files)} region files to upload:\n")
    for f in geojson_files:
        print(f"  - {os.path.basename(f)}")
    
    # Upload each file
    successful = 0
    failed = 0
    
    for filepath in geojson_files:
        if upload_region(filepath):
            successful += 1
        else:
            failed += 1
    
    # Summary
    print("\n" + "="*60)
    print("UPLOAD SUMMARY")
    print("="*60)
    print(f"Successful: {successful}/{len(geojson_files)}")
    print(f"Failed: {failed}/{len(geojson_files)}")
    print("\nMonitor all tasks:")
    print("  https://code.earthengine.google.com/ → Tasks tab")
    print("\nAssets location:")
    print(f"  users/{USERNAME}/oil-tankers/[region_name]")
    print("="*60)
    
    return failed == 0

if __name__ == "__main__":
    success = upload_all_regions()
    sys.exit(0 if success else 1)
