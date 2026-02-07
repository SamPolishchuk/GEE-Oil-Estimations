import ee
from datetime import datetime, timedelta

# Configuration
CLOUD_PROJECT = 'oil-tankers'
USERNAME = "2samenpoli"
START_DATE = datetime(2024, 1, 3)  # Wednesday
END_DATE = datetime(2024, 3, 1)
COMPOSITE_INTERVAL_DAYS = 7
SCALE = 10  # meters per pixel

"""
IMPORTNT: Will need to ask group if we want to keep Wednesdays (EIA release day) in data-set or exclude them.
"""

EIA_RELEASE_WEEKDAY = 2  # Monday=0, Tuesday=1, Wednesday=2

if START_DATE.weekday() != EIA_RELEASE_WEEKDAY:
    raise ValueError(
        f"START_DATE must be a Wednesday (EIA release day). "
        f"You provided {START_DATE.strftime('%A %Y-%m-%d')}"
    )

# Region assets (update these based on your uploaded regions)
REGION_ASSETS = [
    f"users/{USERNAME}/oil-tankers/fujairah_uae",
    f"users/{USERNAME}/oil-tankers/rotterdam_netherlands",
    f"users/{USERNAME}/oil-tankers/jurong_island_singapore",
    f"users/{USERNAME}/oil-tankers/houston_ship_channel_usa",
    f"users/{USERNAME}/oil-tankers/saldanha_bay_south_africa",
    f"users/{USERNAME}/oil-tankers/zhoushan_china",
    f"users/{USERNAME}/oil-tankers/cushing_ok"
]

def initialize_ee():
    """Initialize Earth Engine with error handling."""
    try:
        ee.Initialize(project=CLOUD_PROJECT)
        print(f"✓ Initialized GEE with project: {CLOUD_PROJECT}")
        return True
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        print("Run: earthengine authenticate --force")
        return False

def load_storage_polygons():
    """Load and merge all regional storage tank assets, skipping missing ones."""
    print("\nLoading storage tank assets...")
    
    valid_collections = []
    missing_assets = []
    
    for asset in REGION_ASSETS:
        try:
            # Try to load the asset
            collection = ee.FeatureCollection(asset)
            
            # Force evaluation to check if asset actually exists
            count = collection.size().getInfo()
            
            if count > 0:
                valid_collections.append(collection)
                region_name = asset.split('/')[-1].replace('_', ' ').title()
                print(f"  ✓ Loaded {region_name}: {count} tanks")
            else:
                print(f"  ⚠️  {asset}: exists but contains 0 features")
                
        except ee.EEException as e:
            missing_assets.append(asset)
            region_name = asset.split('/')[-1].replace('_', ' ').title()
            print(f"  ✗ {region_name}: Asset does not exist")
        except Exception as e:
            missing_assets.append(asset)
            region_name = asset.split('/')[-1].replace('_', ' ').title()
            print(f"  ✗ {region_name}: Error loading ({str(e)[:50]}...)")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Asset Loading Summary:")
    print(f"  Loaded: {len(valid_collections)}/{len(REGION_ASSETS)} regions")
    print(f"  Missing: {len(missing_assets)}/{len(REGION_ASSETS)} regions")
    
    if missing_assets:
        print(f"\nMissing assets:")
        for asset in missing_assets:
            print(f"  - {asset}")
    print(f"{'='*60}\n")
    
    # Check if we have any valid data
    if not valid_collections:
        print("✗ No valid storage tank assets found!")
        return None
    
    # Merge all valid collections
    try:
        merged = valid_collections[0]
        for coll in valid_collections[1:]:
            merged = merged.merge(coll)
        
        total_count = merged.size().getInfo()
        print(f"✓ Successfully merged {total_count} total storage tanks\n")
        return merged
        
    except Exception as e:
        print(f"✗ Failed to merge collections: {e}")
        return None

def mask_s2_clouds(image):
    """Mask clouds and cirrus in Sentinel-2 imagery."""
    qa = image.select('QA60')
    cloud_bit = 1 << 10
    cirrus_bit = 1 << 11
    mask = qa.bitwiseAnd(cloud_bit).eq(0).And(
           qa.bitwiseAnd(cirrus_bit).eq(0))
    
    # Also mask based on SCL (Scene Classification Layer) if available
    try:
        scl = image.select('SCL')
        # Mask clouds (3), cloud shadows (8), cirrus (9), snow (11)
        scl_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(11))
        combined_mask = mask.And(scl_mask)
    except:
        # If SCL not available, just use QA60
        combined_mask = mask
    
    return (image.updateMask(combined_mask)
                 .divide(10000)
                 .copyProperties(image, ['system:time_start', 'MEAN_SOLAR_ZENITH_ANGLE']))

def add_features(image):
    """Add derived spectral indices and texture features."""
    nir = image.select('B8')
    red = image.select('B4')
    green = image.select('B3')
    swir1 = image.select('B11')
    
    # Shadow index (lower values = darker = more oil potentially)
    shadow_index = nir.subtract(red).rename('shadow_index')
    
    # NDVI (to distinguish vegetation)
    ndvi = nir.subtract(red).divide(nir.add(red).add(0.0001)).rename('ndvi')
    
    # NDWI (water index - can help identify floating roof tanks)
    ndwi = green.subtract(nir).divide(green.add(nir).add(0.0001)).rename('ndwi')
    
    # Overall brightness
    brightness = image.select(['B2', 'B3', 'B4', 'B8']).reduce(ee.Reducer.mean()).rename('brightness')
    
    # Texture features (contrast indicates surface roughness)
    # CRITICAL FIX: Scale NIR back to integers for glcmTexture
    nir_int = nir.multiply(10000).toInt()
    texture = nir_int.glcmTexture(size=3)
    texture_contrast = texture.select('B8_contrast').rename('texture_contrast')
    texture_entropy = texture.select('B8_ent').rename('texture_entropy')
    
    return image.addBands([
        shadow_index,
        ndvi,
        ndwi,
        brightness,
        texture_contrast,
        texture_entropy
    ])

def generate_date_list(start, end, interval_days):
    """Generate list of dates for compositing."""
    dates = []
    current = start
    while current < end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=interval_days)
    print(f"✓ Generated {len(dates)} time periods from {start.date()} to {end.date()}")
    return dates

def create_composite(date_str, storage_bounds):
    """Create cloud-free composite for a given time period."""
    start = ee.Date(date_str)
    end = start.advance(COMPOSITE_INTERVAL_DAYS, 'day')
    
    # Get the image collection
    collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                   .filterDate(start, end)
                   .filterBounds(storage_bounds)
                   .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                   .map(mask_s2_clouds)
                   .map(add_features))
    
    # Calculate mean solar zenith angle for this period
    mean_solar_zenith = collection.aggregate_mean('MEAN_SOLAR_ZENITH_ANGLE')
    
    # Calculate sun elevation (90 - zenith angle)
    sun_elevation = ee.Number(90).subtract(mean_solar_zenith)
    
    composite = (collection.median()  # Use median to reduce outliers
                   .set({
                       'week': start.format('YYYY-MM-dd'),
                       'system:time_start': start.millis(),
                       'solar_zenith_angle': mean_solar_zenith,
                       'sun_elevation': sun_elevation
                   }))
    
    return composite

def extract_statistics(image, storage_polygons):
    """Extract statistics for each storage tank polygon."""
    stats = image.reduceRegions(
        collection=storage_polygons,
        reducer=ee.Reducer.mean()
                  .combine(ee.Reducer.stdDev(), '', True)
                  .combine(ee.Reducer.min(), '', True)
                  .combine(ee.Reducer.max(), '', True)
                  .combine(ee.Reducer.count(), '', True),  # Count valid pixels
        scale=SCALE,
        tileScale=4  # Reduce memory usage for large polygons
    )
    
    return stats.map(lambda f: f.set({
        'week': image.get('week'),
        'solar_zenith_angle': image.get('solar_zenith_angle'),
        'sun_elevation': image.get('sun_elevation')
    }))

def run_extraction():
    """Main function to run the tank monitoring extraction."""
    
    # Initialize
    if not initialize_ee():
        return False
    
    # Load storage tank polygons
    storage_polygons = load_storage_polygons()
    if storage_polygons is None:
        return False
    
    # Generate date list
    dates = generate_date_list(START_DATE, END_DATE, COMPOSITE_INTERVAL_DAYS)
    
    # Create composites
    print(f"Creating {COMPOSITE_INTERVAL_DAYS}-day composites...")
    storage_bounds = storage_polygons.geometry().bounds()
    
    composites = []
    for date_str in dates:
        comp = create_composite(date_str, storage_bounds)
        composites.append(comp)
    
    weekly_images = ee.ImageCollection(composites)
    print(f"✓ Created {len(composites)} composite images\n")
    
    # Extract statistics
    print("Extracting tank statistics...")
    tank_metrics = weekly_images.map(
        lambda img: extract_statistics(img, storage_polygons)
    ).flatten()
    
    # Export to Drive
    print("Starting export to Google Drive...")
    task = ee.batch.Export.table.toDrive(
        collection=tank_metrics,
        description='weekly_tank_metrics',
        folder='oil_tank_monitoring',  # Creates folder in Drive
        fileFormat='CSV',
        selectors=[  # Specify which properties to export
            'tank_id', 'location', 'week',
            'solar_zenith_angle', 'sun_elevation',
            'B2_mean', 'B3_mean', 'B4_mean', 'B8_mean',
            'shadow_index_mean', 'ndvi_mean', 'ndwi_mean',
            'brightness_mean', 'texture_contrast_mean',
            'B8_mean_stdDev', 'shadow_index_mean_stdDev',
            'B8_mean_count'  # Number of valid pixels
        ]
    )
    
    task.start()
    
    print("\n" + "="*60)
    print("✓ EXPORT STARTED")
    print("="*60)
    print("Monitor progress:")
    print("  https://code.earthengine.google.com/ → Tasks tab")
    print("\nOutput will be saved to:")
    print("  Google Drive → oil_tank_monitoring/weekly_tank_metrics.csv")
    print("  PRIMARY:")
    print("    • shadow_index_mean: Lower = darker = MORE OIL")
    print("    • shadow_index_stdDev: Lower = uniform = FULL TANK")
    print("  SUPPORTING:")
    print("    • B8_mean: NIR reflectance baseline")
    print("    • B8_count: Data quality check")
    print("  CONTROL:")
    print("    • ndvi_mean: Vegetation contamination check")
    print("\n SOLAR CORRECTION:")
    print("  - solar_zenith_angle: angle from vertical (0° = overhead sun)")
    print("  - sun_elevation: angle from horizon (90° = overhead sun)")
    print("="*60)
    
    return True

if __name__ == "__main__":
    success = run_extraction()
    exit(0 if success else 1)