# 🌍 GeoJSON Seeding System

Complete guide for managing geographic data in the DDP backend.

## 🏗️ System Architecture

```
seed/geojsons/
├── countries/                     # Country-organized GeoJSON files
│   └── [country]/                # Each country in separate folder
│       ├── country_metadata.json # Required metadata file
│       ├── [country].json        # Country-level GeoJSON (optional)
│       ├── layer1/               # First admin level (states/provinces)
│       │   └── *.json
│       ├── layer2/               # Second admin level (districts/counties)
│       │   └── *.json
│       └── layerN/               # Additional levels as needed
│           └── *.json
└── README.md                     # This file
```

## 📋 Quick Start

### 1. Validate Your Setup
```bash
python manage.py seed_map_data --validate-only
```

### 2. Seed All Countries
```bash
python manage.py seed_map_data
```

### 3. Seed Specific Country
```bash
python manage.py seed_map_data --country india
```

## 🔧 Adding a New Country

### Step 1: Create Folder Structure
```bash
mkdir -p seed/geojsons/countries/[country]/layer1
mkdir -p seed/geojsons/countries/[country]/layer2
```

### Step 2: Create Metadata File
Create `countries/[country]/country_metadata.json`:

```json
{
  "country": {
    "id": 200,
    "name": "Brazil",
    "type": "country", 
    "parent_id": null,
    "country_code": "BRA",
    "region_code": "BRA",
    "display_name": "Brazil"
  },
  "hierarchy": {
    "layer1": {
      "type": "state",
      "display_name": "States",
      "description": "Brazilian states"
    },
    "layer2": {
      "type": "municipality",
      "display_name": "Municipalities", 
      "description": "Cities and municipalities"
    }
  },
  "regions": [
    {
      "id": 201,
      "name": "São Paulo",
      "type": "state",
      "parent_id": 200,
      "country_code": "BRA",
      "region_code": "SP", 
      "display_name": "São Paulo",
      "layer": "layer1"
    }
  ]
}
```

### Step 3: Add GeoJSON Files
- Use **perfect naming format**: `region_name.json` (lowercase, underscores for spaces)
- Place in correct layer folder
- Ensure UTF-8 encoding

### Step 4: Validate and Seed
```bash
python manage.py seed_map_data --validate-only
python manage.py seed_map_data --country brazil
```

## 📝 File Naming Rules

### ✅ Perfect Format
Convert database names to filename format:
- **Lowercase everything**: `São Paulo` → `sao_paulo.json`
- **Replace spaces with underscores**: `West Bengal` → `west_bengal.json`
- **Use .json extension**: Always `.json`, never `.geojson`

### ✅ Examples
| Database Name | Perfect Filename |
|---------------|------------------|
| "Tamil Nadu" | `tamil_nadu.json` |
| "New York" | `new_york.json` |
| "São Paulo" | `sao_paulo.json` |
| "Île-de-France" | `ile_de_france.json` |

## 🏛️ Metadata File Structure

### Country Section
```json
"country": {
  "id": 1,                    // Unique global ID
  "name": "India",            // Exact database name
  "type": "country",          // Always "country"
  "parent_id": null,          // Always null for countries
  "country_code": "IND",      // ISO 3-letter code
  "region_code": "IND",       // Usually same as country_code
  "display_name": "India"     // User-friendly name
}
```

### Hierarchy Section
```json
"hierarchy": {
  "layer1": {
    "type": "state",                    // Admin level type name
    "display_name": "States",           // User-friendly plural
    "description": "First admin level"  // Detailed description
  }
}
```

### Regions Section
```json
"regions": [
  {
    "id": 2,                    // Unique global ID
    "name": "Maharashtra",      // Exact name for filename matching
    "type": "state",           // Matches hierarchy type
    "parent_id": 1,            // Reference to parent region ID
    "country_code": "IND",     // Country code
    "region_code": "MH",       // Unique region code
    "display_name": "Maharashtra", // User-friendly name
    "layer": "layer1"          // Which folder the file should be in
  }
]
```

## ⚠️ Important Guidelines

### ID Management
- **Use unique IDs globally** (not per country)
- **Start each country at different ranges**:
  - India: 1-99
  - Brazil: 100-199  
  - Kenya: 200-299
- **Never reuse IDs** across countries

### Layer Definition
- **layer1**: First administrative division (states/provinces/regions)
- **layer2**: Second administrative division (districts/counties/cities)
- **layer3**: Third administrative division (blocks/wards/municipalities)
- **layerN**: Additional levels as needed

### File Requirements
- **Valid GeoJSON**: Must have `type: "FeatureCollection"` and `features` array
- **UTF-8 encoding**: Handles special characters properly  
- **Reasonable size**: Large files may cause memory issues

## 🚨 Validation Errors

### Common Issues and Solutions

**❌ Missing metadata file**
```
Error: india: Missing country_metadata.json
Solution: Create the required metadata file
```

**❌ File-metadata mismatch** 
```
Error: india: GeoJSON file exists but no metadata for 'Punjab'
Solution: Add Punjab to regions array in metadata
```

**❌ Duplicate IDs**
```
Error: brazil: Duplicate region ID 1
Solution: Use unique ID ranges per country
```

**❌ Broken parent relationships**
```
Error: brazil: Region 'São Paulo' has invalid parent_id 999
Solution: Ensure parent_id references exist
```

## 🧪 Testing Your Setup

### Full Validation
```bash
# Check all countries
python manage.py seed_map_data --validate-only

# Should show: ✅ All validations passed!
```

### Test Seeding
```bash
# Seed specific country first
python manage.py seed_map_data --country india

# Then seed all
python manage.py seed_map_data
```

### Verify Database
```bash
python manage.py shell -c "
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON
print(f'Regions: {GeoRegion.objects.count()}')
print(f'GeoJSONs: {GeoJSON.objects.count()}')
"
```

## 🔧 Troubleshooting

### Re-running Seeds
The system is **idempotent** - you can run multiple times safely:
```bash
# This is safe to run repeatedly
python manage.py seed_map_data
```

### Clearing Data
```bash  
# Clear all geographic data (use with caution!)
python manage.py shell -c "
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON
GeoJSON.objects.all().delete()
GeoRegion.objects.all().delete()
print('All geographic data cleared')
"
```

### Debug Mode
```bash
# Add --verbosity 2 for detailed output
python manage.py seed_map_data --verbosity 2
```

## 🌟 Best Practices

1. **Always validate first** before seeding
2. **Use consistent ID ranges** per country  
3. **Follow perfect naming format** for 100% matching
4. **Test with one country** before adding many
5. **Keep metadata in sync** with actual files
6. **Use descriptive region codes** (e.g., "MH" for Maharashtra)
7. **Document your hierarchy** in metadata descriptions

## 📞 Support

For issues or questions:
1. **Check validation output** first - it shows exact problems
2. **Verify file naming format** matches database names
3. **Ensure metadata completeness** - all required fields
4. **Test with minimal setup** before scaling up

## 🎯 Example: Complete India Setup

The `countries/india/` folder demonstrates a complete working setup:
- ✅ Complete metadata with all 36 regions
- ✅ Perfect filename format for all files
- ✅ Proper layer1/layer2 organization  
- ✅ 100% validation success
- ✅ All 36 GeoJSONs seeded successfully

Use India as a reference for your country implementations!