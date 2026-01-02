# Extract cities from KML — progress bar + consecutive duplicate control (v2.4)

This tool walks the KML in document order and reverse‑geocodes each vertex to a **city/admin/country** label. It shows a **tqdm progress bar** while processing and lets you skip **consecutive duplicates only** — so round‑trips will still list a city again when you return later.

## Features
- **Distance tracking**: Calculates distance between consecutive points and maintains cumulative distance
- **Multiple output formats**: Main CSV with full details, cities-only CSV, and per-placemark summaries
- **Flexible geocoding**: Online (Nominatim) or offline (reverse_geocoder) modes
- **Smart filtering**: Skip consecutive duplicates while preserving round-trip sequences
## Install (Linux)
```bash
python3 -m pip install --requrements.txt
```
## Install (macOS)
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install reverse_geocoder geopy tqdm
```

## Offline example (nearest city; skip consecutive duplicates by city name)
```bash
python3 extract_cities_from_kml.py   --input-kml routes.kml   --mode offline   --output cities_in_order.csv   --sample-every 1   --unique-only   --unique-on city
```

## Online example (Nominatim; richer labels; same duplicate behavior)
```bash
python3 extract_cities_from_kml.py   --input-kml routes.kml   --mode online   --rate 1.0   --user-agent "agesen_el_teknik_city_extractor"   --output cities_in_order.csv   --sample-every 1   --unique-only   --unique-on city
```

## Output CSV Format
The main output CSV includes these columns:
- `seq`: Sequential index
- `placemark`: Source placemark name from KML
- `lat`, `lon`: Coordinates
- `city`, `admin`, `country`: Geocoded location
- `distance_km`: Distance in kilometers from the previous point (0 for first point)
- `cumulative_distance_km`: Total distance traveled from the start

## Additional Options
```bash
# Generate a cities-only CSV (no coordinates)
--cities-only cities_only.csv

# Remove all duplicate cities (not just consecutive)
--global-unique

# Group cities by placemark
--group-by-placemark cities_by_placemark.csv --group-stats

# Limit points per placemark
--max-per-placemark 100
```

### Notes
- **Distance calculation**: Uses geodesic (great-circle) distance for accurate measurements accounting for Earth's curvature
- **Consecutive only**: The `--unique-only` filter compares each result with the **previous** one. If the city repeats later (e.g., round trip), it will appear again.
- **Choose the key**: Use `--unique-on city_admin_country` if you want duplicates removed only when **all three** match.
- Blank city names: consecutive blank results are also collapsed; once a real city appears it will be listed.
- If `geopy` can't init, the script falls back to offline and warns in console.

### Tips
- Big tracks? Use `--sample-every 5` (or 10) to keep it snappy while preserving order.
- Very long Placemarks? Add `--max-per-placemark 100` so one giant segment doesn't dominate the run
