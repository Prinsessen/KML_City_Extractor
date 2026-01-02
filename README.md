# Extract cities from KML — progress bar + consecutive duplicate control (v2.1)

This tool walks the KML in document order and reverse‑geocodes each vertex to a **city/admin/country** label. It shows a **tqdm progress bar** while processing and lets you skip **consecutive duplicates only** — so round‑trips will still list a city again when you return later.

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

### Notes
- **Consecutive only**: The `--unique-only` filter compares each result with the **previous** one. If the city repeats later (e.g., round trip), it will appear again.
- **Choose the key**: Use `--unique-on city_admin_country` if you want duplicates removed only when **all three** match.
- Blank city names: consecutive blank results are also collapsed; once a real city appears it will be listed.
- If `geopy` can’t init, the script falls back to offline and warns in console.
