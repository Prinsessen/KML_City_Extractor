#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract cities from a KML file (start-to-end order) — with progress bar

v2.1
- NEW: `--unique-on` lets you choose duplicate logic:
    * `city` (default): skip only consecutive repeats of the **same city name**
    * `city_admin_country`: skip repeats only if **city+admin+country** are all identical
- Keeps the progress bar via tqdm, sampling, and per‑Placemark caps.

Features
- Parses Placemark LineString, Point, and gx:Track (gx:coord)
- Iterates coordinates in-file order and reverse-geocodes each vertex
- Offline geocoding (reverse_geocoder) OR online geocoding (geopy/Nominatim)
- Progress bar via tqdm (overall points processed)
- Options to sample every Nth point, deduplicate consecutive identical cities,
  and cap maximum lookups per Placemark
- Outputs a single CSV listing cities in traversal order
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional, Dict

from tqdm import tqdm

# -------------------- KML parsing --------------------

def _parse_coords_string(text: str) -> List[Tuple[float, float]]:
    if not text:
        return []
    out: List[Tuple[float, float]] = []
    for tok in text.strip().split():
        parts = tok.split(',')
        if len(parts) >= 2:
            try:
                lon = float(parts[0]); lat = float(parts[1])
                if -180 <= lon <= 180 and -90 <= lat <= 90:
                    out.append((lat, lon))
            except Exception:
                pass
    return out


def _parse_gx_coord(text: str) -> Optional[Tuple[float, float]]:
    parts = (text or '').strip().split()
    if len(parts) >= 2:
        try:
            lon = float(parts[0]); lat = float(parts[1])
            if -180 <= lon <= 180 and -90 <= lat <= 90:
                return (lat, lon)
        except Exception:
            return None
    return None


def parse_kml_points(kml_path: str) -> List[Tuple[float, float, str]]:
    """Return a flat list of (lat, lon, source_id) following KML order.
    source_id = Placemark name or auto index.
    """
    ns = {
        'kml': 'http://www.opengis.net/kml/2.2',
        'gx': 'http://www.google.com/kml/ext/2.2',
    }
    tree = ET.parse(kml_path)
    root = tree.getroot()

    def get_text(elem, path):
        child = elem.find(path, ns)
        return child.text if child is not None and child.text else None

    points: List[Tuple[float, float, str]] = []
    auto = 0
    for pm in root.findall('.//kml:Placemark', ns):
        name = get_text(pm, 'kml:name') or f'placemark_{auto}'; auto += 1
        # LineString vertices
        for ls in pm.findall('.//kml:LineString', ns):
            coords = get_text(ls, 'kml:coordinates') or ''
            for lat, lon in _parse_coords_string(coords):
                points.append((lat, lon, name))
        # Point(s)
        for pt in pm.findall('.//kml:Point', ns):
            coords = get_text(pt, 'kml:coordinates') or ''
            for lat, lon in _parse_coords_string(coords):
                points.append((lat, lon, name))
        # gx:Track
        for tr in pm.findall('.//gx:Track', ns):
            for c in tr.findall('.//gx:coord', ns):
                tpl = _parse_gx_coord(c.text or '')
                if tpl:
                    lat, lon = tpl
                    points.append((lat, lon, name))
    return points

# -------------------- Geocoding ----------------------

def init_online(rate: float, user_agent: str):
    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        g = Nominatim(user_agent=user_agent)
        return RateLimiter(g.reverse, min_delay_seconds=rate, max_retries=2, error_wait_seconds=2.0)
    except Exception:
        return None


def geo_offline(lat: float, lon: float) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        import reverse_geocoder as rg
        res = rg.search([(lat, lon)], mode=1)[0]
        return res.get('name'), res.get('admin1'), res.get('cc')
    except Exception:
        return None, None, None


def geo_online(lat: float, lon: float, reverse_func, language: str='en') -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        loc = reverse_func((lat, lon), language=language, exactly_one=True, timeout=10)
        if not loc or 'address' not in loc.raw:
            return None, None, None
        adr = loc.raw.get('address', {})
        city_like = adr.get('city') or adr.get('town') or adr.get('village') or adr.get('municipality') or adr.get('hamlet')
        admin = adr.get('state') or adr.get('region')
        country = adr.get('country')
        return city_like, admin, country
    except Exception:
        return None, None, None

# -------------------- Dedup helpers ------------------

def _norm(s: Optional[str]) -> str:
    return (s or '').strip().lower()


def _make_key(city: Optional[str], admin: Optional[str], country: Optional[str], mode: str) -> Tuple[str, ...]:
    if mode == 'city':
        return (_norm(city),)
    else:
        # city_admin_country
        return (_norm(city), _norm(admin), _norm(country))

# -------------------- Main extraction ----------------

def extract_cities(kml_path: str, mode: str, output_csv: str, sample_every: int, unique_only: bool, unique_on: str, max_per_placemark: Optional[int], language: str, rate: float, user_agent: str) -> None:
    import csv

    pts = parse_kml_points(kml_path)
    if not pts:
        print('No points found in KML.'); sys.exit(2)

    reverse = None
    if mode == 'online':
        reverse = init_online(rate, user_agent)
        if reverse is None:
            print('[WARN] geopy/Nominatim not available; switching to offline nearest-city.')
            mode = 'offline'

    rows = []
    last_key: Optional[Tuple[str, ...]] = None
    count_by_src: Dict[str, int] = {}

    # Progress bar over total points; respects sampling inside loop
    for idx, (lat, lon, src) in enumerate(tqdm(pts, desc='Geocoding', unit='pt')):
        # Sampling
        if sample_every > 1 and (idx % sample_every) != 0:
            continue
        # Per-Placemark cap
        if max_per_placemark is not None:
            c = count_by_src.get(src, 0)
            if c >= max_per_placemark:
                continue
        # Geocode
        if mode == 'online' and reverse is not None:
            city, admin, country = geo_online(lat, lon, reverse, language=language)
        else:
            city, admin, country = geo_offline(lat, lon)

        # Unique-only filter (skip consecutive duplicates only)
        if unique_only:
            key = _make_key(city, admin, country, unique_on)
            if last_key == key:
                continue
            last_key = key
        else:
            key = None  # unused

        count_by_src[src] = count_by_src.get(src, 0) + 1
        rows.append({
            'seq': len(rows),
            'placemark': src,
            'lat': lat,
            'lon': lon,
            'city': city,
            'admin': admin,
            'country': country,
        })

    # Write CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['seq','placemark','lat','lon','city','admin','country'])
        w.writeheader(); w.writerows(rows)

    print(f'Wrote {len(rows)} rows to {output_csv}')

# -------------------- CLI ----------------------------

def main():
    ap = argparse.ArgumentParser(description='Extract all cities from a KML, in traversal order (with progress bar).')
    ap.add_argument('--input-kml', required=True, help='Path to KML file')
    ap.add_argument('--output', default='cities_in_order.csv', help='Output CSV path')
    ap.add_argument('--mode', choices=['offline','online'], default='offline', help='Geocoding mode')
    ap.add_argument('--rate', type=float, default=1.0, help='Rate limit for online Nominatim')
    ap.add_argument('--user-agent', default='agesen_el_teknik_city_extractor', help='User-Agent for online geocoding')
    ap.add_argument('--city-language', default='en', help='Language for Nominatim results (online)')
    ap.add_argument('--sample-every', type=int, default=1, help='Use every Nth point to speed up (default 1 = all points)')
    ap.add_argument('--unique-only', action='store_true', help='Skip consecutive duplicates (do not remove later repeats)')
    ap.add_argument('--unique-on', choices=['city','city_admin_country'], default='city', help='Duplicate key for unique-only filter')
    ap.add_argument('--max-per-placemark', type=int, default=None, help='Cap lookups per Placemark (optional)')
    args = ap.parse_args()

    extract_cities(kml_path=args.input_kml,
                   mode=args.mode,
                   output_csv=args.output,
                   sample_every=max(1, args.sample_every),
                   unique_only=args.unique_only,
                   unique_on=args.unique_on,
                   max_per_placemark=args.max_per_placemark,
                   language=args.city_language,
                   rate=args.rate,
                   user_agent=args.user_agent)

if __name__ == '__main__':
    main()
