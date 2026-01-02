#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract cities from a KML file (start-to-end order) — with progress bar

v2.3
- NEW options:
  * `--cities-only <path>` → write a second CSV with city names only (no lat/lon)
  * `--global-unique` → when used with `--cities-only`, drop later repeats of the same city anywhere in the sequence
  * `--group-by-placemark <path>` → write a per‑Placemark summary CSV
  * `--group-stats` → when used with `--group-by-placemark`, include per‑Placemark distinct city counts
- Keeps: progress bar, consecutive duplicate control (`--unique-only`, `--unique-on`),
  sampling (`--sample-every`), per‑Placemark cap (`--max-per-placemark`), offline/online geocoding.
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

def extract_cities(kml_path: str, mode: str, output_csv: str, sample_every: int, unique_only: bool, unique_on: str, max_per_placemark: Optional[int], language: str, rate: float, user_agent: str, cities_only: Optional[str], global_unique: bool, group_by_placemark: Optional[str], group_stats: bool) -> None:
    import csv
    from collections import Counter

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

    # For secondary outputs
    cities_seq: List[str] = []
    cities_seq_norm: List[str] = []
    placemark_cities: Dict[str, List[str]] = {}

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

        count_by_src[src] = count_by_src.get(src, 0) + 1

        # Append to main rows
        rows.append({
            'seq': len(rows),
            'placemark': src,
            'lat': lat,
            'lon': lon,
            'city': city,
            'admin': admin,
            'country': country,
        })
        # Track cities-only in traversal order
        cval = city or ''
        cities_seq.append(cval)
        cities_seq_norm.append(_norm(cval))
        # Accumulate per-placemark
        placemark_cities.setdefault(src, []).append(cval)

    # Write main CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['seq','placemark','lat','lon','city','admin','country'])
        w.writeheader(); w.writerows(rows)

    print(f'Wrote {len(rows)} rows to {output_csv}')

    # Secondary CSV: cities-only sequence
    if cities_only:
        c_only_out: List[Dict[str, str]] = []
        if global_unique:
            seen = set()
            for i, (c, cn) in enumerate(zip(cities_seq, cities_seq_norm)):
                if cn in seen:
                    continue
                seen.add(cn)
                c_only_out.append({'seq': i, 'city': c})
        else:
            # collapse consecutive duplicates only
            last_city_norm = None
            for i, cn in enumerate(cities_seq_norm):
                if last_city_norm == cn:
                    continue
                last_city_norm = cn
                c_only_out.append({'seq': i, 'city': cities_seq[i]})
        with open(cities_only, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['seq','city'])
            w.writeheader(); w.writerows(c_only_out)
        print(f'Wrote {len(c_only_out)} rows to {cities_only}')

    # Secondary CSV: group by placemark
    if group_by_placemark:
        gb_out: List[Dict[str, str]] = []
        for src, clist in placemark_cities.items():
            # First/last non-empty city
            first_city = next((c for c in clist if c), '')
            last_city = next((c for c in reversed(clist) if c), '')
            # joined full sequence (non-empty)
            seq_non_empty = [c for c in clist if c]
            all_cities_joined = ' | '.join(seq_non_empty)
            row = {
                'placemark': src,
                'first_city': first_city,
                'last_city': last_city,
                'cities': all_cities_joined,
                'count': str(len(seq_non_empty))
            }
            if group_stats:
                # counts per distinct city within this placemark
                ctr = Counter([_norm(c) for c in seq_non_empty])
                # Build label map with original case by first occurrence
                label_map: Dict[str, str] = {}
                for c in seq_non_empty:
                    n = _norm(c)
                    if n and n not in label_map:
                        label_map[n] = c
                parts = [f"{label_map[n]}: {ctr[n]}" for n in ctr]
                row['city_counts'] = ' | '.join(parts)
            gb_out.append(row)
        # fieldnames vary if stats present
        fields = ['placemark','first_city','last_city','cities','count'] + (['city_counts'] if group_stats else [])
        with open(group_by_placemark, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader(); w.writerows(gb_out)
        print(f'Wrote {len(gb_out)} rows to {group_by_placemark}')

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
    # Secondary outputs
    ap.add_argument('--cities-only', help='Write a second CSV with only city names in traversal order (e.g., cities_only.csv)')
    ap.add_argument('--global-unique', action='store_true', help='With --cities-only, drop later repeats anywhere (not just consecutive)')
    ap.add_argument('--group-by-placemark', help='Write a second CSV grouped by Placemark (e.g., cities_by_placemark.csv)')
    ap.add_argument('--group-stats', action='store_true', help='With --group-by-placemark, include per-placemark distinct city counts')
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
                   user_agent=args.user_agent,
                   cities_only=args.cities_only,
                   global_unique=args.global_unique,
                   group_by_placemark=args.group_by_placemark,
                   group_stats=args.group_stats)

if __name__ == '__main__':
    main()
