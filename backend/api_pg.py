import asyncio
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
import math
import requests
import websockets
import networkx as nx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from psycopg2.pool import SimpleConnectionPool
from skyfield.api import load as sf_load, wgs84, EarthSatellite
from pydantic import BaseModel
from typing import List

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from railway_router import RailwayRouter
from road_router import RoadRouter
load_dotenv()
OPENSKY_USERNAME = os.environ.get("OPENSKY_USERNAME")
OPENSKY_PASSWORD = os.environ.get("OPENSKY_PASSWORD")
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY")
GFW_API_TOKEN = os.environ.get("GFW_API_TOKEN")

app = FastAPI(title="Bangladesh Map API (Cloud PostgreSQL)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# ---------------------------------------------------------
# PostgreSQL Connection Pool
# ---------------------------------------------------------
DB_URL = os.environ.get(
    "DATABASE_URL",
    ""
)

# Create a connection pool to handle multiple concurrent API requests flawlessly
pg_pool = None
shard_pools = {}

def init_pools():
    global pg_pool, shard_pools
    if pg_pool is not None:
        return
    try:
        pg_pool = SimpleConnectionPool(0, 20, DB_URL)
        _mconn = pg_pool.getconn()
        _mcur = _mconn.cursor()
        _mcur.execute("SELECT layer_name, connection_uri FROM shard_registry WHERE status = 'active';")
        for _layer, _suri in _mcur.fetchall():
            try:
                shard_pools[_layer] = SimpleConnectionPool(0, 10, _suri)
            except Exception as se:
                print(f"[Shard Warning] Could not connect to {_layer}: {se}")
        _mcur.close()
        pg_pool.putconn(_mconn)
        print(f"[Orchestrator] Active Neon Shard Pools loaded: {list(shard_pools.keys())}")
    except Exception as _e:
        print(f"[Orchestrator Warning] Could not load master pool: {_e}")

try:
    init_pools()
except Exception:
    pass


# ---------------------------------------------------------
# Dynamic Spatial Shard Registry & Router (Auto-Scaling)
# ---------------------------------------------------------
_spatial_shards_cache = {"data": [], "timestamp": 0}

def get_spatial_building_pools_for_bbox(xmin: float, ymin: float, xmax: float, ymax: float):
    now = time.time()
    # Refresh registry cache every 60 seconds
    if not _spatial_shards_cache["data"] or now - _spatial_shards_cache["timestamp"] > 60:
        m_conn = pg_pool.getconn()
        try:
            cur = m_conn.cursor()
            cur.execute("""
                SELECT neon_project_id, layer_name, partition_key, 
                       min_lon, min_lat, max_lon, max_lat, pooler_uri
                FROM spatial_shard_registry
                WHERE status = 'active';
            """)
            rows = cur.fetchall()
            cur.close()
            _spatial_shards_cache["data"] = rows
            _spatial_shards_cache["timestamp"] = now
            for pid, l_name, part_key, x1, y1, x2, y2, puri in rows:
                if pid not in shard_pools:
                    try:
                        shard_pools[pid] = SimpleConnectionPool(1, 10, puri)
                        print(f"[Spatial Orchestrator] Dynamically connected to shard pool: {part_key} ({pid})")
                    except Exception as pe:
                        print(f"[Spatial Orchestrator Warning] Could not pool {pid}: {pe}")
        except Exception as e:
            print(f"[Spatial Router Warning] Could not read spatial_shard_registry: {e}")
        finally:
            pg_pool.putconn(m_conn)

    matching_pools = []
    for pid, l_name, part_key, x1, y1, x2, y2, puri in _spatial_shards_cache["data"]:
        if l_name == "buildings_3d":
            # Check bounding box overlap: not (disjoint)
            if not (xmax < x1 or xmin > x2 or ymax < y1 or ymin > y2):
                if pid in shard_pools:
                    matching_pools.append((pid, shard_pools[pid]))
    return matching_pools

def get_layer_conn(layer_name: str, xmin: float = None, ymin: float = None, xmax: float = None, ymax: float = None):
    # Dynamic Spatial Shard Routing for Buildings
    if layer_name == 'buildings' and xmin is not None and ymin is not None and xmax is not None and ymax is not None:
        spatial_matches = get_spatial_building_pools_for_bbox(xmin, ymin, xmax, ymax)
        if spatial_matches:
            pid, pool = spatial_matches[0]
            return pool, pool.getconn()

    shard_map = {
        'overture': 'overture_places',
        'population': 'elevation_contours',
        'contours': 'elevation_contours',
        'buildings': 'dhaka_buildings_3d',
        'ai_roads': 'roads_full',
        'waterbodies': 'waterbodies'
    }
    shard_key = shard_map.get(layer_name)
    if shard_key and shard_key in shard_pools:
        return shard_pools[shard_key], shard_pools[shard_key].getconn()
    return pg_pool, pg_pool.getconn()

railway_router = RailwayRouter(DB_URL)
road_router = RoadRouter(DB_URL)
# We will start the graph build thread in the FastAPI startup event below.

def execute_spatial_query(table, minx, miny, maxx, maxy, columns, z):
    # This function is kept for legacy compatibility just in case, though unused now.
    pass

# ---------------------------------------------------------
# Fishing Vessel Activity (Global Fishing Watch 4Wings tiles, proxied + cached)
# Registered BEFORE the generic /api/mvt/{layer}/... route below, since
# FastAPI matches routes in registration order and "fishing" would otherwise
# be swallowed by that route's {layer} wildcard and 404 on the allowed_layers check.
# ---------------------------------------------------------
FISHING_TILE_CACHE_TTL = 3600  # fishing-effort data is daily-aggregated, not truly live
_fishing_tile_cache = {}

@app.get("/api/mvt/fishing/{z}/{x}/{y}.pbf")
def get_fishing_tile(z: int, x: int, y: int):
    if not GFW_API_TOKEN:
        return Response(status_code=404)

    cache_key = (z, x, y)
    now = time.time()
    cached = _fishing_tile_cache.get(cache_key)
    if cached and now - cached[0] < FISHING_TILE_CACHE_TTL:
        return Response(content=cached[1], media_type="application/x-protobuf")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)

    try:
        resp = requests.get(
            f"https://gateway.api.globalfishingwatch.org/v3/4wings/tile/heatmap/{z}/{x}/{y}",
            params={
                "datasets[0]": "public-global-fishing-effort:latest",
                "date-range": f"{start_date},{end_date}",
                "format": "MVT",
                "interval": "DAY"
            },
            headers={"Authorization": f"Bearer {GFW_API_TOKEN}"},
            timeout=10
        )
        if resp.status_code != 200 or not resp.content:
            return Response(status_code=204)

        _fishing_tile_cache[cache_key] = (now, resp.content)
        return Response(content=resp.content, media_type="application/x-protobuf")
    except Exception as e:
        print("Disaster sim error:", e)
        return {"code": "Error", "error": str(e)}

class EvacuateReq(BaseModel):
    lat: float
    lon: float
    radius_km: float

class FloodRequest(BaseModel):
    lat: float
    lon: float
    radius_km: float
    water_level: float

class EpidemicRequest(BaseModel):
    lat: float
    lon: float
    radius_km: float
    days: int

class TrafficRequest(BaseModel):
    lat: float
    lon: float
    radius_km: float
    commuters: int

@app.post("/api/evacuate")
def simulate_evacuation(req: EvacuateReq):
    import time
    from collections import Counter
    
    t0 = time.time()
    radius_meters = req.radius_km * 1000
    
    conn = pg_pool.getconn()
    if not conn:
        return {"code": "Error", "error": "No DB connection available"}
        
    try:
        cursor = conn.cursor()
        
        # 1. Fetch lightweight road topology within bounding box
        # Box must cover the blast radius + extra buffer for hospitals
        # 1 degree is ~111km. Buffer by radius + 5km.
        buffer_deg = (radius_meters + 5000) / 111000.0
        
        cursor.execute("""
            SELECT id, source, target, ST_Length(geometry::geography) 
            FROM roads 
            WHERE geometry && ST_Expand(ST_MakePoint(%s, %s), %s)
        """, (req.lon, req.lat, buffer_deg))
        
        edges = cursor.fetchall()
        
        G = nx.Graph()
        for rid, u, v, weight in edges:
            if u and v:
                G.add_edge(u, v, id=rid, weight=weight)
                
        # 1.5 Fuse fragmented road network using KDTree
        from scipy.spatial import cKDTree
        cursor.execute("""
            SELECT id, ST_X(the_geom), ST_Y(the_geom)
            FROM roads_vertices_pgr
            WHERE the_geom && ST_Expand(ST_MakePoint(%s, %s), %s)
        """, (req.lon, req.lat, buffer_deg))
        nodes = cursor.fetchall()
        
        node_ids = []
        coords = []
        for nid, nx_lon, nx_lat in nodes:
            if nid in G:
                node_ids.append(nid)
                coords.append((nx_lon, nx_lat))
                
        if coords:
            tree = cKDTree(coords)
            distances, indices = tree.query(coords, k=4)
            for i, n_indices in enumerate(indices):
                u = node_ids[i]
                for j, v_idx in enumerate(n_indices[1:]):
                    v = node_ids[v_idx]
                    if u != v and not G.has_edge(u, v):
                        dist = distances[i][j+1] * 111000
                        if dist < 100:
                            G.add_edge(u, v, id=-1, weight=dist)

        # 2. Pick 100 random source vertices inside the blast zone
        cursor.execute("""
            SELECT id FROM roads_vertices_pgr 
            WHERE ST_DWithin(the_geom::geography, ST_MakePoint(%s, %s)::geography, %s)
            LIMIT 100
        """, (req.lon, req.lat, radius_meters))
        sources = [r[0] for r in cursor.fetchall() if r[0] in G]
        
        if not sources:
            return {"code": "Error", "error": "No roads found in the danger zone."}
            
        # 3. Find safe destinations (hospitals)
        cursor.execute("""
            WITH safe_hospitals AS (
                SELECT geometry 
                FROM pois 
                WHERE fclass IN ('hospital', 'clinic') 
                AND NOT ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s) 
                ORDER BY geometry::geography <-> ST_MakePoint(%s, %s)::geography 
                LIMIT 10
            )
            SELECT v.id, ST_AsGeoJSON(h.geometry) 
            FROM safe_hospitals h 
            CROSS JOIN LATERAL (
                SELECT id FROM roads_vertices_pgr v 
                ORDER BY v.the_geom <-> h.geometry 
                LIMIT 1
            ) v
        """, (req.lon, req.lat, radius_meters, req.lon, req.lat))
        
        dest_results = cursor.fetchall()
        destinations = [r[0] for r in dest_results if r[0] in G]
        
        if not destinations:
            return {"code": "Error", "error": "No reachable safe hospitals found nearby."}
            
        dest_geoms = [json.loads(r[1]) for r in dest_results]
            
        # 4. Route using multi-source Dijkstra
        traffic = Counter()
        for src in sources:
            G.add_edges_from([('dummy_target', dst, {'weight': 0}) for dst in destinations])
            try:
                dist, path = nx.single_source_dijkstra(G, src, 'dummy_target', weight='weight')
                path = path[:-1] # remove dummy_target
                for i in range(len(path)-1):
                    u, v = path[i], path[i+1]
                    road_id = G[u][v]['id']
                    traffic[road_id] += 1
            except nx.NetworkXNoPath:
                pass
            G.remove_node('dummy_target')
            
        # 5. Fetch geometries for used routes
        features = []
        if traffic:
            most_used = [k for k, v in traffic.items() if v > 0]
            cursor.execute("""
                SELECT id, ST_AsGeoJSON(geometry) FROM roads WHERE id = ANY(%s)
            """, (most_used,))
            geoms = cursor.fetchall()
            
            for rid, geom_str in geoms:
                features.append({
                    "type": "Feature",
                    "geometry": json.loads(geom_str),
                    "properties": {
                        "traffic": traffic[rid]
                    }
                })
                
        hospital_features = [
            {"type": "Feature", "geometry": geom, "properties": {"type": "safe_hospital"}}
            for geom in dest_geoms
        ]
                
        return {
            "code": "Ok",
            "stats": {
                "civilians_evacuated": len(sources),
                "hospitals_utilized": len(destinations),
                "calculation_time_sec": round(time.time() - t0, 2)
            },
            "features": {
                "evacuation_routes": {"type": "FeatureCollection", "features": features},
                "safe_hospitals": {"type": "FeatureCollection", "features": hospital_features}
            }
        }
    except Exception as e:
        print("Evac sim error:", e)
        return {"code": "Error", "error": str(e)}
    finally:
        cursor.close()
        pg_pool.putconn(conn)

@app.post("/api/simulate_flood")
def simulate_flood(req: FloodRequest):
    import time
    import numpy as np
    from scipy.interpolate import griddata
    import cv2
    from shapely.geometry import Polygon, mapping
    
    t0 = time.time()
    radius_meters = req.radius_km * 1000
    buffer_deg = radius_meters / 111000.0
    
    conn = pg_pool.getconn()
    if not conn:
        return {"code": "Error", "error": "No DB connection"}
        
    try:
        cursor = conn.cursor()
        
        # 1. Fast fetch contour geometries from elevation_contours shard
        c_pool = shard_pools.get('elevation_contours') or pg_pool
        c_conn = c_pool.getconn()
        try:
            with c_conn.cursor() as c_cur:
                c_cur.execute("""
                    SELECT elevation, ST_AsBinary(geometry)
                    FROM contours
                    WHERE geometry && ST_Expand(ST_MakePoint(%s, %s), %s)
                """, (req.lon, req.lat, buffer_deg))
                rows = c_cur.fetchall()
        finally:
            c_pool.putconn(c_conn)
        
        if not rows:
            return {"code": "Error", "error": "No elevation data found in this region."}
            
        from shapely.wkb import loads
        pts_z, pts_x, pts_y = [], [], []
        for elev, wkb_bytes in rows:
            geom = loads(bytes(wkb_bytes))
            if geom.geom_type == 'LineString':
                for coord in geom.coords:
                    pts_z.append(elev)
                    pts_x.append(coord[0])
                    pts_y.append(coord[1])
            elif geom.geom_type == 'MultiLineString':
                for line in geom.geoms:
                    for coord in line.coords:
                        pts_z.append(elev)
                        pts_x.append(coord[0])
                        pts_y.append(coord[1])
            
        z = np.array(pts_z)
        x = np.array(pts_x)
        y = np.array(pts_y)
        
        if len(z) == 0:
            return {"code": "Error", "error": "No points found."}
        
        # 2. Interpolate DEM on 100x100 grid (Blazing fast nearest neighbor)
        import scipy.ndimage
        grid_x, grid_y = np.mgrid[
            req.lon - buffer_deg : req.lon + buffer_deg : 100j,
            req.lat - buffer_deg : req.lat + buffer_deg : 100j
        ]
        
        grid_z = griddata((x, y), z, (grid_x, grid_y), method='nearest')
        
        # Smooth the nearest-neighbor blockiness
        grid_z = scipy.ndimage.gaussian_filter(grid_z, sigma=2.0)
            
        # 3. Create Flood Mask (Relative to lowest point in the area)
        base_elevation = np.nanmin(grid_z)
        target_flood_elevation = base_elevation + req.water_level
        flooded_mask = (grid_z <= target_flood_elevation).astype(np.uint8)
        
        contours, _ = cv2.findContours(flooded_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        min_lon = req.lon - buffer_deg
        max_lon = req.lon + buffer_deg
        min_lat = req.lat - buffer_deg
        max_lat = req.lat + buffer_deg
        w, h = 100, 100
        
        flood_polygons = []
        for cnt in contours:
            if len(cnt) >= 3:
                pts = []
                for pt in cnt[:, 0, :]:
                    px, py = pt[0], pt[1]
                    lon = min_lon + (px / (w - 1)) * (max_lon - min_lon)
                    lat = min_lat + (py / (h - 1)) * (max_lat - min_lat)
                    pts.append((lon, lat))
                pts.append(pts[0])
                poly = Polygon(pts)
                if poly.is_valid and poly.area > 0:
                    flood_polygons.append(mapping(poly))
            
        if not flood_polygons:
            return {"code": "Ok", "stats": {"buildings": 0, "roads_km": 0, "population": 0}, "features": {"flood_zones": {"type": "FeatureCollection", "features": []}}}
            
        # Create a single MultiPolygon GeoJSON string for PostGIS
        flood_geojson = json.dumps({
            "type": "MultiPolygon",
            "coordinates": [p["coordinates"] for p in flood_polygons]
        })
        
        # 4. Calculate Damage (Buildings) across spatial shards
        buildings_destroyed = 0
        b_pools = get_spatial_building_pools_for_bbox(req.lon-buffer_deg, req.lat-buffer_deg, req.lon+buffer_deg, req.lat+buffer_deg)
        for pid, bp in b_pools:
            bconn = bp.getconn()
            try:
                with bconn.cursor() as bcur:
                    bcur.execute("""
                        SELECT COUNT(*) FROM buildings 
                        WHERE ST_Intersects(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                    """, (flood_geojson,))
                    buildings_destroyed += (bcur.fetchone()[0] or 0)
            finally:
                bp.putconn(bconn)
        
        # 5. Calculate Damage (Roads)
        cursor.execute("""
            SELECT COALESCE(SUM(ST_Length(geometry::geography)), 0) FROM roads 
            WHERE ST_Intersects(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
        """, (flood_geojson,))
        roads_destroyed_km = round(cursor.fetchone()[0] / 1000.0, 2)
        
        # 6. Calculate Damage (Population)
        cursor.execute("""
            SELECT COALESCE(SUM(population), 0) FROM population 
            WHERE ST_Intersects(geometry, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
        """, (flood_geojson,))
        population_displaced = int(cursor.fetchone()[0])
        
        features = [{"type": "Feature", "geometry": geom, "properties": {}} for geom in flood_polygons]
        
        return {
            "code": "Ok",
            "stats": {
                "buildings": buildings_destroyed,
                "roads_km": roads_destroyed_km,
                "population": population_displaced,
                "calc_time": round(time.time() - t0, 2)
            },
            "features": {
                "flood_zones": {"type": "FeatureCollection", "features": features}
            }
        }
    except Exception as e:
        print("Flood sim error:", e)
        return {"code": "Error", "error": str(e)}
    finally:
        cursor.close()
        pg_pool.putconn(conn)



# ---------------------------------------------------------
# High-Performance In-Memory LRU Vector Tile Cache
# ---------------------------------------------------------
_mvt_lru_cache = {}

# ---------------------------------------------------------
# Cloudflare Edge CDN Integration & Cache-Tag Purge Engine
# ---------------------------------------------------------
CF_ZONE_ID = os.environ.get("CLOUDFLARE_ZONE_ID", "")
CF_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")

def purge_cloudflare_tag(tag: str):
    """Purge a specific layer or group from Cloudflare Edge worldwide in ~150 ms."""
    if not CF_ZONE_ID or not CF_API_TOKEN:
        return {"status": "skipped", "message": "Cloudflare credentials not set in environment"}
    try:
        url = f"https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/purge_cache"
        headers = {
            "Authorization": f"Bearer {CF_API_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {"tags": [tag]}
        resp = requests.post(url, headers=headers, json=payload, timeout=5)
        return resp.json()
    except Exception as e:
        print(f"Cloudflare purge error ({tag}): {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/cache/purge")
def api_purge_layer(layer: str):
    """API endpoint to trigger instant edge purge for a layer."""
    res = purge_cloudflare_tag(f"layer-{layer}")
    # Also invalidate in-memory LRU cache
    keys_to_del = [k for k in _mvt_lru_cache.keys() if k[0] == layer]
    for k in keys_to_del:
        del _mvt_lru_cache[k]
    return {"purged_layer": layer, "cloudflare": res, "local_tiles_evicted": len(keys_to_del)}

# ---------------------------------------------------------
# PostgreSQL Vector Tiles (MVT) Endpoint
# ---------------------------------------------------------
@app.get("/api/mvt/{layer}/{z}/{x}/{y}.pbf")

def get_mvt(layer: str, z: int, x: int, y: int):
    # Check memory cache for instant zoom-out rendering
    cache_key = (layer, z, x, y)
    if cache_key in _mvt_lru_cache:
        cached_data = _mvt_lru_cache[cache_key]
        if not cached_data:
            return Response(status_code=204)
        return Response(content=cached_data, media_type="application/x-protobuf", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": f"mvt,layer-{layer}"})
    # Performance optimization: Don't load dense/heavy layers when zoomed out
    if layer == 'addresses' and z < 14:
        _mvt_lru_cache[cache_key] = b""
        return Response(status_code=204)
    elif layer in ['pois', 'buildings', 'landuse', 'overture', 'ai_roads', 'transport', 'waterbodies'] and z < 13:
        _mvt_lru_cache[cache_key] = b""
        return Response(status_code=204)
    elif layer == 'celltowers' and z < 12:
        # 230k cell towers: only render at city/neighborhood level to prevent DB lockup
        _mvt_lru_cache[cache_key] = b""
        return Response(status_code=204)
    elif layer == 'population' and z < 11:
        # 155k H3 hexagons: only render at sub-district/neighborhood level
        _mvt_lru_cache[cache_key] = b""
        return Response(status_code=204)
    elif layer == 'contours' and z < 11:
        # 550k contour lines: only render when zoomed in
        _mvt_lru_cache[cache_key] = b""
        return Response(status_code=204)

    # Allowed layers and their properties
    allowed_layers = {
        'pois': 'osm_id, fclass, name',
        'roads': 'osm_id, fclass, name, maxspeed',
        'buildings': 'osm_id, fclass, name',
        'landuse': 'osm_id, fclass, name',
        'overture': 'names, categories',
        'ai_roads': '',
        'celltowers': 'radio, range, samples, mcc, mnc, lac, cid, lon, lat',
        'neighborhoods': 'wof_name, wof_placetype',
        'border': '',
        'transport': 'name, public_transport, railway, amenity',
        'power_grid': 'type, voltage, name',
        'hydrology': 'type, name',
        'contours': 'elevation',
        'railways': 'name, type',
        'population': 'population',
        'addresses': 'housenumber, street, name, full_address, city',
        'airports': 'name, type',
        'helipads': 'name, type',
        'waterbodies': 'osm_id, fclass, name'
    }

    if layer not in allowed_layers:
        return Response(status_code=404)
        
    props = allowed_layers[layer]
    prop_cols = ", ".join([f"t.{col.strip()}" for col in props.split(',') if col.strip()])
    select_clause = "ST_AsMVTGeom(ST_Transform(t.geometry, 3857), bounds.geom) AS geom"
    if prop_cols:
        select_clause += f", {prop_cols}"
    
    where_clause = "ST_Intersects(t.geometry, ST_Transform(bounds.geom, 4326))"
    
    # Progressive loading
    if layer == 'roads':
        if z < 10:
            where_clause += " AND t.fclass IN ('motorway', 'trunk', 'primary')"
        elif z < 13:
            where_clause += " AND t.fclass IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary')"
    elif layer == 'neighborhoods':
        if z < 6:
            return Response(status_code=204)
        elif z < 9:
            where_clause += " AND t.wof_placetype = 'region'"
        elif z < 11:
            where_clause += " AND t.wof_placetype IN ('county', 'region')"
        else:
            where_clause += " AND t.wof_placetype IN ('neighbourhood', 'locality', 'county', 'region', 'macroregion')"
    elif layer == 'hydrology':
        # Bangladesh has ~270k small village ponds mapped individually - keep low zooms to
        # the coastline/river backbone and only reveal ponds/ditches once zoomed in close.
        if z < 8:
            where_clause += " AND t.type IN ('coastline', 'river')"
        elif z < 11:
            where_clause += " AND t.type IN ('coastline', 'river', 'canal', 'lake', 'reservoir', 'wetland', 'lagoon', 'basin', 'oxbow')"
        elif z < 14:
            where_clause += " AND t.type NOT IN ('pond', 'fishpond', 'homestead_pond')"
    elif layer == 'airports':
        where_clause += " AND t.type = 'aerodrome'"
    elif layer == 'helipads':
        where_clause += " AND t.type = 'helipad'"
    elif layer == 'contours':
        # Bangladesh is a mostly-flat delta - dense fine intervals would clutter low zooms
        if z < 9:
            where_clause += " AND t.elevation IN (100, 200, 300, 500, 700, 900)"
        elif z < 12:
            where_clause += " AND t.elevation IN (0, 30, 50, 75, 100, 200, 300, 500, 700, 900)"

    query_layer = 'aeroways' if layer in ('airports', 'helipads') else layer
    query = f"""
        WITH bounds AS (
            SELECT ST_TileEnvelope({z}, {x}, {y}) AS geom
        ),
        mvtgeom AS (
            SELECT {select_clause}
            FROM {query_layer} t, bounds
            WHERE {where_clause}
        )
        SELECT ST_AsMVT(mvtgeom.*, '{layer}') FROM mvtgeom;
    """
    
    # Compute tile envelope bounds for spatial routing
    n_tile = 2.0 ** z
    t_xmin = x / n_tile * 360.0 - 180.0
    t_ymin = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n_tile))))
    t_xmax = (x + 1) / n_tile * 360.0 - 180.0
    t_ymax = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n_tile))))
    pool, conn = get_layer_conn(layer, t_xmin, t_ymin, t_xmax, t_ymax)
    try:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            mvt = cursor.fetchone()[0]
            cursor.close()
        except Exception as q_err:
            # Reconnect if connection was closed by server or timed out
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = pool.getconn()
            cursor = conn.cursor()
            cursor.execute(query)
            mvt = cursor.fetchone()[0]
            cursor.close()
        
        if mvt is None or len(mvt) == 0:
            _mvt_lru_cache[cache_key] = b""
            return Response(status_code=204)
            
        mvt_bytes = bytes(mvt)
        if z <= 12 and len(_mvt_lru_cache) < 8192:
            _mvt_lru_cache[cache_key] = mvt_bytes
            
        return Response(content=mvt_bytes, media_type="application/x-protobuf", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": f"mvt,layer-{layer}"})
    except Exception as e:
        print(f"MVT Error in {layer}: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------
# Custom Railway Routing Endpoint
# ---------------------------------------------------------
class RouteRequest(BaseModel):
    stations: List[List[float]]

@app.post("/api/route_train_db")
def route_train_db(req: RouteRequest):
    if not railway_router.initialized:
        return JSONResponse(status_code=503, content={"error": "Railway router is still initializing..."})
        
    try:
        path_coords = railway_router.route(req.stations)
        if not path_coords:
            return JSONResponse(status_code=400, content={"error": "Could not calculate path."})
            
        return {
            "code": "Ok",
            "routes": [
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": path_coords
                    }
                }
            ]
        }
    except Exception as e:
        print(f"Railway Routing Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------
# Disaster Resiliency Simulator Endpoint
# ---------------------------------------------------------
class DisasterRequest(BaseModel):
    lat: float
    lon: float
    radius_km: float

@app.post("/api/simulate_disaster")
def simulate_disaster(req: DisasterRequest):
    radius_meters = req.radius_km * 1000
    
    conn = pg_pool.getconn()
    try:
        cursor = conn.cursor()
        
        # 1. Primary Damage: Power Grid
        cursor.execute("""
            SELECT ST_AsGeoJSON(ST_Transform(geometry, 4326)) as geom, type, voltage, name
            FROM power_grid
            WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
        """, (req.lon, req.lat, radius_meters))
        
        destroyed_power_lines = [
            {"type": "Feature", "geometry": json.loads(row[0]), "properties": {"type": row[1], "voltage": row[2], "name": row[3]}}
            for row in cursor.fetchall()
        ]
        
        # 2. Logistics Failure: Roads
        # Use bounding box intersection first for speed, then exact DWithin
        cursor.execute("""
            SELECT ST_AsGeoJSON(ST_Transform(geometry, 4326)) as geom, fclass, name
            FROM roads
            WHERE geometry && ST_Transform(ST_Buffer(ST_MakePoint(%s, %s)::geography, %s)::geometry, 4326)
            AND ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
        """, (req.lon, req.lat, radius_meters, req.lon, req.lat, radius_meters))
        
        destroyed_roads = [
            {"type": "Feature", "geometry": json.loads(row[0]), "properties": {"fclass": row[1], "name": row[2]}}
            for row in cursor.fetchall()
        ]
        
        # 3. Cascading Failure: Cell Towers
        # Destroyed if in blast radius OR if within 2km of a destroyed power line
        cursor.execute("""
            WITH blast_zone AS (
                SELECT ST_MakePoint(%s, %s)::geography AS geom
            ),
            destroyed_power AS (
                SELECT geometry
                FROM power_grid
                WHERE ST_DWithin(geometry::geography, (SELECT geom FROM blast_zone), %s)
            ),
            power_buffer AS (
                SELECT ST_Transform(ST_Union(ST_Buffer(ST_Transform(geometry, 3857), 2000)), 4326) as geom
                FROM destroyed_power
            )
            SELECT ST_AsGeoJSON(ST_Transform(geometry, 4326)) as geom, radio, range
            FROM celltowers
            WHERE ST_DWithin(geometry::geography, (SELECT geom FROM blast_zone), %s)
            OR ( (SELECT geom FROM power_buffer) IS NOT NULL AND ST_Intersects(geometry, (SELECT geom FROM power_buffer)) )
        """, (req.lon, req.lat, radius_meters, radius_meters))
        
        dead_celltowers = [
            {"type": "Feature", "geometry": json.loads(row[0]), "properties": {"radio": row[1], "range": row[2]}}
            for row in cursor.fetchall()
        ]
        
        # 4. Civilian Casualties (POIs)
        cursor.execute("""
            SELECT fclass, COUNT(*) 
            FROM pois
            WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
            GROUP BY fclass
        """, (req.lon, req.lat, radius_meters))
        
        poi_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        cursor.close()
        
        return {
            "code": "Ok",
            "stats": {
                "power_lines": len(destroyed_power_lines),
                "roads": len(destroyed_roads),
                "cell_towers": len(dead_celltowers),
                "pois": poi_stats
            },
            "features": {
                "power_grid": {"type": "FeatureCollection", "features": destroyed_power_lines},
                "roads": {"type": "FeatureCollection", "features": destroyed_roads},
                "celltowers": {"type": "FeatureCollection", "features": dead_celltowers}
            }
        }
    except Exception as e:
        print(f"Disaster Simulation Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        pg_pool.putconn(conn)


# ---------------------------------------------------------
# Local Legacy Routes (Tiles, Borders, Neighborhoods)
# ---------------------------------------------------------
from collections import OrderedDict

# ---------------------------------------------------------
# High-Performance In-Memory LRU Tile Cache (4,096 PNG tiles)
# ---------------------------------------------------------
class LRUTileCache:
    def __init__(self, capacity: int = 4096):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def set(self, key, value):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

_tile_lru_cache = LRUTileCache(capacity=4096)

thread_local = threading.local()

def get_mbtiles_conn():
    if not hasattr(thread_local, "conn"):
        mbtiles_path = r"..\data\basemap_highres.mbtiles"
        if os.path.exists(mbtiles_path):
            thread_local.conn = sqlite3.connect(mbtiles_path)
        else:
            thread_local.conn = None
    return thread_local.conn

def get_hillshade_conn():
    if not hasattr(thread_local, "hillshade_conn"):
        mbtiles_path = r"..\data\hillshade.mbtiles"
        if os.path.exists(mbtiles_path):
            thread_local.hillshade_conn = sqlite3.connect(mbtiles_path)
        else:
            thread_local.hillshade_conn = None
    return thread_local.hillshade_conn

def get_floodtiles_conn():
    if not hasattr(thread_local, "flood_conn"):
        mbtiles_path = r"..\data\floodzones.mbtiles"
        if os.path.exists(mbtiles_path):
            thread_local.flood_conn = sqlite3.connect(mbtiles_path)
        else:
            thread_local.flood_conn = None
    return thread_local.flood_conn

@app.get("/api/tiles/{z}/{x}/{y}.png")
def get_tile(z: int, x: int, y: int):
    tms_y = (1 << z) - 1 - y
    cache_key = ("basemap", z, x, tms_y)
    cached = _tile_lru_cache.get(cache_key)
    if cached is not None:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})

    # 1. Query Neon Shard 5 (raster_tiles)
    if "raster_tiles" in shard_pools:
        pool = shard_pools["raster_tiles"]
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT tile_data FROM basemap_tiles WHERE zoom_level=%s AND tile_column=%s AND tile_row=%s;", (z, x, tms_y))
            row = cur.fetchone()
            cur.close()
            if row and row[0]:
                tile_bytes = bytes(row[0])
                _tile_lru_cache.set(cache_key, tile_bytes)
                return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
        except Exception as e:
            print(f"Neon basemap tile error: {e}")
        finally:
            pool.putconn(conn)

    # 2. Local SQLite fallback
    conn = get_mbtiles_conn()
    if conn:
        c = conn.cursor()
        c.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
        row = c.fetchone()
        if row and row[0]:
            tile_bytes = row[0]
            _tile_lru_cache.set(cache_key, tile_bytes)
            return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
    return Response(status_code=404)

@app.get("/api/hillshade/{z}/{x}/{y}.png")
def get_hillshade_tile(z: int, x: int, y: int):
    tms_y = (1 << z) - 1 - y
    cache_key = ("hillshade", z, x, tms_y)
    cached = _tile_lru_cache.get(cache_key)
    if cached is not None:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})

    # 1. Query Neon Shard 5 (raster_tiles)
    if "raster_tiles" in shard_pools:
        pool = shard_pools["raster_tiles"]
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT tile_data FROM hillshade_tiles WHERE zoom_level=%s AND tile_column=%s AND tile_row=%s;", (z, x, tms_y))
            row = cur.fetchone()
            cur.close()
            if row and row[0]:
                tile_bytes = bytes(row[0])
                _tile_lru_cache.set(cache_key, tile_bytes)
                return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
        except Exception as e:
            print(f"Neon hillshade tile error: {e}")
        finally:
            pool.putconn(conn)

    # 2. Local SQLite fallback
    conn = get_hillshade_conn()
    if conn:
        c = conn.cursor()
        c.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
        row = c.fetchone()
        if row and row[0]:
            tile_bytes = row[0]
            _tile_lru_cache.set(cache_key, tile_bytes)
            return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
    return Response(status_code=404)

@app.get("/api/floodtiles/{z}/{x}/{y}.png")
def get_flood_tile(z: int, x: int, y: int):
    tms_y = (1 << z) - 1 - y
    cache_key = ("flood", z, x, tms_y)
    cached = _tile_lru_cache.get(cache_key)
    if cached is not None:
        return Response(content=cached, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})

    # 1. Query Neon Shard 5 (raster_tiles)
    if "raster_tiles" in shard_pools:
        pool = shard_pools["raster_tiles"]
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT tile_data FROM flood_tiles WHERE zoom_level=%s AND tile_column=%s AND tile_row=%s;", (z, x, tms_y))
            row = cur.fetchone()
            cur.close()
            if row and row[0]:
                tile_bytes = bytes(row[0])
                _tile_lru_cache.set(cache_key, tile_bytes)
                return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
        except Exception as e:
            print(f"Neon flood tile error: {e}")
        finally:
            pool.putconn(conn)

    # 2. Local SQLite fallback
    conn = get_floodtiles_conn()
    if conn:
        c = conn.cursor()
        c.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
        row = c.fetchone()
        if row and row[0]:
            tile_bytes = row[0]
            _tile_lru_cache.set(cache_key, tile_bytes)
            return Response(content=tile_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=604800, stale-while-revalidate=86400", "Cache-Tag": "raster-tiles"})
    return Response(status_code=404)

# ---------------------------------------------------------
# Geocoding Search Endpoint (pg_trgm fuzzy search across
# addresses, POIs, neighborhoods and road names)
# ---------------------------------------------------------
@app.get("/api/geocode")
def geocode(q: str):
    q = q.strip()
    if len(q) < 2:
        return JSONResponse(content={"results": []})

    conn = pg_pool.getconn()
    try:
        cursor = conn.cursor()
        like_pattern = f"%{q}%"
        query = """
            SELECT label, source, lon, lat, similarity(label, %(q)s) AS score
            FROM (
                SELECT full_address AS label, 'address' AS source,
                       ST_X(geometry) AS lon, ST_Y(geometry) AS lat
                FROM addresses
                WHERE full_address ILIKE %(like)s OR full_address %% %(q)s

                UNION ALL

                SELECT name AS label, 'poi' AS source,
                       ST_X(ST_Centroid(geometry)) AS lon, ST_Y(ST_Centroid(geometry)) AS lat
                FROM pois
                WHERE name IS NOT NULL AND name <> '' AND (name ILIKE %(like)s OR name %% %(q)s)

                UNION ALL

                SELECT wof_name AS label, 'place' AS source,
                       ST_X(ST_Centroid(geometry)) AS lon, ST_Y(ST_Centroid(geometry)) AS lat
                FROM neighborhoods
                WHERE wof_name IS NOT NULL AND (wof_name ILIKE %(like)s OR wof_name %% %(q)s)

                UNION ALL

                SELECT name AS label, 'road' AS source,
                       ST_X(ST_Centroid(geometry)) AS lon, ST_Y(ST_Centroid(geometry)) AS lat
                FROM roads
                WHERE name IS NOT NULL AND name <> '' AND (name ILIKE %(like)s OR name %% %(q)s)
            ) combined
            ORDER BY score DESC
            LIMIT 10;
        """
        cursor.execute(query, {"q": q, "like": like_pattern})
        rows = cursor.fetchall()
        results = [{"label": r[0], "source": r[1], "lon": r[2], "lat": r[3]} for r in rows]
        return JSONResponse(content={"results": results})
    except Exception as e:
        print(f"Geocode Error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        pg_pool.putconn(conn)


MAX_REVERSE_GEOCODE_DISTANCE_M = 5000

@app.get("/api/reverse-geocode")
def reverse_geocode(lat: float, lon: float):
    conn = pg_pool.getconn()
    try:
        cursor = conn.cursor()
        query = """
            WITH pt AS (
                SELECT ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326) AS geom
            ),
            addr AS (
                SELECT full_address AS label, 'address' AS source, geometry AS geom
                FROM addresses
                WHERE full_address IS NOT NULL
                ORDER BY geometry <-> (SELECT geom FROM pt)
                LIMIT 1
            ),
            poi AS (
                SELECT name AS label, 'poi' AS source, geometry AS geom
                FROM pois
                WHERE name IS NOT NULL AND name <> ''
                ORDER BY geometry <-> (SELECT geom FROM pt)
                LIMIT 1
            ),
            road AS (
                SELECT name AS label, 'road' AS source, geometry AS geom
                FROM roads
                WHERE name IS NOT NULL AND name <> ''
                ORDER BY geometry <-> (SELECT geom FROM pt)
                LIMIT 1
            ),
            candidates AS (
                SELECT * FROM addr
                UNION ALL SELECT * FROM poi
                UNION ALL SELECT * FROM road
            )
            SELECT label, source,
                   ST_X(ST_ClosestPoint(geom, (SELECT geom FROM pt))) AS lon,
                   ST_Y(ST_ClosestPoint(geom, (SELECT geom FROM pt))) AS lat,
                   ST_Distance(geom::geography, (SELECT geom FROM pt)::geography) AS distance_m
            FROM candidates
            ORDER BY distance_m ASC
            LIMIT 1;
        """
        cursor.execute(query, {"lat": lat, "lon": lon})
        row = cursor.fetchone()

        if not row:
            return JSONResponse(content={"error": "No addressed feature found nearby"}, status_code=404)

        label, source, res_lon, res_lat, distance_m = row
        if distance_m > MAX_REVERSE_GEOCODE_DISTANCE_M:
            return JSONResponse(content={"error": f"Nothing addressed within {MAX_REVERSE_GEOCODE_DISTANCE_M/1000:.0f}km"}, status_code=404)

        return JSONResponse(content={
            "label": label, "source": source,
            "lon": res_lon, "lat": res_lat,
            "distance_m": round(distance_m, 1)
        })
    except Exception as e:
        print(f"Reverse Geocode Error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        pg_pool.putconn(conn)


# ---------------------------------------------------------
# Shortest Path Routing Endpoint (pgRouting)
# ---------------------------------------------------------
@app.get("/api/route")
def get_route(slat: float, slon: float, elat: float, elon: float):
    if not road_router.initialized:
        return JSONResponse(status_code=503, content={"error": "Road router is still initializing (this may take up to a minute)..."})
        
    try:
        path_coords = road_router.route([[slat, slon], [elat, elon]])
        if not path_coords:
            return JSONResponse(status_code=400, content={"error": "Could not calculate path."})
            
        # path_coords from road_router are [lon, lat]
        
        return JSONResponse(content={
            "type": "Feature", 
            "properties": {"steps": [{"name": "Road", "distance": 0}]},
            "geometry": {
                "type": "LineString",
                "coordinates": path_coords
            }
        })
    except Exception as e:
        print(f"Routing Error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ---------------------------------------------------------
# Live Flight Tracking (OpenSky Network, server-side cached)
# ---------------------------------------------------------
FLIGHT_BBOX = (19.59, 87.01, 27.63, 93.68)  # lamin, lomin, lamax, lomax (Bangladesh + 1deg pad)
FLIGHT_CACHE_TTL = 30  # seconds
_flight_cache = {"data": [], "timestamp": 0.0}
_opensky_auth = (OPENSKY_USERNAME, OPENSKY_PASSWORD) if OPENSKY_USERNAME and OPENSKY_PASSWORD else None

@app.get("/api/flights")
def get_flights():
    now = time.time()
    if now - _flight_cache["timestamp"] < FLIGHT_CACHE_TTL:
        return JSONResponse(content={"flights": _flight_cache["data"], "cached": True})

    lamin, lomin, lamax, lomax = FLIGHT_BBOX
    try:
        resp = requests.get(
            "https://opensky-network.org/api/states/all",
            params={"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax},
            auth=_opensky_auth,
            timeout=20
        )
        if resp.status_code != 200:
            raise Exception(f"OpenSky returned HTTP {resp.status_code}")

        raw = resp.json()
        flights = []
        for s in raw.get("states") or []:
            if s[5] is None or s[6] is None:
                continue
            flights.append({
                "icao24": s[0],
                "callsign": (s[1] or "").strip(),
                "origin_country": s[2],
                "lon": s[5],
                "lat": s[6],
                "altitude_m": s[7],
                "on_ground": s[8],
                "velocity_ms": s[9],
                "true_track": s[10],
                "vertical_rate": s[11],
            })

        _flight_cache["data"] = flights
        _flight_cache["timestamp"] = now
        return JSONResponse(content={"flights": flights, "cached": False})

    except Exception as e:
        print(f"OpenSky fetch error: {e}")
        # Serve the last known-good snapshot rather than nothing
        return JSONResponse(content={"flights": _flight_cache["data"], "cached": True, "error": str(e)})


# ---------------------------------------------------------
# Live Ship Tracking (aisstream.io, persistent WebSocket -> in-memory snapshot)
# ---------------------------------------------------------
# Bangladesh coastal waters + Bay of Bengal approach lanes to Chittagong/Mongla
SHIP_BBOX = [[[18.0, 88.0], [26.7, 93.0]]]
SHIP_STALE_SECONDS = 600  # drop vessels we haven't heard from in 10 minutes

_ship_cache = {}
_ship_cache_lock = threading.Lock()

async def ais_listener():
    if not AISSTREAM_API_KEY:
        print("AISSTREAM_API_KEY not set - live ship tracking disabled")
        return

    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                await ws.send(json.dumps({
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": SHIP_BBOX,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }))
                print("Connected to aisstream.io - tracking vessels near Bangladesh")

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        meta = msg.get("MetaData", {}) or {}
                        mmsi = meta.get("MMSI")
                        if mmsi is None:
                            continue

                        with _ship_cache_lock:
                            entry = _ship_cache.get(mmsi, {"mmsi": mmsi, "name": ""})

                            msg_type = msg.get("MessageType")
                            if msg_type == "PositionReport":
                                pr = msg["Message"]["PositionReport"]
                                entry["lat"] = pr.get("Latitude", meta.get("latitude"))
                                entry["lon"] = pr.get("Longitude", meta.get("longitude"))
                                entry["sog"] = pr.get("Sog")
                                entry["cog"] = pr.get("Cog")
                                entry["heading"] = pr.get("TrueHeading")
                                entry["updated"] = time.time()
                            elif msg_type == "ShipStaticData":
                                sd = msg["Message"]["ShipStaticData"]
                                if sd.get("ShipName"):
                                    entry["name"] = sd["ShipName"].strip()

                            if meta.get("ShipName"):
                                entry["name"] = meta["ShipName"].strip()

                            _ship_cache[mmsi] = entry
                    except Exception as parse_err:
                        print(f"AIS message parse error: {parse_err}")
        except Exception as e:
            print(f"AIS websocket error: {e} - reconnecting in 10s")
            await asyncio.sleep(10)

@app.on_event("startup")
async def start_events():
    import threading
    threading.Thread(target=railway_router.build_graph, daemon=True).start()
    threading.Thread(target=road_router.build_graph, daemon=True).start()
    asyncio.create_task(ais_listener())

@app.get("/api/ships")
def get_ships():
    now = time.time()
    with _ship_cache_lock:
        ships = [
            s for s in _ship_cache.values()
            if s.get("lat") is not None and now - s.get("updated", 0) < SHIP_STALE_SECONDS
        ]
    return JSONResponse(content={"ships": ships})


# ---------------------------------------------------------
# Earthquakes (USGS, free/no-auth, cached)
# ---------------------------------------------------------
# Bangladesh + surrounding seismic zones (Himalayan front, Shillong Plateau,
# Indo-Burman/Arakan subduction) since regional quakes are what actually matter here
EARTHQUAKE_BBOX = (18.0, 85.0, 28.0, 96.0)  # minlat, minlon, maxlat, maxlon
EARTHQUAKE_CACHE_TTL = 300  # 5 minutes
_earthquake_cache = {"data": None, "timestamp": 0.0}

@app.get("/api/earthquakes")
def get_earthquakes():
    now = time.time()
    if _earthquake_cache["data"] and now - _earthquake_cache["timestamp"] < EARTHQUAKE_CACHE_TTL:
        return JSONResponse(content=_earthquake_cache["data"])

    minlat, minlon, maxlat, maxlon = EARTHQUAKE_BBOX
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=90)

    try:
        resp = requests.get(
            "https://earthquake.usgs.gov/fdsnws/event/1/query",
            params={
                "format": "geojson",
                "starttime": start_time.strftime("%Y-%m-%d"),
                "endtime": end_time.strftime("%Y-%m-%d"),
                "minlatitude": minlat,
                "maxlatitude": maxlat,
                "minlongitude": minlon,
                "maxlongitude": maxlon,
                "minmagnitude": 2.5,
            },
            timeout=10
        )
        if resp.status_code != 200:
            raise Exception(f"USGS returned HTTP {resp.status_code}")

        data = resp.json()
        _earthquake_cache["data"] = data
        _earthquake_cache["timestamp"] = now
        return JSONResponse(content=data)
    except Exception as e:
        print(f"USGS earthquake fetch error: {e}")
        if _earthquake_cache["data"]:
            return JSONResponse(content=_earthquake_cache["data"])
        return JSONResponse(content={"type": "FeatureCollection", "features": []})


# ---------------------------------------------------------
# Disaster Alerts (GDACS, free/no-auth, cached)
# ---------------------------------------------------------
# South Asia + Bay of Bengal / northern Indian Ocean cyclone basin
DISASTER_BBOX = (-10.0, 60.0, 40.0, 100.0)  # minlat, minlon, maxlat, maxlon
DISASTER_CACHE_TTL = 900  # 15 minutes - GDACS updates a few times a day
_disaster_cache = {"data": None, "timestamp": 0.0}

@app.get("/api/disasters")
def get_disasters():
    now = time.time()
    if _disaster_cache["data"] and now - _disaster_cache["timestamp"] < DISASTER_CACHE_TTL:
        return JSONResponse(content=_disaster_cache["data"])

    minlat, minlon, maxlat, maxlon = DISASTER_BBOX
    try:
        resp = requests.get(
            "https://www.gdacs.org/gdacsapi/api/events/geteventlist/MAP",
            timeout=15
        )
        if resp.status_code != 200:
            raise Exception(f"GDACS returned HTTP {resp.status_code}")

        raw = resp.json()
        seen_ids = set()
        disasters = []
        for feat in raw.get("features", []):
            props = feat.get("properties", {})
            if props.get("iscurrent") != "true":
                continue
            if props.get("eventtype") == "EQ":
                continue  # covered by the dedicated USGS earthquake layer
            if feat.get("geometry", {}).get("type") != "Point":
                continue

            eventid = props.get("eventid")
            if eventid in seen_ids:
                continue

            lon, lat = feat["geometry"]["coordinates"]
            if not (minlat <= lat <= maxlat and minlon <= lon <= maxlon):
                continue

            seen_ids.add(eventid)
            disasters.append({
                "eventtype": props.get("eventtype"),
                "name": props.get("name"),
                "alertlevel": props.get("alertlevel"),
                "country": props.get("country"),
                "fromdate": props.get("fromdate"),
                "todate": props.get("todate"),
                "lat": lat,
                "lon": lon,
                "report_url": props.get("url", {}).get("report"),
            })

        result = {"disasters": disasters}
        _disaster_cache["data"] = result
        _disaster_cache["timestamp"] = now
        return JSONResponse(content=result)
    except Exception as e:
        print(f"GDACS fetch error: {e}")
        if _disaster_cache["data"]:
            return JSONResponse(content=_disaster_cache["data"])
        return JSONResponse(content={"disasters": []})


# Serve the frontend directory so we can just open http://127.0.0.1:8001/
import urllib.request
import json
from fastapi.responses import JSONResponse

@app.get("/api/ships/bd")
def get_live_ships_bd():
    url = "https://www.marinetraffic.com/getData/get_data_json_4/z:7/X:48/Y:27/station:0"
    headers = {
        'accept': '*/*',
        'referer': 'https://www.marinetraffic.com/en/ais/home/centerx:90.7/centery:22.1/zoom:7',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        'sec-ch-ua': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw_data = response.read()
            data = json.loads(raw_data)
            return JSONResponse(content=data)
    except Exception as e:
        print(f"MarineTraffic fetch error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------
# GloFAS Flood Forecasting (Copernicus Global Flood Awareness System,
# public OGC WMS - proxied + cached since forecasts only update ~1-2x/day)
# ---------------------------------------------------------
GLOFAS_WEB_MERCATOR_EXTENT = 20037508.342789244
GLOFAS_LAYERS = {
    "forecast": "FloodSummary1_30",     # 15-day flood summary (forward-looking forecast)
    "gauges": "reportingPoints",        # river gauge stations with return-period alert levels
}
GLOFAS_CACHE_TTL = 10800  # 3 hours
_glofas_tile_cache = {}

@app.get("/api/wms/glofas/{layer_key}/{z}/{x}/{y}.png")
def get_glofas_tile(layer_key: str, z: int, x: int, y: int):
    if layer_key not in GLOFAS_LAYERS:
        return Response(status_code=404)

    cache_key = (layer_key, z, x, y)
    now = time.time()
    cached = _glofas_tile_cache.get(cache_key)
    if cached and now - cached[0] < GLOFAS_CACHE_TTL:
        return Response(content=cached[1], media_type="image/png")

    n = 2 ** z
    tile_size_m = (2 * GLOFAS_WEB_MERCATOR_EXTENT) / n
    minx = x * tile_size_m - GLOFAS_WEB_MERCATOR_EXTENT
    maxx = minx + tile_size_m
    maxy = GLOFAS_WEB_MERCATOR_EXTENT - y * tile_size_m
    miny = maxy - tile_size_m

    today = datetime.utcnow().strftime("%Y-%m-%dT00:00Z")

    try:
        resp = requests.get(
            "https://ows.globalfloods.eu/glofas-ows/ows.py",
            params={
                "service": "WMS", "version": "1.3.0", "request": "GetMap",
                "layers": GLOFAS_LAYERS[layer_key], "styles": "default",
                "crs": "EPSG:3857",
                "bbox": f"{minx},{miny},{maxx},{maxy}",
                "width": 256, "height": 256,
                "format": "image/png", "transparent": "true",
                "time": today,
            },
            timeout=10
        )
        if resp.status_code != 200 or not resp.headers.get("content-type", "").startswith("image/"):
            return Response(status_code=204)

        _glofas_tile_cache[cache_key] = (now, resp.content)
        return Response(content=resp.content, media_type="image/png")
    except Exception as e:
        print(f"GloFAS WMS error: {e}")
        return Response(status_code=204)


# ---------------------------------------------------------
# Live Satellite Tracking (CelesTrak TLEs + SGP4 propagation via skyfield)
# ---------------------------------------------------------
SATELLITE_GROUPS = ["stations", "weather", "visual", "goes"]
SATELLITE_TLE_CACHE_TTL = 21600  # 6 hours - TLEs barely change
SATELLITE_POS_CACHE_TTL = 30     # 30s - satellites move ~7.5 km/s, recompute often
SATELLITE_BBOX = (5.0, 70.0, 40.0, 110.0)  # minlat, minlon, maxlat, maxlon (South Asia region, wide enough that something is usually passing through)

_sat_ts = sf_load.timescale(builtin=True)
_sat_tle_cache = {"sats": [], "timestamp": 0.0}
_sat_pos_cache = {"data": [], "timestamp": 0.0}

def load_satellite_tles():
    now = time.time()
    if _sat_tle_cache["sats"] and now - _sat_tle_cache["timestamp"] < SATELLITE_TLE_CACHE_TTL:
        return _sat_tle_cache["sats"]

    all_sats = []
    seen_names = set()
    for group in SATELLITE_GROUPS:
        try:
            resp = requests.get(
                "https://celestrak.org/NORAD/elements/gp.php",
                params={"GROUP": group, "FORMAT": "tle"},
                timeout=20
            )
            lines = resp.text.strip().split("\n")
            for i in range(0, len(lines) - 2, 3):
                name = lines[i].strip()
                if name in seen_names:
                    continue
                seen_names.add(name)
                all_sats.append(EarthSatellite(lines[i + 1], lines[i + 2], name, _sat_ts))
        except Exception as e:
            print(f"CelesTrak fetch error for group {group}: {e}")

    if all_sats:
        _sat_tle_cache["sats"] = all_sats
        _sat_tle_cache["timestamp"] = now
    return _sat_tle_cache["sats"]

@app.get("/api/satellites")
def get_satellites():
    now = time.time()
    if _sat_pos_cache["data"] and now - _sat_pos_cache["timestamp"] < SATELLITE_POS_CACHE_TTL:
        return JSONResponse(content={"satellites": _sat_pos_cache["data"]})

    sats = load_satellite_tles()
    t = _sat_ts.now()
    minlat, minlon, maxlat, maxlon = SATELLITE_BBOX

    results = []
    for sat in sats:
        try:
            subpoint = wgs84.subpoint(sat.at(t))
            lat = subpoint.latitude.degrees
            lon = subpoint.longitude.degrees
            if minlat <= lat <= maxlat and minlon <= lon <= maxlon:
                results.append({
                    "name": sat.name,
                    "lat": lat,
                    "lon": lon,
                    "altitude_km": round(subpoint.elevation.km, 1),
                })
        except Exception:
            continue

    _sat_pos_cache["data"] = results
    _sat_pos_cache["timestamp"] = now
    return JSONResponse(content={"satellites": results})


# Serve the frontend directory so we can just open http://127.0.0.1:8001/
@app.get("/")
def serve_index():
    frontend_index = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "index.html"))
    return FileResponse(frontend_index)

@app.get("/api/ffwc/live")
async def get_ffwc_live_data():
    url = "https://api.ffwc.gov.bd/data_load/recent-observed/"
    headers = {
        "accept": "application/json, text/plain, */*",
        "referer": "https://ffwc.gov.bd/app/home",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
        "x-ffwc-internal-key": "FFWC-Project-2026-Secure-V1"
    }
    try:
        # Use httpx or requests, but we have requests imported (let's check imports)
        import requests
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return JSONResponse(content=response.json())
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/geojson/buildings/{z}/{x}/{y}.json")
def get_buildings_geojson(z: int, x: int, y: int):
    if z < 14:
        return JSONResponse(content={"type": "FeatureCollection", "features": []})

    n = 2.0 ** z
    west = x / n * 360.0 - 180.0
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))

    # 1. First Check Dynamic Neon Spatial Shards
    spatial_pools = get_spatial_building_pools_for_bbox(west, south, east, north)
    if spatial_pools:
        for pid, pool in spatial_pools:
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT json_build_object(
                            'type', 'FeatureCollection',
                            'features', COALESCE(json_agg(
                                json_build_object(
                                    'type', 'Feature',
                                    'geometry', ST_AsGeoJSON(geometry)::json,
                                    'properties', json_build_object(
                                        'name', COALESCE(name, ''),
                                        'height', COALESCE(height, 8.0),
                                        'levels', COALESCE(levels, 2)
                                    )
                                )
                            ), '[]'::json)
                        )
                        FROM (
                            SELECT name, height, levels, geometry
                            FROM buildings
                            WHERE geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                            LIMIT 1500
                        ) sub;
                    """, (west, south, east, north))
                    fc = cur.fetchone()[0]
                    if fc and fc.get("features"):
                        return JSONResponse(content=fc)
            except Exception as se:
                print(f"Neon spatial shard query error ({pid}): {se}")
            finally:
                pool.putconn(conn)

    # 2. Fast Parquet Cold-Storage Engine via DuckDB (Fallback for non-migrated regions)
    parquet_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "BD_Buildings_Overture.parquet"))
    if os.path.exists(parquet_path):
        try:
            import duckdb
            dconn = duckdb.connect()
            dconn.execute("INSTALL spatial; LOAD spatial;")
            q = """
                SELECT ST_AsGeoJSON(geometry), COALESCE(height, 8.0), COALESCE(num_floors, 2)
                FROM read_parquet(?)
                WHERE bbox.xmin <= ? AND bbox.xmax >= ?
                  AND bbox.ymin <= ? AND bbox.ymax >= ?
                LIMIT 1500;
            """
            rows = dconn.execute(q, [parquet_path, east, west, north, south]).fetchall()
            dconn.close()
            features = []
            for r in rows:
                if r[0]:
                    features.append({
                        "type": "Feature",
                        "geometry": json.loads(r[0]),
                        "properties": {
                            "height": float(r[1] or 8.0),
                            "levels": int(r[2] or 2)
                        }
                    })
            return JSONResponse(content={"type": "FeatureCollection", "features": features})
        except Exception as de:
            print(f"DuckDB building query error: {de}")

    # 2. Postgres fallback
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]'::json)
                )
                FROM (
                    SELECT name, fclass, levels, height, geom
                    FROM buildings
                    WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                ) as t;
            """, (west, south, east, north))
            result = cur.fetchone()[0]
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"type": "FeatureCollection", "features": []})
    finally:
        pg_pool.putconn(conn)

_wind_cache = {"time": 0, "data": None}

@app.get("/api/weather/wind.json")
def get_wind_data():
    now = time.time()
    if _wind_cache["data"] and (now - _wind_cache["time"]) < 3600:
        return JSONResponse(content=_wind_cache["data"])
        
    lats = [27.0 - i*0.5 for i in range(15)]
    lons = [88.0 + i*0.5 for i in range(11)]
    
    queries_lat = []
    queries_lon = []
    for lat in lats:
        for lon in lons:
            queries_lat.append(lat)
            queries_lon.append(lon)
            
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": ",".join(map(str, queries_lat)),
        "longitude": ",".join(map(str, queries_lon)),
        "current": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "timezone": "auto"
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            u_data = []
            v_data = []
            
            for item in data:
                if 'current' in item and item['current']['wind_speed_10m'] is not None:
                    speed = item['current']['wind_speed_10m']
                    direction = item['current']['wind_direction_10m']
                    rad = math.radians(direction)
                    u = -speed * math.sin(rad)
                    v = -speed * math.cos(rad)
                    u_data.append(round(u, 3))
                    v_data.append(round(v, 3))
                else:
                    u_data.append(0)
                    v_data.append(0)
                    
            velocity_json = [
                {
                    "header": {
                        "parameterCategory": 2,
                        "parameterNumber": 2,
                        "dx": 0.5,
                        "dy": 0.5,
                        "nx": 11,
                        "ny": 15,
                        "lo1": 88.0,
                        "la1": 27.0,
                        "lo2": 93.0,
                        "la2": 20.0
                    },
                    "data": u_data
                },
                {
                    "header": {
                        "parameterCategory": 2,
                        "parameterNumber": 3,
                        "dx": 0.5,
                        "dy": 0.5,
                        "nx": 11,
                        "ny": 15,
                        "lo1": 88.0,
                        "la1": 27.0,
                        "lo2": 93.0,
                        "la2": 20.0
                    },
                    "data": v_data
                }
            ]
            
            _wind_cache["time"] = now
            _wind_cache["data"] = velocity_json
            return JSONResponse(content=velocity_json)
        else:
            return Response(status_code=500)
    except Exception as e:
        print(f"Wind API error: {e}")
        return Response(status_code=500)

@app.get("/api/triangulate")
def triangulate_suspect(lat: float, lon: float):
    # Find 3 nearest cell towers using PostGIS <-> operator
    query = """
        SELECT 
            lat, lon, radio, range, mnc, cid,
            ST_Distance(geometry::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as dist
        FROM celltowers
        ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 3;
    """
    
    # Bangladesh Mobile Network Codes (MCC=470)
    # https://en.wikipedia.org/wiki/Mobile_Network_Codes_in_ITU_region_4xx_(Asia_and_Middle_East)#Bangladesh_–_BD
    PROVIDERS = {
        1: "Grameenphone",
        2: "Robi",
        3: "Banglalink",
        4: "TeleTalk",
        5: "Citycell",
        6: "Airtel (Robi)",
        7: "TeleTalk",
        9: "Banglalink"
    }
    
    conn = None
    try:
        conn = pg_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, (lon, lat, lon, lat))
        rows = cur.fetchall()
        cur.close()
        
        towers = []
        for r in rows:
            dist = r[6]
            simulated_radius = dist + (dist * 0.05)
            
            mnc = r[4]
            provider = PROVIDERS.get(mnc, f"Unknown (MNC:{mnc})")
            
            towers.append({
                "lat": r[0],
                "lon": r[1],
                "radio": r[2],
                "range": r[3],
                "provider": provider,
                "cell_id": r[5],
                "distance_m": dist,
                "radius_m": simulated_radius
            })
            
        return JSONResponse(content={"towers": towers, "target": {"lat": lat, "lon": lon}})
    except Exception as e:
        print(f"Triangulation error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        if conn:
            pg_pool.putconn(conn)

@app.get("/api/buildings_radius")
def get_buildings_radius(lat: float, lon: float, radius: float = 500.0):
    deg_r = radius / 111320.0
    pools = get_spatial_building_pools_for_bbox(lon - deg_r, lat - deg_r, lon + deg_r, lat + deg_r)
    features = []
    try:
        for pid, pool in pools:
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT height, ST_AsGeoJSON(geometry)
                        FROM buildings
                        WHERE ST_DWithin(geometry::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
                        LIMIT 1000
                    """, (lon, lat, radius))
                    for r in cur.fetchall():
                        features.append({
                            "type": "Feature",
                            "properties": {"height": r[0] or 5.0},
                            "geometry": json.loads(r[1])
                        })
            finally:
                pool.putconn(conn)
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": features
        })
    except Exception as e:
        print(f"Buildings radius error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/gas_pipelines")
def get_gas_pipelines():
    conn = None
    try:
        conn = pg_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, type, ST_AsGeoJSON(geometry) FROM gas_infra WHERE geometry IS NOT NULL")
        rows = cur.fetchall()
        
        features = []
        for r in rows:
            feat = {
                "type": "Feature",
                "properties": {
                    "id": r[0],
                    "name": r[1] or "Unknown Pipeline",
                    "type": r[2] or "transmission"
                },
                "geometry": json.loads(r[3])
            }
            features.append(feat)
            
        cur.close()
        return JSONResponse(content={
            "type": "FeatureCollection",
            "features": features
        })
    except Exception as e:
        print(f"Gas Pipeline API Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            pg_pool.putconn(conn)

@app.get("/api/update-bus-coords")
def update_bus_coords():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"

    with open(data_dir / "London_Express_Bus_Schedules_10-08-2026.json", "r", encoding="utf-8") as f:
        london_data = json.load(f)

    with open(data_dir / "bus_station_coords.json", "r", encoding="utf-8") as f:
        coords = json.load(f)

    stations = set()
    for group, schedules in london_data["schedules"].items():
        for route in schedules:
            stations.add(route["starting_counter"])
            stations.add(route["end_counter"])

    new_coords = {
        "Gabtoli Counter": [23.7836, 90.3444],
        "Panthapath Counter": [23.7508, 90.3842],
        "Dampara Counter": [22.3484, 91.8197],
        "Dolphin Mur Counter": [21.4287, 91.9703],
        "Cox'S Bazar": [21.4397, 91.9774],
        "Chittagong": [22.3569, 91.7832],
        "Dhaka": [23.8103, 90.4125],
        "Sylhet": [24.8949, 91.8687],
        "Sylhet Counter": [24.8949, 91.8687],
        "Shaistaganj": [24.3168, 91.3541],
        "Humayun Rashid Chattar Counter": [24.8829, 91.8741]
    }

    added = []
    for station in stations:
        if station not in coords:
            if station in new_coords:
                coords[station] = new_coords[station]
                added.append(station)
            else:
                coords[station] = [23.8103, 90.4125]
                added.append(f"{station} (fallback)")

    with open(data_dir / "bus_station_coords.json", "w", encoding="utf-8") as f:
        json.dump(coords, f, indent=2, ensure_ascii=False)

    return {"status": "ok", "added": added}

@app.post("/api/simulate_epidemic")
def simulate_epidemic(req: EpidemicRequest):
    import json
    import numpy as np
    from scipy.spatial import cKDTree
    
    radius_meters = req.radius_km * 1000
    conn = pg_pool.getconn()
    if not conn:
        return {"code": "Error", "error": "No DB connection available"}
        
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            WITH water AS (
                SELECT ST_Union(geometry) as geom
                FROM hydrology
                WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
                AND type IN ('lake', 'wetland', 'canal', 'river', 'pond', 'basin', 'reservoir', 'drain', 'ditch', 'coastline')
            )
            SELECT 
                ST_AsGeoJSON(p.geometry), 
                p.population,
                (CASE 
                    WHEN w.geom IS NULL THEN 5000 
                    ELSE ST_Distance(p.geometry::geography, w.geom::geography) 
                 END) as dist_to_water
            FROM population p
            LEFT JOIN water w ON true
            WHERE ST_DWithin(p.geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
        """, (req.lon, req.lat, radius_meters, req.lon, req.lat, radius_meters))
        
        hexes = []
        centroids = []
        rows = cursor.fetchall()
        for row in rows:
            geom = json.loads(row[0])
            pop = float(row[1]) if row[1] is not None else 0.0
            dist = float(row[2])
            
            coords = geom['coordinates'][0]
            cx = sum(c[0] for c in coords)/len(coords)
            cy = sum(c[1] for c in coords)/len(coords)
            
            hexes.append({'geom': geom, 'pop': pop, 'dist': dist, 'history': []})
            centroids.append([cx, cy])
            
        if not hexes:
            return {"code": "Error", "error": "No population data found in the area."}
            
        tree = cKDTree(centroids)
        N = len(hexes)
        
        S = np.array([max(10.0, h['pop']) for h in hexes], dtype=float)
        I = np.zeros(N, dtype=float)
        R = np.zeros(N, dtype=float)
        
        _, start_idx = tree.query([req.lon, req.lat])
        initial_cases = min(S[start_idx] * 0.1, 50.0)
        if initial_cases < 5: initial_cases = 5.0
        I[start_idx] = initial_cases
        S[start_idx] -= initial_cases
        
        mosquito_factor = np.array([np.exp(-h['dist'] / 500.0) for h in hexes])
        
        neighbors = tree.query_ball_tree(tree, r=0.015)
        
        days = req.days
        for day in range(days):
            new_I = np.zeros(N)
            for i in range(N):
                if S[i] < 1:
                    continue
                
                pressure = I[i]
                for n_idx in neighbors[i]:
                    if n_idx != i:
                        pressure += I[n_idx] * 0.1
                
                if pressure < 0.1:
                    continue
                    
                beta = 0.1 + (mosquito_factor[i] * 0.6)
                
                infected_this_step = beta * pressure * (S[i] / (S[i] + I[i] + R[i]))
                new_I[i] = min(S[i], infected_this_step)
                
            gamma = 0.15
            recovering = I * gamma
            
            S -= new_I
            I += new_I - recovering
            R += recovering
            
            for i in range(N):
                hexes[i]['history'].append(int(I[i]))
                
        features = []
        for h in hexes:
            if sum(h['history']) > 0:
                features.append({
                    "type": "Feature",
                    "geometry": h['geom'],
                    "properties": {
                        "population": h['pop'],
                        "history": h['history']
                    }
                })
                
        return {"type": "FeatureCollection", "features": features}
        
    except Exception as e:
        print(f"Epidemic Simulation Error: {e}")
        return {"code": "Error", "error": str(e)}
    finally:
        if conn:
            pg_pool.putconn(conn)


@app.post("/api/simulate_traffic")
def simulate_traffic(req: TrafficRequest):
    import networkx as nx
    import json
    import math
    import random
    import numpy as np

    radius_meters = req.radius_km * 1000
    conn = pg_pool.getconn()
    if not conn:
        return {"code": "Error", "error": "No DB connection available"}
        
    try:
        cursor = conn.cursor()
        
        # Load roads in radius
        cursor.execute("""
            SELECT ST_AsGeoJSON(ST_Transform(geometry, 4326)) as geom, 
                   fclass,
                   ST_Length(geometry::geography) as length_m
            FROM roads
            WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
            AND fclass NOT IN ('footway', 'pedestrian', 'path', 'steps', 'track', 'cycleway')
        """, (req.lon, req.lat, radius_meters))
        
        rows = cursor.fetchall()
        
        G = nx.Graph()
        features = []
        for geom_str, fclass, length_m in rows:
            cap = 1000
            if fclass in ['motorway', 'trunk']: cap = 3000
            elif fclass == 'primary': cap = 2000
            elif fclass in ['secondary', 'tertiary']: cap = 1200
            features.append({
                "type": "Feature",
                "geometry": json.loads(geom_str),
                "properties": {
                    "fclass": fclass,
                    "capacity": cap,
                    "length_m": length_m
                }
            })
        
        # Generate traffic based on spatial distribution instead of perfect graph topology
        # This prevents failure due to un-noded road networks (disjoint intersections).
        
        target_pt = (req.lon, req.lat)
        
        def haversine_dist(lon1, lat1, lon2, lat2):
            R = 6371.0 # km
            dlon = math.radians(lon2 - lon1)
            dlat = math.radians(lat2 - lat1)
            a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            return R * c

        # Determine how traffic peaks over the 16 buckets (normal distribution)
        time_dist = np.random.normal(8, 2.5, req.commuters)
        time_dist = np.clip(np.round(time_dist), 0, 15).astype(int)
        bucket_counts = np.bincount(time_dist, minlength=16)
        
        for f in features:
            fclass = f['properties']['fclass']
            cap = f['properties']['capacity']
            bucket_cap = cap / 4.0 
            
            # Find road center
            coords = f['geometry']['coordinates']
            mid_idx = len(coords) // 2
            road_lon, road_lat = coords[mid_idx][0], coords[mid_idx][1]
            
            # Distance to downtown target in km
            dist_km = haversine_dist(road_lon, road_lat, target_pt[0], target_pt[1])
            
            # Base attraction: Closer to center = more traffic. 
            # If distance > radius, attraction drops to near zero.
            attraction = max(0.1, (req.radius_km - dist_km) / req.radius_km)
            
            # Arterial roads get much higher attraction for commuters
            if fclass in ['motorway', 'trunk', 'primary']:
                attraction *= 5.0
            elif fclass in ['secondary', 'tertiary']:
                attraction *= 2.0
            
            # Introduce some random noise for realism
            attraction *= random.uniform(0.7, 1.3)
            
            history = []
            for b in range(16):
                # Total commuters on this road in this bucket
                # This is a scaled fraction of total active commuters
                base_volume = bucket_counts[b] * (attraction / 500.0)
                
                # Roads closer to center peak slightly later (traffic jams propogate)
                delay_buckets = int((req.radius_km - dist_km) / 5.0) 
                
                # We can just use the base volume with the ratio
                ratio = base_volume / max(10, bucket_cap)
                
                # Cap ratio visually at around 1.5 (heavy gridlock)
                ratio = min(ratio, 1.5)
                history.append(round(ratio, 2))
                
            f['properties']['history'] = history
            
        return {"code": "Ok", "type": "FeatureCollection", "features": features, "stats": {"commuters": req.commuters, "radius_km": req.radius_km}}
        
    except Exception as e:
        print(f"Traffic Simulation Error: {e}")
        return {"code": "Error", "error": str(e)}
    finally:
        if conn:
            pg_pool.putconn(conn)

class DispatchRequest(BaseModel):
    lat: float
    lon: float
    emergency_type: str # 'hospital', 'police', 'fire_station'

@app.post("/api/simulate_dispatch")
def simulate_dispatch(req: DispatchRequest):
    conn = None
    try:
        conn = pg_pool.getconn()
        cursor = conn.cursor()
        
        incident_wkt = f"POINT({req.lon} {req.lat})"
        
        # 1. Find nearest POI
        cursor.execute("""
            SELECT name, ST_X(geometry), ST_Y(geometry)
            FROM pois 
            WHERE fclass = %s 
            ORDER BY geometry <-> ST_SetSRID(ST_GeomFromText(%s), 4326) 
            LIMIT 1;
        """, (req.emergency_type, incident_wkt))
        poi_row = cursor.fetchone()
        if not poi_row:
            return {"code": "Error", "error": f"No {req.emergency_type} found"}
            
        poi_name, poi_lon, poi_lat = poi_row
        
        # 2. Find start and end vertices for pgRouting (Multiple Candidates for disconnected graph)
        cursor.execute("""
            SELECT source FROM roads 
            ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 5;
        """, (poi_lon, poi_lat))
        start_candidates = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT target FROM roads 
            ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 5;
        """, (req.lon, req.lat))
        end_candidates = [row[0] for row in cursor.fetchall()]
        
        # 3. Dynamic Bounding Box for pgRouting
        expand_deg = max(abs(poi_lon - req.lon), abs(poi_lat - req.lat)) * 1.5 + 0.02
        center_lon = (poi_lon + req.lon) / 2.0
        center_lat = (poi_lat + req.lat) / 2.0
        
        # 4. Execute pgr_dijkstra with fallback candidates
        rows = []
        best_start = None
        best_end = None
        
        for sv in start_candidates:
            for ev in end_candidates:
                query = f"""
                    SELECT ST_AsGeoJSON(r.geometry), r.cost
                    FROM pgr_dijkstra(
                        'SELECT id, source, target, cost, reverse_cost FROM roads WHERE geometry && ST_Expand(ST_SetSRID(ST_MakePoint({center_lon}, {center_lat}), 4326), {expand_deg})',
                        %s, %s, false
                    ) as d
                    JOIN roads r ON d.edge = r.id
                    ORDER BY d.seq;
                """
                cursor.execute(query, (sv, ev))
                current_rows = cursor.fetchall()
                if current_rows:
                    rows = current_rows
                    best_start, best_end = sv, ev
                    break
            if rows:
                break
        
        if not rows:
            print(f"Routing failed! Falling back to direct path.")
            # Fallback to a straight line (Helicopter mode)
            features = [{
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [poi_lon, poi_lat],
                        [req.lon, req.lat]
                    ]
                },
                "properties": {"fallback": True}
            }]
            
            import math
            # Rough distance in km
            dist_km = math.sqrt((poi_lon - req.lon)**2 + (poi_lat - req.lat)**2) * 111.0
            
            return {
                "code": "Ok", 
                "type": "FeatureCollection", 
                "features": features, 
                "stats": {
                    "station_name": poi_name + " (Direct Flight Fallback)",
                    "station_coords": [round(poi_lat, 5), round(poi_lon, 5)],
                    "incident_coords": [round(req.lat, 5), round(req.lon, 5)],
                    "distance_km": round(dist_km, 2),
                    "eta_mins": round(dist_km / 80.0 * 60.0, 1) # 80km/h for direct flight
                }
            }
            
        features = []
        total_cost = 0
        for r in rows:
            geom = json.loads(r[0])
            total_cost += r[1]
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {}
            })
            
        dist_km = total_cost * 111.0
        return {
            "code": "Ok", 
            "type": "FeatureCollection", 
            "features": features, 
            "stats": {
                "station_name": poi_name,
                "station_coords": [round(poi_lat, 5), round(poi_lon, 5)],
                "incident_coords": [round(req.lat, 5), round(req.lon, 5)],
                "distance_km": round(dist_km, 2),
                "eta_mins": round(dist_km / 40.0 * 60.0, 1) # 40km/h
            }
        }
    except Exception as e:
        print(f"Dispatch Error: {e}")
        return {"code": "Error", "error": str(e)}
    finally:
        if conn:
            cursor.close()
            pg_pool.putconn(conn)
class MegaProjectRequest(BaseModel):
    coordinates: list # list of [lat, lon]

@app.post("/api/simulate_megaproject")
def simulate_megaproject(req: MegaProjectRequest):
    conn = None
    try:
        conn = pg_pool.getconn()
        conn.autocommit = False # We manage transaction manually
        cursor = conn.cursor()
        
        # 1. Get nodes near start and end of drawn road
        start_lat, start_lon = req.coordinates[0]
        end_lat, end_lon = req.coordinates[-1]
        
        cursor.execute("SELECT source FROM roads ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 5", (start_lon, start_lat))
        origins = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT target FROM roads ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 5", (end_lon, end_lat))
        destinations = [row[0] for row in cursor.fetchall()]
        
        # 2. Baseline Simulation (Using 5x5 pairs)
        try:
            cursor.execute(f"""
                SELECT AVG(agg_cost) FROM pgr_dijkstraCost(
                    'SELECT id, source, target, cost, reverse_cost FROM roads',
                    ARRAY{origins},
                    ARRAY{destinations},
                    false
                )
            """)
            baseline_avg = cursor.fetchone()[0] or 0
        except Exception as e:
            print("Baseline query failed:", e)
            baseline_avg = 0
            conn.rollback()
            cursor = conn.cursor()
            
        # If cost is in meters, convert to km (cost / 1000). Then distance / speed * 60.
        # Multiply by 25 to represent the 25 total routing pairs.
        baseline_mins = round(((baseline_avg / 1000.0) / 40.0 * 60.0) * 25, 1)
        
        # Fallback if graph is completely disconnected
        import math
        dist_km = math.sqrt((start_lon - end_lon)**2 + (start_lat - end_lat)**2) * 111.0
        if baseline_mins == 0:
            # Assume baseline requires navigating local roads (1.5x distance at 30km/h)
            baseline_mins = round((dist_km * 1.5) / 30.0 * 60.0, 1) * 25
        
        # 3. Inject New Highway
        temp_source = origins[0]
        temp_target = destinations[0]
        
        geojson_str = json.dumps({
            "type": "LineString",
            "coordinates": [[c[1], c[0]] for c in req.coordinates]
        })
        
        # Assign an extremely low cost (e.g., 1 meter) so traffic loves it
        super_cost = 1.0
        
        cursor.execute("""
            INSERT INTO roads (source, target, cost, reverse_cost, fclass, name, geometry)
            VALUES (%s, %s, %s, %s, 'motorway', 'Mega-Project Highway', ST_GeomFromGeoJSON(%s))
        """, (temp_source, temp_target, super_cost, super_cost, geojson_str))
        
        # 4. Post-Project Simulation
        try:
            cursor.execute(f"""
                SELECT AVG(agg_cost) FROM pgr_dijkstraCost(
                    'SELECT id, source, target, cost, reverse_cost FROM roads',
                    ARRAY{origins},
                    ARRAY{destinations},
                    false
                )
            """)
            new_avg = cursor.fetchone()[0] or 0
            new_mins = round(((new_avg / 1000.0) / 40.0 * 60.0) * 25, 1)
            if new_mins == 0:
                raise Exception("Still 0")
        except:
            # Fallback to direct highway speed (100km/h) for 25 pairs
            new_mins = round((dist_km) / 100.0 * 60.0, 1) * 25
            conn.rollback()
        
        # 5. SAFE ROLLBACK
        conn.rollback()
        
        # 6. Calculate Economics
        
        time_saved_mins = max(0, baseline_mins - new_mins)
        daily_commuters = 100000
        hourly_wage = 10.0 # $10/hr
        
        daily_savings = (time_saved_mins / 60.0) * daily_commuters * hourly_wage
        annual_savings = round((daily_savings * 260) / 1000000.0, 2) # in Millions
        
        return {
            "code": "Ok",
            "stats": {
                "baseline_time": baseline_mins,
                "new_time": new_mins,
                "reduction_pct": round((time_saved_mins / baseline_mins) * 100, 1) if baseline_mins > 0 else 0,
                "annual_savings_mil": annual_savings
            }
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"MegaProject Error: {e}")
        return {"code": "Error", "error": str(e)}
    finally:
        if conn:
            conn.autocommit = True
            cursor.close()
            pg_pool.putconn(conn)

@app.get("/api/test_mega")
def test_mega():
    conn = None
    try:
        conn = pg_pool.getconn()
        cursor = conn.cursor()
        # Simulate user drawing a road from Dhaka center to airport
        start_lon, start_lat = 90.4125, 23.8103
        end_lon, end_lat = 90.3995, 23.8428
        
        cursor.execute("""
            SELECT source FROM roads 
            ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 20;
        """, (start_lon, start_lat))
        origins = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT target FROM roads 
            ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 20;
        """, (end_lon, end_lat))
        destinations = [row[0] for row in cursor.fetchall()]
        
        query = f"""
            SELECT COUNT(*), SUM(agg_cost) FROM pgr_dijkstraCost(
                'SELECT id, source, target, cost, reverse_cost FROM roads',
                ARRAY{origins},
                ARRAY{destinations},
                false
            )
        """
        cursor.execute(query)
        count, total_cost = cursor.fetchone()
        
        return {"origins": len(origins), "destinations": len(destinations), "connected_paths": count, "total_cost": total_cost}
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            cursor.close()
            pg_pool.putconn(conn)


# ---------------------------------------------------------
# Cloud DB Transit & Infrastructure Endpoints (from Neon)
# ---------------------------------------------------------
@app.get("/api/train_schedules")
@app.get("/data/all_train_schedules.json")
def get_train_schedules():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT train_slug, schedule FROM train_schedules;")
        rows = cur.fetchall()
        cur.close()
        return {slug: sched for slug, sched in rows}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/station_coords")
@app.get("/data/station_coords.json")
def get_station_coords():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name, lat, lon FROM railway_stations;")
        rows = cur.fetchall()
        cur.close()
        return {name: [lat, lon] for name, lat, lon in rows}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/bus_station_coords")
@app.get("/data/bus_station_coords.json")
def get_bus_station_coords():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name, lat, lon FROM bus_terminals;")
        rows = cur.fetchall()
        cur.close()
        return {name: [lat, lon] for name, lat, lon in rows}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/bus_schedules")
def get_bus_schedules(operator: str = None):
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        if operator:
            clean_op = operator.lower().replace(".", "").replace("_", "").replace(" ", "").replace("-", "")
            cur.execute("SELECT operator, data FROM bus_operator_schedules;")
            for op, data in cur.fetchall():
                if clean_op in op.lower().replace(".", "").replace("_", "").replace(" ", "").replace("-", ""):
                    cur.close()
                    return data
            cur.close()
            return JSONResponse(status_code=404, content={"error": f"Operator '{operator}' not found"})
        cur.execute("SELECT operator, data FROM bus_operator_schedules;")
        rows = cur.fetchall()
        cur.close()
        return {op: data for op, data in rows}
    finally:
        pg_pool.putconn(conn)

@app.get("/data/SI_Enterprise_Bus_Schedules_2026.json")
def get_si_bus_schedules():
    return get_bus_schedules(operator="SI")

@app.get("/data/London_Express_Bus_Schedules_10-08-2026.json")
def get_london_bus_schedules():
    return get_bus_schedules(operator="London")

@app.get("/api/gas_power_plants")
@app.get("/data/gas_power_plants.json")
def get_gas_power_plants():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(geometry)::json,
                        'properties', json_build_object(
                            'name', name,
                            'type', plant_type,
                            'source', source
                        )
                    )
                ), '[]'::json)
            ) FROM gas_power_plants;
        """)
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/ffwc/stations")
@app.get("/data/ffwc/data_load_stations-2025.json")
def get_ffwc_stations():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT raw_data FROM ffwc_stations ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    finally:
        pg_pool.putconn(conn)

@app.get("/api/ffwc/river_attributes")
@app.get("/data/ffwc/river_attriub.json")
def get_ffwc_river_attributes():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(geometry)::json,
                        'properties', json_build_object(
                            'RiverName', river_name
                        )
                    )
                ), '[]'::json)
            ) FROM ffwc_rivers;
        """)
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/ffwc/alerts")
def get_ffwc_alerts():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT raw_data FROM ffwc_alerts ORDER BY alert_date DESC, id ASC;")
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    finally:
        pg_pool.putconn(conn)

@app.get("/api/vessels")
@app.get("/data/marinetraffic_bd.json")
def get_vessels():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(geometry)::json,
                        'properties', json_build_object(
                            'ship_id', ship_id,
                            'ship_name', ship_name,
                            'ship_type', ship_type,
                            'type_name', type_name,
                            'flag', flag,
                            'destination', destination,
                            'status_name', status_name,
                            'speed', speed,
                            'course', course,
                            'heading', heading,
                            'length', length,
                            'width', width,
                            'dwt', dwt
                        )
                    )
                ), '[]'::json)
            ) FROM vessels;
        ''')
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/bus_counters")
@app.get("/data/bus_counters.json")
def get_bus_counters():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT counter_name FROM bus_counters ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    finally:
        pg_pool.putconn(conn)


@app.get("/api/districts")
def get_districts():
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(geometry)::json,
                        'properties', json_build_object(
                            'name', district_name,
                            'division', division_name,
                            'pcode', pcode,
                            'area_sqkm', area_sqkm
                        )
                    )
                ), '[]'::json)
            ) FROM districts;
        ''')
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/places_of_worship")
def get_places_of_worship(xmin: float = None, ymin: float = None, xmax: float = None, ymax: float = None, limit: int = 1000):
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        if xmin is not None and ymin is not None and xmax is not None and ymax is not None:
            cur.execute('''
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(geometry)::json,
                            'properties', json_build_object('fclass', fclass, 'name', name)
                        )
                    ), '[]'::json)
                ) FROM (
                    SELECT * FROM places_of_worship 
                    WHERE geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    LIMIT %s
                ) as sub;
            ''', (xmin, ymin, xmax, ymax, limit))
        else:
            cur.execute('''
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(geometry)::json,
                            'properties', json_build_object('fclass', fclass, 'name', name)
                        )
                    ), '[]'::json)
                ) FROM (SELECT * FROM places_of_worship LIMIT %s) as sub;
            ''', (limit,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

@app.get("/api/celltowers")
def get_celltowers(xmin: float = None, ymin: float = None, xmax: float = None, ymax: float = None, limit: int = 1000):
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        if xmin is not None and ymin is not None and xmax is not None and ymax is not None:
            cur.execute('''
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(geometry)::json,
                            'properties', json_build_object(
                                'radio', radio,
                                'mcc', mcc,
                                'net', net,
                                'area', area,
                                'cell', cell,
                                'range', range,
                                'samples', samples
                            )
                        )
                    ), '[]'::json)
                ) FROM (
                    SELECT * FROM celltowers 
                    WHERE geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    LIMIT %s
                ) as sub;
            ''', (xmin, ymin, xmax, ymax, limit))
        else:
            cur.execute('''
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(geometry)::json,
                            'properties', json_build_object(
                                'radio', radio,
                                'mcc', mcc,
                                'net', net,
                                'area', area,
                                'cell', cell,
                                'range', range,
                                'samples', samples
                            )
                        )
                    ), '[]'::json)
                ) FROM (SELECT * FROM celltowers LIMIT %s) as sub;
            ''', (limit,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
    finally:
        pg_pool.putconn(conn)

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.abspath(os.path.join(_BACKEND_DIR, "..", "data"))
_FRONTEND_DIR = os.path.abspath(os.path.join(_BACKEND_DIR, "..", "frontend"))
_FRONTEND_MAPLIBRE_DIR = os.path.abspath(os.path.join(_BACKEND_DIR, "..", "frontend_maplibre"))

if os.path.exists(_DATA_DIR):
    app.mount("/data", StaticFiles(directory=_DATA_DIR), name="data")
if os.path.exists(_FRONTEND_MAPLIBRE_DIR):
    app.mount("/gl", StaticFiles(directory=_FRONTEND_MAPLIBRE_DIR, html=True), name="maplibre")
if os.path.exists(_FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=_FRONTEND_DIR), name="static")

if __name__ == "__main__":
    import uvicorn
    print("Starting PostgreSQL Map API Server at http://127.0.0.1:8001")
    _dir = os.path.dirname(os.path.abspath(__file__))
    uvicorn.run(app, host="127.0.0.1", port=8001)
