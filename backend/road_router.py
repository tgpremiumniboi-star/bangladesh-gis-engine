import psycopg2
import networkx as nx
import json
from shapely.geometry import Point
from scipy.spatial import cKDTree
import numpy as np
import time

class RoadRouter:
    def __init__(self, db_url):
        self.db_url = db_url
        self.G = None
        self.pts_dict = {}  # id -> (lon, lat)
        self.main_tree = None
        self.main_node_ids = []
        self.initialized = False

    def build_graph(self):
        print("Initializing Road Router (fetching topologies)...")
        start_time = time.time()
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            # Fetch vertices for KDTree
            cur.execute('''
                SELECT id, ST_X(the_geom), ST_Y(the_geom)
                FROM roads_vertices_pgr
            ''')
            vertices = cur.fetchall()
            
            # Fetch roads
            cur.execute('''
                SELECT source, target, cost, ST_AsGeoJSON(ST_Transform(geometry, 4326))
                FROM roads 
                WHERE source IS NOT NULL AND target IS NOT NULL
                AND fclass IN ('primary', 'trunk', 'secondary', 'tertiary', 'motorway', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'unclassified', 'residential')
            ''')
            edges = cur.fetchall()
            
            cur.close()
            conn.close()
        except Exception as e:
            print("Failed to fetch road geometries:", e)
            return

        print(f"Road Router: Fetched {len(vertices)} vertices and {len(edges)} edges in {time.time() - start_time:.1f}s")
        
        # Build node dictionary
        pts_list = []
        for v in vertices:
            self.pts_dict[v[0]] = (v[1], v[2])
            pts_list.append((v[1], v[2]))
            
        pts_arr = np.array(pts_list)
        if len(pts_arr) == 0:
            print("No road points found!")
            return

        print("Road Router: Building Graph...")
        self.G = nx.Graph()
        
        for e in edges:
            source, target, cost, geojson_str = e
            try:
                geom = json.loads(geojson_str)
                coords = []
                if geom['type'] == 'LineString':
                    coords = geom['coordinates']
                elif geom['type'] == 'MultiLineString':
                    coords = geom['coordinates'][0]
                    
                self.G.add_edge(source, target, weight=cost, coords=coords)
            except Exception:
                pass

        # Sort connected components by size (largest first)
        ccs = sorted(list(nx.connected_components(self.G)), key=len, reverse=True)
        print(f'Road Router: Initial components {len(ccs)}')

        # Gap jumping: connect components, but for roads only connect large components to avoid O(N^2) explosion
        if len(ccs) > 1:
            print('Road Router: Adding jump edges between major components...')
            # Keep only components with > 100 nodes, up to max 100 components to prevent explosion
            valid_ccs = [c for c in ccs if len(c) > 100][:100] 
            for i in range(len(valid_ccs)):
                for j in range(i+1, len(valid_ccs)):
                    nodes_i = list(valid_ccs[i])
                    nodes_j = list(valid_ccs[j])
                    
                    coords_i = np.array([self.pts_dict[n] for n in nodes_i])
                    coords_j = np.array([self.pts_dict[n] for n in nodes_j])
                    
                    tree_j = cKDTree(coords_j)
                    dists, idxs = tree_j.query(coords_i, k=1)
                    
                    min_idx_i = np.argmin(dists)
                    min_dist = dists[min_idx_i]
                    
                    # If components are within 20 km (0.2 degrees)
                    if min_dist < 0.2:
                        n_i = nodes_i[min_idx_i]
                        n_j = nodes_j[idxs[min_idx_i]]
                        # Add a jump edge (straight line coordinates)
                        jump_coords = [self.pts_dict[n_i], self.pts_dict[n_j]]
                        self.G.add_edge(n_i, n_j, weight=min_dist * 10.0, coords=jump_coords)
                        
        # Re-compute connected components and build KDTree ONLY for the main component
        # This guarantees that start/end snapping always picks a globally connected node!
        ccs = sorted(list(nx.connected_components(self.G)), key=len, reverse=True)
        main_component_nodes = set(ccs[0])
        print(f"Road Router: Main component has {len(main_component_nodes)} nodes")
        
        main_pts_list = []
        self.main_node_ids = []
        for n in main_component_nodes:
            main_pts_list.append(self.pts_dict[n])
            self.main_node_ids.append(n)
            
        print("Road Router: Building Main KDTree...")
        self.main_tree = cKDTree(np.array(main_pts_list))

        print(f"Road Router initialized with {len(self.G.nodes)} nodes and {len(self.G.edges)} edges in {time.time() - start_time:.1f}s.")
        self.initialized = True

    def route(self, coords_list):
        if not self.initialized or len(coords_list) < 2:
            return None
            
        full_path = []
        for i in range(len(coords_list) - 1):
            start = coords_list[i]
            end = coords_list[i+1]
            
            idx1 = self.main_tree.query((start[1], start[0]))[1]
            idx2 = self.main_tree.query((end[1], end[0]))[1]
            
            n1 = self.main_node_ids[idx1]
            n2 = self.main_node_ids[idx2]
            
            try:
                # Use A* if available or fallback to dijkstra
                path = nx.astar_path(self.G, n1, n2, heuristic=lambda u, v: Point(self.pts_dict[u]).distance(Point(self.pts_dict[v])), weight='weight')
                
                # Append coordinates for this segment
                for j in range(len(path) - 1):
                    u = path[j]
                    v = path[j+1]
                    edge_data = self.G.get_edge_data(u, v)
                    coords = edge_data['coords']
                    
                    if not coords:
                        continue
                        
                    # Ensure coordinates are in correct direction
                    dist_u_first = Point(self.pts_dict[u]).distance(Point(coords[0]))
                    dist_u_last = Point(self.pts_dict[u]).distance(Point(coords[-1]))
                    
                    if dist_u_last < dist_u_first:
                        coords = coords[::-1]
                        
                    full_path.extend(coords)
                    
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                # If path fails despite jump edges, just draw a straight line
                full_path.append([start[1], start[0]])
                full_path.append([end[1], end[0]])
                
        # Deduplicate sequential points
        dedup_path = []
        for p in full_path:
            if not dedup_path or dedup_path[-1] != p:
                dedup_path.append(p)
                
        return dedup_path
