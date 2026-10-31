import psycopg2
import networkx as nx
import json
from shapely.geometry import Point
from scipy.spatial import cKDTree
import numpy as np

class RailwayRouter:
    def __init__(self, db_url):
        self.db_url = db_url
        self.G = None
        self.node_map = {}
        self.pts = []
        self.tree = None
        self.initialized = False

    def build_graph(self):
        print("Initializing Railway Router (fetching geometries)...")
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute('''
                SELECT ST_AsGeoJSON(ST_Transform(geometry, 4326))
                FROM railways 
                WHERE geometry IS NOT NULL
            ''')
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            print("Failed to fetch railway geometries:", e)
            return

        self.pts = []
        edges = []

        for r in rows:
            try:
                geom = json.loads(r[0])
                lines = []
                if geom['type'] == 'LineString':
                    lines = [geom['coordinates']]
                elif geom['type'] == 'MultiLineString':
                    lines = geom['coordinates']
                    
                for line in lines:
                    if not isinstance(line, list) or len(line) < 2: continue
                    if not isinstance(line[0], list): continue
                        
                    for i in range(len(line) - 1):
                        self.pts.append(tuple(line[i]))
                        self.pts.append(tuple(line[i+1]))
                        edges.append((len(self.pts)-2, len(self.pts)-1))
            except Exception as e:
                pass

        pts_arr = np.array(self.pts)
        if len(pts_arr) == 0:
            print("No railway points found!")
            return

        self.tree = cKDTree(pts_arr)
        # Group points within ~50 meters (approx 0.0005 degrees)
        clusters = self.tree.query_ball_point(pts_arr, r=0.0005)

        self.G = nx.Graph()
        self.node_map = {}
        for i, neighbors in enumerate(clusters):
            self.node_map[i] = min(neighbors)

        for e in edges:
            n1 = self.node_map[e[0]]
            n2 = self.node_map[e[1]]
            if n1 != n2:
                dist = Point(self.pts[n1]).distance(Point(self.pts[n2]))
                self.G.add_edge(n1, n2, weight=dist)

        ccs = list(nx.connected_components(self.G))
        print(f'Railway Router: Initial components {len(ccs)}')

        # Gap jumping: connect components
        if len(ccs) > 1:
            print('Railway Router: Adding jump edges...')
            for i in range(len(ccs)):
                for j in range(i+1, len(ccs)):
                    nodes_i = list(ccs[i])
                    nodes_j = list(ccs[j])
                    
                    coords_i = np.array([self.pts[n] for n in nodes_i])
                    coords_j = np.array([self.pts[n] for n in nodes_j])
                    
                    tree_j = cKDTree(coords_j)
                    dists, idxs = tree_j.query(coords_i, k=1)
                    
                    min_idx_i = np.argmin(dists)
                    min_dist = dists[min_idx_i]
                    
                    # If components are within 10 km (0.1 degrees)
                    if min_dist < 0.1:
                        n_i = nodes_i[min_idx_i]
                        n_j = nodes_j[idxs[min_idx_i]]
                        # penalize jump edges heavily
                        self.G.add_edge(n_i, n_j, weight=min_dist * 10.0)
                        
        print(f"Railway Router initialized with {len(self.G.nodes)} nodes and {len(self.G.edges)} edges.")
        self.initialized = True

    def route(self, coords_list):
        if not self.initialized or len(coords_list) < 2:
            return None
            
        full_path = []
        for i in range(len(coords_list) - 1):
            start = coords_list[i]
            end = coords_list[i+1]
            
            idx1 = self.tree.query((start[1], start[0]))[1]
            idx2 = self.tree.query((end[1], end[0]))[1]
            
            n1 = self.node_map[idx1]
            n2 = self.node_map[idx2]
            
            try:
                path = nx.shortest_path(self.G, n1, n2, weight='weight')
                # Append coordinates for this segment
                for node_id in path:
                    full_path.append(list(self.pts[node_id]))
            except nx.NetworkXNoPath:
                # If path fails despite jump edges, just draw a straight line
                full_path.append([start[1], start[0]])
                full_path.append([end[1], end[0]])
                
        # Deduplicate sequential points
        dedup_path = []
        for p in full_path:
            if not dedup_path or dedup_path[-1] != p:
                dedup_path.append(p)
                
        return dedup_path
