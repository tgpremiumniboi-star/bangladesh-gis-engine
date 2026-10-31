# 🚀 Bangladesh 3D GIS & Vector Tile Engine — Render Deployment

This folder contains the complete, production-ready backend and WebGL frontend configured for 24/7 deployment on **Render** (powered by your Neon PostgreSQL cluster).

---

## 📁 What Is in This Deployment Package

- `backend/` — FastAPI geospatial server with PostGIS spatial sharding and dynamic vector tile router.
- `frontend_maplibre/` — Hardware-accelerated 3D WebGL engine (MapLibre GL JS) running at 60 FPS.
- `frontend/` — Legacy 2D Leaflet mapping engine.
- `data/` — Active runtime feeds (train schedules, luxury bus tracking, FFWC river gauge monitoring).
- `render.yaml` — 1-Click Render Blueprint configuration.
- `Dockerfile` — Optional container deployment configuration.
- `requirements.txt` — Python production dependencies.

---

## ⚡ Deployment in 3 Easy Steps

### Option A: Via GitHub (Recommended — Takes 2 Minutes)

1. **Initialize Git inside this folder and push to GitHub**:
   ```bash
   cd render_deploy
   git init
   git add .
   git commit -m "Deploy Bangladesh 3D GIS to Render"
   # Create a new repository on github.com, then:
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
   git branch -M main
   git push -u origin main
   ```

2. **Connect to Render**:
   - Go to [dashboard.render.com](https://dashboard.render.com).
   - Click **New +** -> **Web Service** -> Connect your GitHub repository.
   - Choose:
     - **Name**: `bangladesh-gis-engine`
     - **Region**: `Singapore` (Closest to Bangladesh for <30 ms latency).
     - **Environment**: `Python`
     - **Build Command**: `pip install -r requirements.txt`
     - **Start Command**: `uvicorn backend.api_pg:app --host 0.0.0.0 --port $PORT --workers 2`
     - **Instance Type**: `Starter` ($7/month — covered by your $50 credit).

3. **Set Environment Variables in Render**:
   In the Render dashboard under **Environment**, add:
   - `DATABASE_URL`: `postgresql://neondb_owner:<YOUR_PASSWORD>@ep-late-dawn-aox69qka-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require`
   *(Optional: Add `OPENSKY_USERNAME`, `AISSTREAM_API_KEY`, etc. if using live flight/vessel telemetry).*

---

### Option B: Via Render Blueprint (1-Click)

1. Push this folder to GitHub.
2. In Render, click **New +** -> **Blueprint**.
3. Select your repository. Render will automatically read `render.yaml` and configure the entire service.

---

## 🌐 Connecting Cloudflare (Optional for <5 ms Tiles)

Once Render gives you your live URL (e.g. `https://bangladesh-gis-engine.onrender.com`):
1. In Cloudflare DNS, add a `CNAME` record:
   - **Name**: `map`
   - **Target**: `bangladesh-gis-engine.onrender.com`
   - **Proxy**: `🟠 Proxied`
2. In Render dashboard, go to **Settings** -> **Custom Domains** -> Add `map.yourdomain.com`.
3. Now all tiles are automatically cached at Cloudflare's **Dhaka Edge node** for <5 ms loading!

---

## 🔗 Live Endpoints Once Deployed

- **3D WebGL Engine (60 FPS)**: `https://YOUR_APP.onrender.com/gl/`
- **2D Leaflet Engine**: `https://YOUR_APP.onrender.com/`
- **Vector Tile Endpoint**: `https://YOUR_APP.onrender.com/api/mvt/{layer}/{z}/{x}/{y}.pbf`
- **Interactive Simulations**: Dengue SIR, Flood Inundation, 999 Dispatch, Train & Bus Trackers.
