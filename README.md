# 🚕 NY Taxi Data Pipeline (Dockerized)

End-to-end local data engineering workflow using:

- PostgreSQL 18
- pgAdmin 4
- Docker Compose
- Python ingestion pipeline
- uv (Python dependency manager)

This project reproduces the core ingestion workflow from the Data Engineering Zoomcamp using a fully containerized environment.

---

## 🏗 Architecture

Services managed via Docker Compose:

- **pgdatabase** → PostgreSQL 18
- **pgadmin** → Web-based database UI
- **pipeline scripts** → Python ingestion logic

Both services run on a shared Docker network and persist data using named volumes.

---

## 🚀 Quick Start

### 1️⃣ Clone the repository

```bash
git clone <your-repo-url>
cd pipeline
