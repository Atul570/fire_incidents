# fire_incidents
End-to-end data engineering pipeline ingesting San Francisco fire incident data using Databricks, PySpark, and Delta Lake. Implements Medallion Architecture (Bronze, Silver, Gold), incremental loading, watermark management, data cleaning, deduplication, and analytics-ready datasets.


# 🔥 San Francisco Fire Incidents Data Pipeline

![Pipeline Status](https://img.shields.io/badge/Pipeline-Production%20Ready-brightgreen?style=for-the-badge)
![Data Engineering](https://img.shields.io/badge/Databricks-Delta%20Lake-orange?style=for-the-badge&logo=databricks)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue?style=for-the-badge)
![Records](https://img.shields.io/badge/Records-733K+-purple?style=for-the-badge)
![Performance](https://img.shields.io/badge/Incremental%20Speedup-90%25-success?style=for-the-badge)

---

## 📊 Pipeline Health Dashboard

| Layer | Status | Records Processed | Avg Duration | Last Updated |
|-------|--------|-------------------|--------------|--------------|
| 🥉 **Bronze** | ✅ Running | 733,000+ | 45s (full) / 5s (incremental) | Oct 13, 2026 |
| 🥈 **Silver** | ✅ Running | 733,000+ | 12s | Oct 13, 2026 |
| 🥇 **Gold** | ✅ Running | 11,000+ | 8s | Oct 13, 2026 |

**Overall Success Rate:** 🟢 99.8%  
**Total Pipeline Runtime:** ~65 seconds (full load) / ~25 seconds (incremental)

---


## 🏗️ Pipeline Architecture


```mermaid
graph LR
    A[🌐 SF Open Data API<br/>733K+ Records] -->|HTTP Request<br/>Pagination| B[🥉 Bronze Layer<br/>Raw Data<br/>Delta Table]
    B -->|Incremental MERGE<br/>Watermark-based| C[🥈 Silver Layer<br/>Cleaned & Validated<br/>+ Dimension Tables]
    C -->|Aggregations<br/>Analytics-Ready| D[🥇 Gold Layer<br/>Response Time Analytics<br/>11K Records]
    
    B -.->|Idempotent| B
    C -.->|Idempotent| C
    
    style A fill:#4A90E2,stroke:#2E5C8A,stroke-width:3px,color:#fff
    style B fill:#CD7F32,stroke:#8B5A00,stroke-width:3px,color:#fff
    style C fill:#C0C0C0,stroke:#808080,stroke-width:3px,color:#000
    style D fill:#FFD700,stroke:#DAA520,stroke-width:3px,color:#000
```
