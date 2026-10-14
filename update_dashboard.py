import os
import sys
from databricks import sql
from datetime import datetime
import pytz

def safe_query(cursor, query, default=None):
    """Execute query with error handling"""
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return result if result else default
    except Exception as e:
        print(f"⚠️ Query failed: {str(e)[:100]}")
        return default

def safe_query_all(cursor, query, default=None):
    """Execute query returning all rows with error handling"""
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result if result else (default or [])
    except Exception as e:
        print(f"⚠️ Query failed: {str(e)[:100]}")
        return default or []

try:
    print("🔌 Connecting to Databricks...")
    conn = sql.connect(
        server_hostname=os.environ['DATABRICKS_HOST'],
        http_path=os.environ['DATABRICKS_HTTP_PATH'],
        access_token=os.environ['DATABRICKS_TOKEN']
    )
    cursor = conn.cursor()
    print("✅ Connected!")

    # Query metrics
    print("📊 Querying data...")
    
    bronze_result = safe_query(cursor, 
        "SELECT COUNT(*), MAX(ingestion_datetime) FROM fire_incident.bronze.fire_incidents", (0, None))
    bronze_count = bronze_result[0] if bronze_result else 0
    bronze_last_updated = str(bronze_result[1]) if bronze_result and bronze_result[1] else "Unknown"

    new_today = safe_query(cursor,
        "SELECT COUNT(*) FROM fire_incident.bronze.fire_incidents WHERE DATE(ingestion_datetime) = CURRENT_DATE()", (0,))
    new_today = new_today[0] if new_today else 0

    silver_count = safe_query(cursor, "SELECT COUNT(*) FROM fire_incident.silver.fire_incidents", (0,))
    silver_count = silver_count[0] if silver_count else 0

    gold_count = safe_query(cursor, "SELECT COUNT(*) FROM fire_incident.gold.response_time_by_neighborhood", (0,))
    gold_count = gold_count[0] if gold_count else 0

    avg_response = safe_query(cursor, "SELECT AVG(average_response_time) FROM fire_incident.gold.response_time_by_neighborhood", (0,))
    avg_response_time = round(float(avg_response[0]), 2) if avg_response and avg_response[0] else 0

    top_neighborhoods = safe_query_all(cursor,
        "SELECT neighborhood_district, total_record, average_response_time FROM fire_incident.gold.response_time_by_neighborhood ORDER BY total_record DESC LIMIT 5", [])

    missing_coords = safe_query(cursor, "SELECT COUNT(*) FROM fire_incident.silver.fire_incidents WHERE latitude IS NULL OR longitude IS NULL", (0,))
    missing_coords = missing_coords[0] if missing_coords else 0

    total_silver = safe_query(cursor, "SELECT COUNT(*) FROM fire_incident.silver.fire_incidents", (1,))
    total_silver = total_silver[0] if total_silver else 1

    coord_quality = round((1 - missing_coords / total_silver) * 100, 1) if total_silver > 0 else 100

    cursor.close()
    conn.close()
    print("✅ Queries complete!")

    # Calculate freshness
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)

    try:
        if bronze_last_updated != "Unknown":
            last_update_time = datetime.fromisoformat(bronze_last_updated.replace('Z', '+00:00')).astimezone(ist)
            time_diff = current_time - last_update_time
            
            if time_diff.total_seconds() < 3600:
                freshness = f"{int(time_diff.total_seconds() / 60)} min ago"
            elif time_diff.total_seconds() < 86400:
                freshness = f"{int(time_diff.total_seconds() / 3600)} hrs ago"
            else:
                freshness = f"{int(time_diff.total_seconds() / 86400)} days ago"
        else:
            freshness = "Unknown"
    except:
        freshness = "Unknown"

    current_time_str = current_time.strftime('%b %d, %Y %I:%M %p IST')

    # Build top neighborhoods
    top_table = ""
    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    
    if top_neighborhoods and len(top_neighborhoods) > 0:
        for idx, row in enumerate(top_neighborhoods[:5]):
            try:
                neighborhood = str(row[0])
                incidents = int(row[1])
                response = round(float(row[2]), 1)
                top_table += f"| {emojis[idx]} | {neighborhood} | {incidents:,} | {response} min |\n"
            except:
                continue
    
    if not top_table:
        top_table = "| - | No data | - | - |\n"

    # Create dashboard
    dashboard = f"""## 📊 Pipeline Health Dashboard

**Last Updated:** {current_time_str}

| Metric | Value |
|--------|-------|
| 🔥 Total Records | {bronze_count:,} |
| ➕ New Today | {new_today:,} |
| 📅 Data Freshness | {freshness} |
| 🏙️ Neighborhoods | {gold_count} |
| ⏱️ Avg Response | {avg_response_time} min |
| 📍 Coord Quality | {coord_quality}% |

### Layer Status

| Layer | Records | Status |
|-------|---------|--------|
| 🥉 Bronze | {bronze_count:,} | ✅ Running |
| 🥈 Silver | {silver_count:,} | ✅ Running |
| 🥇 Gold | {gold_count} | ✅ Running |

### Top 5 Neighborhoods

| Rank | Neighborhood | Incidents | Avg Response |
|------|--------------|-----------|--------------|
{top_table}
---
"""

    # Update README
    print("📝 Updating README...")
    with open('README.md', 'r', encoding='utf-8') as f:
        readme = f.read()

    start = "## 📊 Pipeline Health Dashboard"
    end = "---\n\n## 🏗️"
    
    start_idx = readme.find(start)
    end_idx = readme.find(end, start_idx) if start_idx != -1 else -1
    
    if start_idx != -1 and end_idx != -1:
        readme = readme[:start_idx] + dashboard + "\n" + readme[end_idx:]
        
        with open('README.md', 'w', encoding='utf-8') as f:
            f.write(readme)
        
        print("✅ README updated!")
        print(f"\n📊 Bronze: {bronze_count:,} | Silver: {silver_count:,} | Gold: {gold_count}")
        print(f"➕ New today: {new_today:,} | Freshness: {freshness}")
    else:
        print(f"❌ Markers not found! Start: {start_idx}, End: {end_idx}")
        sys.exit(1)

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
