"""
Fabric E-Commerce Lakehouse Analytics Platform — Full-Stack Server & REST API Engine
Serves both the interactive web dashboard and REST API endpoints over DuckDB for Medallion Lakehouse Analytics.
"""

import os
import json
import threading
import duckdb
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 8080
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")

# Shared DuckDB connection & lock for thread safety
db_conn = duckdb.connect(":memory:")
db_lock = threading.Lock()

class LakehouseRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=PUBLIC_DIR, **kwargs)

    def end_headers(self):
        # Disable browser caching for static assets
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200, "OK")
        self.end_headers()

    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query = parse_qs(parsed_path.query)

        if path.startswith("/api/"):
            self.handle_api_get(path, query)
        else:
            super().do_GET()

    def do_POST(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else "{}"
        
        try:
            payload = json.loads(post_data)
        except Exception:
            payload = {}

        if path == "/api/pipeline/run":
            self.handle_pipeline_run(payload)
        elif path == "/api/sql/execute":
            self.handle_sql_execute(payload)
        else:
            self.send_json_response({"error": "Endpoint not found"}, status=404)

    def handle_api_get(self, path, query):
        try:
            with db_lock:
                tables = [t[0] for t in db_conn.execute("SHOW TABLES").fetchall()]
                if "fact_sales" not in tables:
                    from run_local_pipeline_simulation import run_pipeline
                    run_pipeline(db_conn, stage='all')

                if path == "/api/metrics/summary":
                    res = db_conn.execute("""
                        SELECT 
                            ROUND(SUM(sales_amount), 2) AS total_revenue,
                            COUNT(DISTINCT order_id) AS total_orders,
                            ROUND(SUM(sales_amount) / COUNT(DISTINCT order_id), 2) AS average_order_value,
                            ROUND(SUM(profit_amount), 2) AS total_profit,
                            ROUND((SUM(profit_amount) / SUM(sales_amount)) * 100, 2) AS profit_margin_pct
                        FROM fact_sales;
                    """).fetchone()
                    
                    data = {
                        "total_revenue": res[0] if res and res[0] is not None else 0,
                        "total_orders": res[1] if res and res[1] is not None else 0,
                        "average_order_value": res[2] if res and res[2] is not None else 0,
                        "total_profit": res[3] if res and res[3] is not None else 0,
                        "profit_margin_pct": res[4] if res and res[4] is not None else 0
                    }
                    self.send_json_response(data)

                elif path == "/api/metrics/revenue-trend":
                    rows = db_conn.execute("""
                        SELECT 
                            d.year,
                            d.month,
                            d.month_name,
                            ROUND(SUM(f.sales_amount), 2) AS revenue,
                            ROUND(SUM(f.profit_amount), 2) AS profit,
                            COUNT(DISTINCT f.order_id) AS orders
                        FROM fact_sales f
                        JOIN dim_date d ON f.date_key = d.date_key
                        GROUP BY d.year, d.month, d.month_name
                        ORDER BY d.year, d.month;
                    """).fetchall()
                    
                    data = [{"year": r[0], "month": r[1], "month_name": r[2], "revenue": r[3], "profit": r[4], "orders": r[5]} for r in rows]
                    self.send_json_response(data)

                elif path == "/api/metrics/categories":
                    rows = db_conn.execute("""
                        SELECT 
                            p.category_name,
                            SUM(f.quantity) AS total_units,
                            ROUND(SUM(f.sales_amount), 2) AS category_revenue,
                            ROUND(SUM(f.profit_amount), 2) AS category_profit
                        FROM fact_sales f
                        JOIN dim_product p ON f.product_key = p.product_key
                        GROUP BY p.category_name
                        ORDER BY category_revenue DESC;
                    """).fetchall()
                    
                    data = [{"category_name": r[0], "units_sold": r[1], "category_revenue": r[2], "category_profit": r[3]} for r in rows]
                    self.send_json_response(data)

                elif path == "/api/metrics/customer-segments":
                    rows = db_conn.execute("""
                        SELECT 
                            c.customer_segment,
                            COUNT(DISTINCT c.customer_id) AS total_customers,
                            COUNT(DISTINCT f.order_id) AS total_orders,
                            ROUND(SUM(f.sales_amount), 2) AS segment_revenue
                        FROM fact_sales f
                        JOIN dim_customer c ON f.customer_key = c.customer_key
                        WHERE c.is_current = TRUE
                        GROUP BY c.customer_segment
                        ORDER BY segment_revenue DESC;
                    """).fetchall()
                    
                    data = [{"customer_segment": r[0], "total_customers": r[1], "total_orders": r[2], "segment_revenue": r[3]} for r in rows]
                    self.send_json_response(data)

                elif path == "/api/metrics/quality":
                    rows = db_conn.execute("""
                        SELECT table_name, check_name, category, severity, total_records, failed_records, success_percentage, status
                        FROM data_quality_results;
                    """).fetchall()
                    
                    data = [{
                        "table_name": r[0],
                        "check_name": r[1],
                        "category": r[2],
                        "severity": r[3],
                        "total_records": r[4],
                        "failed_records": r[5],
                        "success_percentage": r[6],
                        "status": r[7]
                    } for r in rows]
                    self.send_json_response(data)

                elif path == "/api/governance/catalog":
                    rows = db_conn.execute("""
                        SELECT table_name, layer, column_name, data_type, pii_classification, quality_rules_applied, retention_days, owner
                        FROM data_governance_catalog;
                    """).fetchall()
                    
                    data = [{
                        "table_name": r[0],
                        "layer": r[1],
                        "column_name": r[2],
                        "data_type": r[3],
                        "pii_classification": r[4],
                        "quality_rules_applied": r[5],
                        "retention_days": r[6],
                        "owner": r[7]
                    } for r in rows]
                    self.send_json_response(data)

                elif path == "/api/tables/data":
                    table_name = query.get("name", ["silver_sales"])[0]
                    limit = int(query.get("limit", ["30"])[0])
                    
                    allowed_tables = [
                        "bronze_customers", "bronze_products", "bronze_orders", 
                        "silver_customers", "silver_products", "silver_orders", "silver_order_items", "silver_sales", 
                        "fact_sales", "fact_returns", "dim_customer", "dim_product", "dim_date", 
                        "data_quality_results", "data_governance_catalog"
                    ]
                    if table_name not in allowed_tables:
                        table_name = "silver_sales"
                        
                    cur = db_conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
                    cols = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    
                    formatted_rows = []
                    for row in rows:
                        formatted_rows.append([str(v) if v is not None else None for v in row])
                        
                    self.send_json_response({"table": table_name, "columns": cols, "rows": formatted_rows})

                else:
                    self.send_json_response({"error": "Unknown API endpoint"}, status=404)

        except Exception as e:
            self.send_json_response({"error": str(e)}, status=500)

    def handle_pipeline_run(self, payload):
        stage = payload.get("stage", "all")
        try:
            with db_lock:
                from run_local_pipeline_simulation import run_pipeline
                res = run_pipeline(db_conn, stage=stage)
            self.send_json_response({
                "status": "SUCCESS", 
                "stage": stage, 
                "logs": res["logs"], 
                "message": f"Medallion Pipeline stage '{stage.upper()}' executed successfully."
            })
        except Exception as e:
            self.send_json_response({"error": str(e)}, status=500)

    def handle_sql_execute(self, payload):
        sql_query = payload.get("query", "")
        if not sql_query or not sql_query.strip():
            self.send_json_response({"error": "Empty query provided"}, status=400)
            return

        try:
            with db_lock:
                cur = db_conn.execute(sql_query)
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall() if cur.description else []
                
                formatted_rows = []
                for row in rows:
                    formatted_rows.append([str(v) if v is not None else None for v in row])

            self.send_json_response({"columns": cols, "rows": formatted_rows})
        except Exception as e:
            self.send_json_response({"error": str(e)}, status=400)

    def send_json_response(self, data, status=200):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main():
    print("=" * 80)
    print("INITIALIZING DUCKDB LAKEHOUSE ENGINE & RUNNING MEDALLION PIPELINE")
    print("=" * 80)
    
    from run_local_pipeline_simulation import run_pipeline
    with db_lock:
        run_pipeline(db_conn, stage='all')
    
    print("\n" + "=" * 80)
    print(f"FABRIC LAKEHOUSE ANALYTICS PLATFORM WEB SERVER LIVE ON PORT {PORT}")
    print("=" * 80)
    print(f" -> Open Interactive Web Dashboard: http://localhost:{PORT}")
    print("Press Ctrl+C to stop server.\n")
    
    server = HTTPServer(('localhost', PORT), LakehouseRequestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER STOPPED]")

if __name__ == "__main__":
    main()
