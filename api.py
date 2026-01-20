from flask import Flask, request, jsonify
import sqlite3

DB_PATH = "train_analysis.db"

app = Flask(__name__)

def query_db(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/health")
def health():
    return jsonify({"status": "ok", "db": DB_PATH})

@app.get("/stats")
def stats():
    rows = query_db("""
        SELECT
            COUNT(*) AS n_rows,
            COUNT(DISTINCT train_no) AS n_trains,
            MIN(date_only) AS date_min,
            MAX(date_only) AS date_max
        FROM train_delays
    """)
    return jsonify(rows[0] if rows else {})


@app.get("/top-trains")
def top_trains():
    # /top-trains?n=15&metric=days|avg
    n = request.args.get("n", default=15, type=int)
    metric = request.args.get("metric", default="days", type=str)

    if metric not in ("days", "avg"):
        return jsonify({"error": "metric mora biti 'days' ili 'avg'"}), 400

    order_clause = "days_late DESC, avg_delay_when_late DESC" if metric == "days" else "avg_delay_when_late DESC, days_late DESC"

    rows = query_db(f"""
        SELECT
            train_no,
            SUM(CASE WHEN delay_min > 0 THEN 1 ELSE 0 END) AS days_late,
            AVG(CASE WHEN delay_min > 0 THEN delay_min END) AS avg_delay_when_late
        FROM train_delays
        GROUP BY train_no
        ORDER BY {order_clause}
        LIMIT ?
    """, (n,))
    return jsonify(rows)

@app.get("/destinations")
def destinations():
    # /destinations?n=15
    n = request.args.get("n", default=15, type=int)

    rows = query_db("""
        SELECT
            last_stop,
            SUM(CASE WHEN delay_min > 0 THEN 1 ELSE 0 END) AS delays_count,
            COUNT(DISTINCT train_no) AS unique_trains
        FROM train_delays
        GROUP BY last_stop
        ORDER BY delays_count DESC
        LIMIT ?
    """, (n,))
    return jsonify(rows)

@app.get("/dayparts")
def dayparts():
    """
    /dayparts
    Radi raspodjelu kašnjenja po dobu dana koristeći first_dep (minute od početka dana).
    Ako ti u bazi nema stupca first_dep, ovo će vratiti grešku -> tada treba dodati first_dep u tablicu.
    """
    rows = query_db("""
        SELECT
            CASE
                WHEN (first_dep % 1440) < 360 THEN 'Noć'
                WHEN (first_dep % 1440) < 720 THEN 'Jutro'
                WHEN (first_dep % 1440) < 1080 THEN 'Popodne'
                ELSE 'Večer'
            END AS doba_dana,
            COUNT(*) AS n_rows,
            SUM(CASE WHEN delay_min > 0 THEN 1 ELSE 0 END) AS n_late,
            AVG(CASE WHEN delay_min > 0 THEN delay_min END) AS avg_delay_when_late
        FROM train_delays
        GROUP BY doba_dana
        ORDER BY
            CASE doba_dana
                WHEN 'Noć' THEN 1
                WHEN 'Jutro' THEN 2
                WHEN 'Popodne' THEN 3
                ELSE 4
            END
    """)
    return jsonify(rows)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
