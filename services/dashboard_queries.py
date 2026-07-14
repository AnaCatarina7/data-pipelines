import os
import json
from crate import client


def detect_environment():
    if "COLAB_RELEASE_TAG" in os.environ:
        return "colab"
    if "RENDER" in os.environ:
        return "render"
    return "local"


def get_secret_json(name):
    env = detect_environment()

    if env == "render":
        with open(f"/etc/secrets/{name}", "r", encoding="utf-8") as f:
            return json.load(f)

    with open(f"secrets/{name}", "r", encoding="utf-8") as f:
        return json.load(f)


def get_connection():
    dbcreds = get_secret_json("ACTC-crate.json")
    return client.connect(
        dbcreds["dest_host"],
        username=dbcreds["username"],
        password=dbcreds["password"],
        verify_ssl_cert=True
    )


def fetch_one(cursor, query, args=None):
    cursor.execute(query, args or [])
    return cursor.fetchone()


def fetch_all(cursor, query, args=None):
    cursor.execute(query, args or [])
    return cursor.fetchall()


def get_kpis():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        total_a = fetch_one(cursor, "SELECT COUNT(*) FROM balcao_digital")[0]
        total_b = fetch_one(cursor, "SELECT COUNT(*) FROM balcao_digital_consumo")[0]
        distinct_cpes = fetch_one(cursor, "SELECT COUNT(DISTINCT cpe) FROM balcao_digital")[0]
        last_entry = fetch_one(cursor, "SELECT MAX(tstamp) FROM balcao_digital")[0]
        first_entry = fetch_one(cursor, "SELECT MIN(tstamp) FROM balcao_digital")[0]

        return {
            "total_balcao_digital": total_a or 0,
            "total_balcao_digital_consumo": total_b or 0,
            "distinct_cpes": distinct_cpes or 0,
            "last_entry": str(last_entry) if last_entry else None,
            "first_entry": str(first_entry) if first_entry else None,
        }
    finally:
        connection.close()


def get_latest_run_summary_real():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        max_a = fetch_one(cursor, "SELECT MAX(tstamp) FROM balcao_digital")[0]
        max_b = fetch_one(cursor, "SELECT MAX(tstamp) FROM balcao_digital_consumo")[0]

        latest_tstamp = None
        if max_a and max_b:
            latest_tstamp = max(max_a, max_b)
        else:
            latest_tstamp = max_a or max_b

        count_a_latest_day = 0
        count_b_latest_day = 0
        distinct_cpes_latest_day = 0
        latest_entry_a = str(max_a) if max_a else None
        latest_entry_b = str(max_b) if max_b else None

        if latest_tstamp:
            latest_day = str(latest_tstamp)[:10]

            row_a = fetch_one(cursor, """
                SELECT COUNT(*)
                FROM balcao_digital
                WHERE DATE_FORMAT('%Y-%m-%d', tstamp) = ?
            """, [latest_day])
            count_a_latest_day = row_a[0] if row_a and row_a[0] is not None else 0

            row_b = fetch_one(cursor, """
                SELECT COUNT(*)
                FROM balcao_digital_consumo
                WHERE DATE_FORMAT('%Y-%m-%d', tstamp) = ?
            """, [latest_day])
            count_b_latest_day = row_b[0] if row_b and row_b[0] is not None else 0

            row_cpes = fetch_one(cursor, """
                SELECT COUNT(DISTINCT cpe)
                FROM balcao_digital
                WHERE DATE_FORMAT('%Y-%m-%d', tstamp) = ?
            """, [latest_day])
            distinct_cpes_latest_day = row_cpes[0] if row_cpes and row_cpes[0] is not None else 0

            return {
                "status": "success",
                "run_date": latest_day,
                "latest_entry_balcao_digital": latest_entry_a,
                "latest_entry_balcao_digital_consumo": latest_entry_b,
                "type_a_rows_latest_day": count_a_latest_day,
                "type_b_rows_latest_day": count_b_latest_day,
                "distinct_cpes_latest_day": distinct_cpes_latest_day,
                "source": "CrateDB",
                "notes": f"Live summary based on the latest day found in CrateDB ({latest_day}).",
            }

        return {
            "status": "warn",
            "run_date": None,
            "latest_entry_balcao_digital": None,
            "latest_entry_balcao_digital_consumo": None,
            "type_a_rows_latest_day": 0,
            "type_b_rows_latest_day": 0,
            "distinct_cpes_latest_day": 0,
            "source": "CrateDB",
            "notes": "No records found in CrateDB yet.",
        }

    finally:
        connection.close()


def get_database_status():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        total_a = fetch_one(cursor, "SELECT COUNT(*) FROM balcao_digital")[0] or 0
        total_b = fetch_one(cursor, "SELECT COUNT(*) FROM balcao_digital_consumo")[0] or 0

        return [
            {"table": "balcao_digital", "label": "Power data (Type A)", "active": total_a > 0},
            {"table": "balcao_digital_consumo", "label": "Consumption data (Type B)", "active": total_b > 0},
        ]
    finally:
        connection.close()


def get_cpe_summary():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        rows = fetch_all(cursor, """
            SELECT
                cpe,
                COUNT(*) AS total_rows,
                MIN(tstamp) AS first_tstamp,
                MAX(tstamp) AS last_tstamp,
                AVG(potencia_ativa) AS avg_potencia_ativa,
                MAX(potencia_ativa) AS max_potencia_ativa
            FROM balcao_digital
            GROUP BY cpe
            ORDER BY cpe
        """)

        result = []
        for row in rows:
            result.append({
                "cpe": row[0],
                "total_rows": row[1],
                "first_tstamp": str(row[2]) if row[2] else None,
                "last_tstamp": str(row[3]) if row[3] else None,
                "avg_potencia_ativa": round(float(row[4]), 3) if row[4] is not None else None,
                "max_potencia_ativa": round(float(row[5]), 3) if row[5] is not None else None,
            })
        return result
    finally:
        connection.close()


def get_daily_evolution(selected_cpe=None):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        if not selected_cpe:
            first_cpe_row = fetch_one(cursor, "SELECT cpe FROM balcao_digital LIMIT 1")
            selected_cpe = first_cpe_row[0] if first_cpe_row else None

        if not selected_cpe:
            return []

        rows = fetch_all(cursor, """
            SELECT
                cpe,
                DATE_FORMAT('%Y-%m-%d', tstamp) AS day_label,
                AVG(potencia_ativa) AS avg_potencia
            FROM balcao_digital
            WHERE potencia_ativa IS NOT NULL AND cpe = ?
            GROUP BY cpe, DATE_FORMAT('%Y-%m-%d', tstamp)
            ORDER BY day_label
        """, [selected_cpe])

        result = []
        for row in rows:
            result.append({
                "cpe": row[0],
                "day": row[1],
                "avg_potencia": round(float(row[2]), 3) if row[2] is not None else None
            })
        return result
    finally:
        connection.close()


def get_weekday_weekend_profile(selected_cpe=None):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        if not selected_cpe:
            first_cpe_row = fetch_one(cursor, "SELECT cpe FROM balcao_digital LIMIT 1")
            selected_cpe = first_cpe_row[0] if first_cpe_row else None

        if not selected_cpe:
            return []

        rows = fetch_all(cursor, """
            SELECT
                cpe,
                CASE
                    WHEN DAY_OF_WEEK(tstamp) IN (1, 7) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type,
                DATE_FORMAT('%H', tstamp) AS hour_of_day,
                AVG(potencia_ativa) AS avg_potencia
            FROM balcao_digital
            WHERE potencia_ativa IS NOT NULL AND cpe = ?
            GROUP BY
                cpe,
                CASE
                    WHEN DAY_OF_WEEK(tstamp) IN (1, 7) THEN 'Weekend'
                    ELSE 'Weekday'
                END,
                DATE_FORMAT('%H', tstamp)
            ORDER BY day_type, hour_of_day
        """, [selected_cpe])

        result = []
        for row in rows:
            result.append({
                "cpe": row[0],
                "day_type": row[1],
                "hour": row[2],
                "avg_potencia": round(float(row[3]), 3) if row[3] is not None else None,
            })
        return result
    finally:
        connection.close()


def get_histogram_points():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        rows = fetch_all(cursor, """
            SELECT
                cpe,
                CASE
                    WHEN DAY_OF_WEEK(tstamp) IN (1, 7) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type,
                potencia_ativa
            FROM balcao_digital
            WHERE potencia_ativa IS NOT NULL
            ORDER BY cpe
        """)

        result = []
        for row in rows:
            result.append({
                "cpe": row[0],
                "day_type": row[1],
                "potencia_ativa": float(row[2]) if row[2] is not None else None,
            })
        return result
    finally:
        connection.close()