from services.dashboard_queries import get_kpis


def get_latest_run_summary():
    return {
        "status": "success",
        "run_date": "2026-04-25",
        "files_found": 24,
        "files_processed": 24,
        "inserted_balcao_digital": 0,
        "skipped_balcao_digital": 70080,
        "inserted_balcao_digital_consumo": 0,
        "skipped_balcao_digital_consumo": 0,
        "source": "Google Drive shared folder",
        "notes": "Pipeline completed successfully. All records in this test window were already present in the database."
    }


def get_dashboard_summary():
    kpis = get_kpis()
    latest_run = get_latest_run_summary()

    return {
        "kpis": kpis,
        "latest_run": latest_run
    }