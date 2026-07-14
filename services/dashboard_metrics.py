from services.dashboard_queries import get_kpis, get_latest_run_summary_real


def get_latest_run_summary():
    return get_latest_run_summary_real()


def get_dashboard_summary():
    kpis = get_kpis()
    latest_run = get_latest_run_summary()

    return {
        "kpis": kpis,
        "latest_run": latest_run
    }