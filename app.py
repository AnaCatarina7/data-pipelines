from importlib.resources import files
import os
import sys
import json
import threading
import queue
import requests
from datetime import datetime
from flask import Flask, render_template, request, jsonify, stream_with_context, Response, send_file

from io import BytesIO
from matplotlib.figure import Figure
import seaborn as sns

from services.dashboard_queries import (
    get_kpis,
    get_database_status,
    get_cpe_summary,
    get_daily_evolution,
    get_weekday_weekend_profile,
    get_histogram_points,
)
from services.dashboard_metrics import get_latest_run_summary
from services.file_normalization import normalize_files_metadata


app = Flask(__name__)


# ── State ─────────────────────────────────────────────────────────────────────
log_queue = queue.Queue()
pipeline_running = False


# ── Stdout capture ────────────────────────────────────────────────────────────
class QueueLogger:
    """Redirects stdout to the log queue so every print() reaches the browser."""

    def __init__(self, q):
        self.queue = q
        self.terminal = sys.__stdout__

    def write(self, message):
        if message.strip():
            self.terminal.write(message)
            self.queue.put(message)

    def flush(self):
        self.terminal.flush()


# ── Pipeline runner ───────────────────────────────────────────────────────────
def run_pipeline(start_date: str, days_back: int, source: str):
    """Loads and executes the correct pipeline script in a background thread."""
    global pipeline_running
    pipeline_running = True
    sys.stdout = QueueLogger(log_queue)

    try:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] A iniciar pipeline ({source})...\n")
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] start_date={start_date} | days_back={days_back}\n")

        os.environ["START_DATE"] = start_date
        os.environ["DAYS_BACK"] = str(days_back)
        os.environ["SOURCE"] = source

        import importlib.util
        import pathlib

        if source == "fronius":
            script_name = "ACTC-Fronius2.py"
        else:
            script_name = "ACTC-BalcaoDigital.py"

            if source == "shared_drive":
                folder_id = os.getenv("BALCAO_SHARED_FOLDER_ID")
                os.environ["BALCAO_FOLDER_ID"] = folder_id or ""

            elif source == "drive_own":
                folder_id = os.getenv("BALCAO_OWN_FOLDER_ID")
                os.environ["BALCAO_FOLDER_ID"] = folder_id or ""

            else:
                os.environ["BALCAO_FOLDER_ID"] = ""

        script_path = pathlib.Path(__file__).parent / "scripts" / script_name

        if script_path.exists():
            spec = importlib.util.spec_from_file_location("pipeline", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, "run_pipeline"):
                module.run_pipeline({
                    "start_date": start_date,
                    "days_back": days_back,
                    "source": source
                })
                log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Pipeline concluído com sucesso!\n")
            else:
                log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ O script não tem função run_pipeline().\n")
        else:
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Script not found at: {script_path}\n")

    except Exception as e:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {e}\n")

    finally:
        log_queue.put("__DONE__")
        sys.stdout = sys.__stdout__
        pipeline_running = False


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run():
    global pipeline_running

    if pipeline_running:
        return jsonify({"error": "Pipeline já está em execução."}), 409

    data = request.get_json()
    start_date = data.get("start_date", datetime.today().strftime("%Y-%m-%d"))
    days_back = int(data.get("days_back", 7))
    source = data.get("source", "fronius")

    while not log_queue.empty():
        log_queue.get_nowait()

    thread = threading.Thread(
        target=run_pipeline,
        args=(start_date, days_back, source),
        daemon=True
    )
    thread.start()

    return jsonify({"status": "started"})


@app.route("/stream")
def stream():
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=25)

                if msg == "__DONE__":
                    yield "data: __DONE__\n\n"
                    break

                yield f"data: {msg.rstrip()}\n\n"

            except queue.Empty:
                yield ": keepalive\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/status")
def status():
    return jsonify({"running": pipeline_running})


@app.route("/files")
def list_files():
    source = request.args.get("source", "fronius")

    if source == "fronius":
        token = os.getenv("GITHUB_TOKEN")
        url = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/Fronius"
        headers = {"Authorization": f"token {token}"} if token else {}

        r = requests.get(url, headers=headers)
        files = [f["name"] for f in r.json() if isinstance(f, dict) and "name" in f]

        return jsonify({"files": files})

    elif source in ("shared_drive", "drive_own"):
        folder_id = (
            os.getenv("BALCAO_SHARED_FOLDER_ID")
            if source == "shared_drive"
            else os.getenv("BALCAO_OWN_FOLDER_ID")
        )

        if not folder_id:
            return jsonify({"files": [], "error": "Folder ID não definido."}), 500

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            raw = os.getenv("ACTC_DRIVE_CREDENTIALS")
            if raw:
                creds_raw = json.loads(raw)
            else:
                secret_paths = [
                    "/etc/secrets/ACTC-DriveCredentials.json",
                    "secrets/ACTC-DriveCredentials.json",
                    "ACTC-DriveCredentials.json",
                ]

                creds_raw = None
                for path in secret_paths:
                    if os.path.exists(path):
                        with open(path) as f:
                            creds_raw = json.load(f)
                        break

                if creds_raw is None:
                    raise FileNotFoundError("ACTC-DriveCredentials.json não encontrado")

            credentials = service_account.Credentials.from_service_account_info(
                creds_raw,
                scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )

            drive = build("drive", "v3", credentials=credentials)
            response = drive.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="files(id, name, size, modifiedTime)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            drive_files = response.get("files", [])
            files = []

            for f in drive_files:
                name = f.get("name")
                if not name:
                    continue

                # Normalize only the essential file metadata for display.
                meta = normalize_files_metadata([name])[0]
                files.append(str(meta.get("display_title", name)))

            return jsonify({"files": files})

        except Exception as e:
            return jsonify({"files": [], "error": str(e)})

    return jsonify({"files": []})


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.route("/dashboard")
def dashboard():
    """Serves the analytics dashboard with KPIs and chart data."""
    try:
        kpis = get_kpis()
    except Exception:
        kpis = {}

    try:
        latest_run = get_latest_run_summary()
    except Exception:
        latest_run = {}

    try:
        database_status = get_database_status()
    except Exception:
        database_status = []

    try:
        cpe_summary = get_cpe_summary()
    except Exception:
        cpe_summary = []

    try:
        daily_evolution = get_daily_evolution()
    except Exception:
        daily_evolution = []

    try:
        weekday_weekend = get_weekday_weekend_profile()
    except Exception:
        weekday_weekend = []

    return render_template(
        "dashboard.html",
        kpis=kpis,
        latest_run=latest_run,
        database_status=database_status,
        cpe_summary=cpe_summary,
        daily_evolution=daily_evolution,
        weekday_weekend=weekday_weekend,
    )


@app.route("/dashboard/histogram.png")
def histogram_png():
    """
    Generates the hourly power distribution histogram with Seaborn/Matplotlib
    and serves it as a PNG image embedded in the dashboard.
    """
    try:
        points = get_histogram_points()
        values = [p["potencia_ativa"] for p in points if p.get("potencia_ativa") is not None]

        fig = Figure(figsize=(8, 4), dpi=110)
        fig.patch.set_facecolor("#1e293b")
        ax = fig.add_subplot(1, 1, 1)
        ax.set_facecolor("#0f172a")

        if values:
            sns.histplot(
                values,
                bins=40,
                kde=True,
                ax=ax,
                color="#4f98a3",
                edgecolor="#0f172a",
                linewidth=0.4,
                alpha=0.85,
                line_kws={"color": "#81d4da", "linewidth": 2},
            )

        # Chart styling
        ax.set_title("Distribuição Horária de Potência Ativa", color="#cdccca", fontsize=11, pad=12)
        ax.set_xlabel("Potência Ativa (kW)", color="#797876", fontsize=9)
        ax.set_ylabel("Frequência", color="#797876", fontsize=9)
        ax.tick_params(colors="#797876", labelsize=8)

        for spine in ax.spines.values():
            spine.set_edgecolor("#393836")

        ax.grid(axis="y", color="#262523", linewidth=0.6, linestyle="--")
        fig.tight_layout(pad=1.5)

        buf = BytesIO()
        fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
        buf.seek(0)

        return send_file(buf, mimetype="image/png")

    except Exception as e:
        # Return a minimal fallback PNG so the dashboard image still renders.
        fig = Figure(figsize=(8, 4), dpi=110)
        fig.patch.set_facecolor("#1e293b")
        ax = fig.add_subplot(1, 1, 1)
        ax.set_facecolor("#0f172a")
        ax.text(
            0.5,
            0.5,
            f"Sem dados disponíveis\n{e}",
            ha="center",
            va="center",
            color="#797876",
            fontsize=10,
            transform=ax.transAxes
        )
        ax.set_axis_off()

        buf = BytesIO()
        fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
        buf.seek(0)

        return send_file(buf, mimetype="image/png")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Use the PORT environment variable if set (by Render), otherwise default to 5000.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)