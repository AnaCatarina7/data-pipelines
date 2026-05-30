import os
import sys
import json
import threading
import queue
import requests
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)


# ── State ─────────────────────────────────────────────────────────────────────
log_queue        = queue.Queue()  # holds log messages to stream to the browser
pipeline_running = False          # prevents concurrent pipeline runs


# ── Stdout capture ────────────────────────────────────────────────────────────
class QueueLogger:
    """Redirects stdout to the log queue so every print() reaches the browser."""
    def __init__(self, q):
        self.queue    = q
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

        # pass parameters to the pipeline script via environment variables
        os.environ["START_DATE"] = start_date
        os.environ["DAYS_BACK"]  = str(days_back)
        os.environ["SOURCE"]     = source

        import importlib.util, pathlib

        # select script based on source
        if source == "fronius":
            script_name = "ACTC-Fronius2.py"
        else:
            script_name = "ACTC-BalcaoDigital.py"
            # pass the correct folder id depending on which Drive folder was selected
            folder_id = (
                os.getenv("BALCAO_SHARED_FOLDER_ID")
                if source == "shared_drive"
                else os.getenv("BALCAO_OWN_FOLDER_ID")
            )
            os.environ["BALCAO_FOLDER_ID"] = folder_id or ""

        script_path = pathlib.Path(__file__).parent / "scripts" / script_name

        if script_path.exists():
            spec   = importlib.util.spec_from_file_location("pipeline", script_path)
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
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Script not found at: {script_path}\n")
            
    except Exception as e:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {e}\n")

    finally:
        log_queue.put("__DONE__")
        sys.stdout       = sys.__stdout__
        pipeline_running = False


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """Serves the main control panel."""
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run():
    """Starts the pipeline in a background thread. Rejects concurrent runs."""
    global pipeline_running
    if pipeline_running:
        return jsonify({"error": "Pipeline já está em execução."}), 409

    data       = request.get_json()
    start_date = data.get("start_date", datetime.today().strftime("%Y-%m-%d"))
    days_back  = int(data.get("days_back", 7))
    source     = data.get("source", "fronius")

    # clear any leftover messages from a previous run
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
    """Server-Sent Events endpoint — pushes log lines to the browser in real time."""
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=30)
                if msg == "__DONE__":
                    yield "data: __DONE__\n\n"
                    break
                yield f"data: {msg.rstrip()}\n\n"
            except queue.Empty:
                yield "data: [timeout] Sem resposta há 30s.\n\n"
                break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/status")
def status():
    """Returns whether the pipeline is currently running (used by the frontend)."""
    return jsonify({"running": pipeline_running})


@app.route("/files")
def list_files():
    """Returns the list of files depending on the selected source."""
    source = request.args.get("source", "fronius")

    if source == "fronius":
        token   = os.getenv("GITHUB_TOKEN")
        url     = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/Fronius"
        headers = {"Authorization": f"token {token}"}
        r       = requests.get(url, headers=headers)
        files   = [f["name"] for f in r.json() if isinstance(f, dict) and "name" in f]
        return jsonify({"files": files})

    elif source in ("shared_drive", "drive_own"):
        folder_id = (
            os.getenv("BALCAO_SHARED_FOLDER_ID")
            if source == "shared_drive"
            else os.getenv("BALCAO_OWN_FOLDER_ID")
        )
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            # credentials loaded from env var (JSON string) or secrets file
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
            drive    = build("drive", "v3", credentials=credentials)
            response = drive.files().list(
                q=f"'{folder_id}' in parents",
                fields="files(id, name, size, modifiedTime)"
            ).execute()
            files = [f["name"] for f in response.get("files", [])]
            return jsonify({"files": files})

        except Exception as e:
            return jsonify({"files": [], "error": str(e)})

    return jsonify({"files": []})



# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # PORT is injected by Render in production; defaults to 5000 locally
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)