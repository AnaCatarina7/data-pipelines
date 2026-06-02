# ================================================================================
# ACTC-BalcaoDigital.py
# Balcão Digital (E-Redes) data pipeline - adapted for Colab, local, Flask and Render
# ================================================================================

import os
import sys
import datetime
import json
import socket
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

import clts_pcp as clts

print("... imports done.")


# ================================================================================
# detect environment: colab, render or local
# ================================================================================
def detect_environment():
    if "COLAB_RELEASE_TAG" in os.environ:
        return "colab"
    elif "RENDER" in os.environ:
        return "render"
    else:
        return "local"


# ================================================================================
# secret loaders - unified interface for all environments
# ================================================================================
def get_secret(name):
    return os.getenv(name)


def get_secret_json(name):
    if env == "colab":
        return json.loads(userdata.get(name))
    elif env == "render":
        with open(f"/etc/secrets/{name}") as f:
            return json.load(f)
    else:
        with open(f"secrets/{name}") as f:
            return json.load(f)


# ================================================================================
# fetch existing IDs from CrateDB in batches to avoid statement_max_length limits
# ================================================================================
def fetch_existing_ids_in_batches(cursor, table, all_ids, batch_size=500):
    existing = set()
    for i in range(0, len(all_ids), batch_size):
        batch        = all_ids[i:i + batch_size]
        placeholders = ", ".join(["?" for _ in batch])
        cursor.execute(f"SELECT id FROM {table} WHERE id IN ({placeholders})", batch)
        existing.update(row[0] for row in cursor.fetchall())
    return existing


# ================================================================================
# pipeline
# ================================================================================
def run_pipeline(run_params=None):
    global env, hostname, ip, user, script, channel, destination, verbose, send_mail, email_addresses, context

    env = detect_environment()
    print("Running in:", env)

    if env == "colab":
        from google.colab import userdata  # type: ignore
        import ipynbname                   # type: ignore
    elif env == "local":
        from dotenv import load_dotenv
        load_dotenv()

    tstart = clts.getts()
    clts.elapt.clear()

    DEFAULT_PARAMS = {
        "verbose":         True,
        "destination":     "-*-",
        "send_mail":       True,
        "email_addresses": ["acatarinatc@gmail.com"],
    }

    if run_params:
        if run_params.get("start_date"):
            DEFAULT_PARAMS["start_date"] = run_params["start_date"]
        if run_params.get("days_back") is not None:
            DEFAULT_PARAMS["days_back"] = int(run_params["days_back"])

    hostname = socket.gethostname()
    ip       = requests.get("https://api.ipify.org").text
    print("Server name:", hostname, "Public IP Address:", ip)

    if env == "colab":
        notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
        user         = notebookname.split("-")[0]
        script       = ipynbname.name()
    else:
        user   = os.getenv("USER", "ACTC")
        script = os.path.basename(__file__)

    channel         = "balcao_digital"
    destination     = DEFAULT_PARAMS["destination"]
    verbose         = DEFAULT_PARAMS["verbose"]
    send_mail       = DEFAULT_PARAMS["send_mail"]
    email_addresses = DEFAULT_PARAMS["email_addresses"]

    context = f"{hostname} ({ip}) | {user} | {channel} | {script} | {destination}"
    clts.setcontext(context)
    if verbose:
        print("context:", context)
    clts.elapt[f"Environment detected: {env}"] = clts.deltat(tstart)

    # ================================================================================
    # derive date window from run_params
    # ================================================================================
    start_date_str = DEFAULT_PARAMS.get("start_date", datetime.date.today().strftime("%Y-%m-%d"))
    days_back      = DEFAULT_PARAMS.get("days_back", 0)
    start_date     = datetime.datetime.strptime(str(start_date_str), "%Y-%m-%d").date()
    window_start   = start_date - datetime.timedelta(days=days_back)
    window_end     = start_date
    print(f"Date window: {window_start} → {window_end}")

    # ================================================================================
    # connect to google drive and list files in folder
    # ================================================================================
    # FOLDER_ID = os.getenv("BALCAO_FOLDER_ID", "1flPuJWecI4DomnQZVitG7mS8vRcvXjOW")
    FOLDER_ID = os.getenv("BALCAO_SHARED_FOLDER_ID")

    creds_raw   = get_secret_json("ACTC-DriveCredentials.json")
    credentials = service_account.Credentials.from_service_account_info(
        creds_raw,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=credentials)
    print("Connected to Google Drive.")

    # list all files in folder (excluding trashed), supporting shared drives
    response = service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name, size, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    all_files = response.get("files", [])
    clts.elapt["Balcão Digital files list retrieved from Google Drive"] = clts.deltat(tstart)
    print(f"Total files found in folder: {len(all_files)}")

    # ── filter files by date window using filename: Consumos_<CPE>_<YYYYMMDD>HHMMSS.xlsx ──
    files = []
    for f in all_files:
        try:
            date_str  = f["name"].split("_")[2][:8]   # e.g. "20260425"
            file_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            if window_start <= file_date <= window_end:
                files.append(f)
        except Exception:
            pass

    clts.elapt[f"Files identified ({window_start} → {window_end}, {days_back} days back)"] = clts.deltat(tstart)
    print(f"Files in window ({window_start} → {window_end}): {len(files)} / {len(all_files)}")
    for f in files:
        print(f"  {f['name']}  |  size: {f.get('size', 'N/A')} bytes  |  modified: {f['modifiedTime']}")

    # ================================================================================
    # download filtered files into memory as BytesIO objects
    # ================================================================================
    raw_files = {}

    for f in files:
        file_id  = f["id"]
        filename = f["name"]

        request    = service.files().get_media(fileId=file_id)
        buffer     = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done       = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        raw_files[filename] = buffer
        clts.elapt[f"Downloaded {filename}"] = clts.deltat(tstart)
        print(f"Downloaded into memory: {filename}")

    print(f"\nTotal files in memory: {len(raw_files)}")
    clts.elapt[f"Files downloaded: {len(raw_files)}"] = clts.deltat(tstart)

    if not raw_files:
        print("No files found for the selected date window. Nothing to process.")
        return

    # ================================================================================
    # parse files — detect type A or B by column content
    # Type A: potência (Potência Ativa / Reativa)   → balcao_digital
    # Type B: consumo registado (kW)                → balcao_digital_consumo
    # skiprows is auto-detected (tries 8 then 9)
    # CPE is extracted from the filename: Consumos_<CPE>_<timestamp>.xlsx
    # ================================================================================
    dfs_a         = []
    dfs_b         = []
    skipped_files = []

    for filename, buffer in raw_files.items():
        cpe = filename.split("_")[1]

        try:
            file_type = None
            skip      = None

            # auto-detect skiprows and file type
            for sr in [8, 9]:
                buffer.seek(0)
                df_peek = pd.read_excel(buffer, skiprows=sr, header=0, nrows=0)
                cols    = df_peek.columns.tolist()

                if "Consumo registado (kW)" in cols:
                    file_type = "B"
                    skip      = sr
                    break
                elif any("Potência Ativa" in c for c in cols):
                    file_type = "A"
                    skip      = sr
                    break

            if file_type is None:
                print(f"⚠️  Unknown file type: {filename} — columns: {cols}")
                print(f"⚠️  This file will be skipped. Schema not yet supported.")
                skipped_files.append(filename)
                clts.elapt[f"SKIPPED (unknown schema): {filename}"] = clts.deltat(tstart)
                continue

            buffer.seek(0)
            df = pd.read_excel(buffer, skiprows=skip, header=0)
            df = df.dropna(subset=["Data", "Hora"])
            df["tstamp"] = pd.to_datetime(
                df["Data"].astype(str) + " " + df["Hora"].astype(str),
                format="%Y/%m/%d %H:%M",
                errors="coerce"
            )
            df["cpe"] = cpe
            df["id"]  = cpe + "_" + df["tstamp"].dt.strftime("%Y%m%d%H%M")

            if file_type == "B":
                df = df.rename(columns={"Estado": "estado"})
                df["consumo_kw"] = pd.to_numeric(df["Consumo registado (kW)"], errors="coerce")
                df = df[["id", "tstamp", "cpe", "consumo_kw", "estado"]]
                dfs_b.append(df)
                clts.elapt[f"{filename} parsed (Type B): {len(df)} records"] = clts.deltat(tstart)
                print(f"{filename}: {len(df)} rows | Type B | CPE: {cpe}")

            else:  # file_type == "A"
                df = df.rename(columns={
                    "Potência Ativa Saldo (kW) - Consumo":          "potencia_ativa",
                    "Potência Reativa Indutiva (kVAr) - Consumo":   "potencia_reativa_ind",
                    "Potência Reativa Capacitiva (kVAr) - Consumo": "potencia_reativa_cap"
                })
                df = df[["id", "tstamp", "cpe", "potencia_ativa", "potencia_reativa_ind", "potencia_reativa_cap"]]
                dfs_a.append(df)
                clts.elapt[f"{filename} parsed (Type A): {len(df)} records"] = clts.deltat(tstart)
                print(f"{filename}: {len(df)} rows | Type A | CPE: {cpe}")

        except Exception as e:
            print(f"Error parsing {filename}: {e}")
            skipped_files.append(filename)
            clts.elapt[f"Error parsing {filename}: {e}"] = clts.deltat(tstart)

    df_final_a = pd.concat(dfs_a, ignore_index=True) if dfs_a else pd.DataFrame()
    df_final_b = pd.concat(dfs_b, ignore_index=True) if dfs_b else pd.DataFrame()

    clts.elapt["All files parsed"] = clts.deltat(tstart)
    print(f"\nType A (potência):  {len(df_final_a):,} rows" if not df_final_a.empty else "Type A: 0 rows")
    print(f"Type B (consumo):   {len(df_final_b):,} rows" if not df_final_b.empty else "Type B: 0 rows")
    if skipped_files:
        print(f"Skipped files ({len(skipped_files)}): {skipped_files}")

    # ================================================================================
    # insert into CrateDB
    # id = cpe + tstamp (YYYYMMDDHHMM, no seconds) — consistent with historical data
    # batching avoids statement_max_length limits
    # ================================================================================
    dblist = get_secret_json(f"{user}-dblist.json")
    print(dblist)

    status = "nok"
    clts.elapt["Connecting to CrateDB"] = clts.deltat(tstart)
    if verbose:
        print("connecting to crate")

    try:
        print(f"Credentials in `{user}-crate.json`")
        dbcreds = get_secret_json(f"{user}-crate.json")

        from crate import client
        print("... connecting to crate database...")
        connection = client.connect(
            dbcreds["dest_host"],
            username=dbcreds["username"],
            password=dbcreds["password"],
            verify_ssl_cert=True
        )
        cursor = connection.cursor()
        clts.elapt["... connected to CrateDB"] = clts.deltat(tstart)
        status = "ok"

    except Exception as e:
        print("Error:", e)
        clts.elapt[f"... error connecting: {e}"] = clts.deltat(tstart)
        status = "onerror"

    print("status:", status)

    if status == "ok":

        for table, df_insert in [
            ("balcao_digital",         df_final_a),
            ("balcao_digital_consumo", df_final_b)
        ]:
            if df_insert.empty:
                print(f"No data for {table}, skipping.")
                continue

            inserts      = 0
            skipped      = 0
            skipped_list = []

            clts.elapt[f"Processing {table}"] = clts.deltat(tstart)

            all_ids      = df_insert["id"].tolist()
            existing_ids = fetch_existing_ids_in_batches(cursor, table, all_ids)
            clts.elapt[f"... {len(existing_ids)} existing records fetched from {table}"] = clts.deltat(tstart)

            values_to_insert = []
            for _, row in df_insert.iterrows():
                if row["id"] in existing_ids:
                    skipped += 1
                    skipped_list.append(str(row["tstamp"]))
                else:
                    if table == "balcao_digital":
                        values_to_insert.append((
                            row["id"],
                            row["tstamp"].strftime("%Y-%m-%d %H:%M:%S"),
                            row["cpe"],
                            float(row["potencia_ativa"]),
                            float(row["potencia_reativa_ind"]),
                            float(row["potencia_reativa_cap"])
                        ))
                    else:
                        values_to_insert.append((
                            row["id"],
                            row["tstamp"].strftime("%Y-%m-%d %H:%M:%S"),
                            row["cpe"],
                            float(row["consumo_kw"]),
                            str(row["estado"])
                        ))
                    inserts += 1

            if values_to_insert:
                if table == "balcao_digital":
                    sql = (
                        "INSERT INTO balcao_digital "
                        "(id, tstamp, cpe, potencia_ativa, potencia_reativa_ind, potencia_reativa_cap) "
                        "VALUES (?, ?, ?, ?, ?, ?)"
                    )
                else:
                    sql = (
                        "INSERT INTO balcao_digital_consumo "
                        "(id, tstamp, cpe, consumo_kw, estado) "
                        "VALUES (?, ?, ?, ?, ?)"
                    )
                cursor.executemany(sql, values_to_insert)
                connection.commit()
                cursor.execute(f"REFRESH TABLE {table}")

            clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {table}"] = clts.deltat(tstart)
            print(f"... {inserts} inserted, {skipped} skipped @ {table}")
            if skipped_list:
                print(f"... skipped timestamps @ {table}: " + ", ".join(skipped_list[:10]) +
                      (f" ... +{len(skipped_list)-10} more" if len(skipped_list) > 10 else ""))

    print("Connection closing....")
    connection.close()
    clts.elapt["... CrateDB connection closed"] = clts.deltat(tstart)

    # ================================================================================
    # send profiling email
    # ================================================================================
    clts.elapt["Overall (before email):"] = clts.deltat(tstart)

    if send_mail and email_addresses:
        toem = clts.listtimes()

        if env == "colab":
            notebook_link_html = "<p><a href='[URL_DO_NOTEBOOK]'>&#128211; Abrir notebook no Colab</a></p>"
        else:
            notebook_link_html = ""

        html = f"""
        <html>
            <body style='font-family:Montserrat;'>
                {notebook_link_html}
                <hr color='orange'>
                {toem}
                <hr color='orange'>
                This message is an automated notification from {context}
            </body>
        </html>
        """

        if env == "render":
            try:
                import resend
                resend.api_key = os.getenv("RESEND_API_KEY")
                resend.Emails.send({
                    "from":    "onboarding@resend.dev",
                    "to":      email_addresses,
                    "subject": context,
                    "html":    html
                })
                print("Notification sent.")
                clts.elapt["After sending email"] = clts.deltat(tstart)
            except Exception as e:
                print("Notification not sent:", e)
                clts.elapt[f"email not sent ({e})"] = clts.deltat(tstart)
        else:
            try:
                if env == "colab":
                    credsgmail = json.loads(userdata.get(f"configGMail_{user}.json"))
                else:
                    with open("./secrets/configGMail_ACTC.json", "r") as fh:
                        credsgmail = json.loads(fh.read())

                message = MIMEMultipart("alternative")
                message["Subject"] = context
                message["From"]    = credsgmail["UserFrom"]
                message["To"]      = ", ".join(email_addresses)

                message.attach(MIMEText(f"This is an automated notification from {context}", "plain"))
                message.attach(MIMEText(html, "html"))

                ssl_context = ssl.create_default_context()
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl_context) as server:
                    server.login(credsgmail["UserName"], credsgmail["UserPwd"])
                    server.sendmail(credsgmail["UserFrom"], email_addresses, message.as_string())

                print("Notification sent.")
                clts.elapt["After sending email"] = clts.deltat(tstart)

            except Exception as e:
                print("Notification not sent:", e)
                clts.elapt[f"email not sent ({e})"] = clts.deltat(tstart)


if __name__ == "__main__":
    run_pipeline()