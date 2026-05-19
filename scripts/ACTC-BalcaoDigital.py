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
        "verbose": True,
        "destination": "-*-",
        "send_mail": True,
        "email_addresses": ["acatarinatc@gmail.com"],
    }

    if run_params:
        if run_params.get("start_date"):
            DEFAULT_PARAMS["start_date"] = run_params["start_date"]
        if run_params.get("days_back") is not None:
            DEFAULT_PARAMS["days_back"] = int(run_params["days_back"])

    hostname = socket.gethostname()
    ip = requests.get("https://api.ipify.org").text
    print("Server name:", hostname, "Public IP Address:", ip)

    if env == "colab":
        notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
        user = notebookname.split("-")[0]
        script = ipynbname.name()
    else:
        user = os.getenv("USER", "ACTC")
        script = os.path.basename(__file__)

    channel = "balcao_digital"
    destination = DEFAULT_PARAMS["destination"]
    verbose = DEFAULT_PARAMS["verbose"]
    send_mail = DEFAULT_PARAMS["send_mail"]
    email_addresses = DEFAULT_PARAMS["email_addresses"]

    context = f"{hostname} ({ip}) | {user} | {channel} | {script} | {destination}"
    clts.setcontext(context)
    clts.elapt[f"Environment detected: {env}"] = clts.deltat(tstart)

    if verbose:
        print("context:", context)

    # ================================================================================
    # connect to google drive and list files in folder
    # ================================================================================
    FOLDER_ID = os.getenv("BALCAO_FOLDER_ID", "1flPuJWecI4DomnQZVitG7mS8vRcvXjOW")

    creds_raw = get_secret_json("ACTC-DriveCredentials.json")
    credentials = service_account.Credentials.from_service_account_info(
        creds_raw,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=credentials)
    print("Connected to Google Drive.")

    response = service.files().list(
        q=f"'{FOLDER_ID}' in parents",
        fields="files(id, name, size, modifiedTime)"
    ).execute()

    files = response.get("files", [])
    clts.elapt["Balcão Digital files list retrieved from Google Drive"] = clts.deltat(tstart)
    print(f"Total files found: {len(files)}")
    for f in files:
        print(f"  {f['name']}  |  size: {f.get('size', 'N/A')} bytes  |  modified: {f['modifiedTime']}")

    # ================================================================================
    # download files into memory as BytesIO objects to avoid disk usage
    # ================================================================================
    raw_files = {}

    for f in files:
        file_id = f["id"]
        filename = f["name"]

        request = service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        raw_files[filename] = buffer
        clts.elapt[f"Downloaded {filename}"] = clts.deltat(tstart)
        print(f"Downloaded into memory: {filename}")

    print(f"\nTotal files in memory: {len(raw_files)}")
    clts.elapt[f"Files downloaded: {len(raw_files)}"] = clts.deltat(tstart)

    if not raw_files:
        print("No files found. Nothing to process.")
        return

    # ================================================================================
    # parse all files: extract CPE from header + read data rows
    # skiprows=9 skips the metadata block at the top of each file
    # CPE is located at row index 2, column 1 of the raw header
    # ================================================================================
    SKIPROWS = 9
    all_dfs = []

    for filename, buffer in raw_files.items():
        buffer.seek(0)
        df_meta = pd.read_excel(buffer, header=None, nrows=3)
        cpe = df_meta.iloc[2, 1]

        buffer.seek(0)
        df_temp = pd.read_excel(buffer, skiprows=SKIPROWS, header=0)
        df_temp["cpe"] = cpe
        df_temp["source_file"] = filename
        all_dfs.append(df_temp)
        clts.elapt[f"{filename} loaded: {len(df_temp)} records"] = clts.deltat(tstart)
        print(f"{filename}: {len(df_temp)} rows | CPE: {cpe}")

    df = pd.concat(all_dfs, ignore_index=True)
    clts.elapt[f"Data loaded: {len(df)} records from {len(raw_files)} files"] = clts.deltat(tstart)
    print(f"\nTotal records combined: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # ================================================================================
    # data cleaning: parse timestamp, rename columns, drop nulls
    # ================================================================================
    df["tstamp"] = pd.to_datetime(df["Data"].astype(str) + " " + df["Hora"].astype(str))
    df = df.rename(columns={
        "Potência Ativa Saldo (kW) - Consumo":         "potencia_ativa",
        "Potência Reativa Indutiva (kVAr) - Consumo":  "potencia_reativa_ind",
        "Potência Reativa Capacitiva (kVAr) - Consumo":"potencia_reativa_cap"
    })
    df = df.dropna(subset=["tstamp", "cpe", "potencia_ativa"])
    df["potencia_ativa"] = df["potencia_ativa"].astype(float)
    df["potencia_reativa_ind"] = df["potencia_reativa_ind"].astype(float)
    df["potencia_reativa_cap"] = df["potencia_reativa_cap"].astype(float)

    clts.elapt["Timestamp parsed and columns renamed"] = clts.deltat(tstart)
    print("Data types after conversion:")
    print(df[["tstamp", "cpe", "potencia_ativa", "potencia_reativa_ind", "potencia_reativa_cap"]].dtypes)
    print("\nNull values after conversion:")
    print(df[["tstamp", "cpe", "potencia_ativa"]].isnull().sum())

    # ================================================================================
    # insert into CrateDB
    # id = cpe + tstamp to ensure uniqueness across multiple meters
    # ================================================================================
    dblist = get_secret_json(f"{user}-dblist.json")
    print(dblist)

    for db in dblist:
        if db != "crate":
            continue

        status = "nok"
        clts.elapt[f"Connecting to `{db}`"] = clts.deltat(tstart)
        if verbose:
            print(f"db in dblist: {db}")
            print(f"connecting to `{db}`")

        try:
            print(f"Credentials in `{user}-{db}.json`")
            dbcreds = get_secret_json(f"{user}-{db}.json")

            from crate import client
            print("... connecting to crate database...")
            connection = client.connect(
                dbcreds["dest_host"],
                username=dbcreds["username"],
                password=dbcreds["password"],
                verify_ssl_cert=True
            )
            cursor = connection.cursor()
            clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
            status = "ok"

        except Exception as e:
            print("Error:", e)
            clts.elapt[f"... error `{e}`"] = clts.deltat(tstart)
            status = "onerror"

        print("status:", status)

        if status == "ok":
            inserts = 0
            skipped = 0

            try:
                all_ids = [
                    f"{row['cpe']}_{row['tstamp'].strftime('%Y%m%d%H%M%S')}"
                    for _, row in df.iterrows()
                ]
                placeholders = ", ".join(["?" for _ in all_ids])
                sql_check = f"SELECT id FROM balcao_digital WHERE id IN ({placeholders})"
                cursor.execute(sql_check, all_ids)
                existing_ids = set(row[0] for row in cursor.fetchall())
                clts.elapt[f"... {len(existing_ids)} existing records fetched from {db}"] = clts.deltat(tstart)

                values_to_insert = []
                for _, row in df.iterrows():
                    tstamp = row["tstamp"]
                    row_id = f"{row['cpe']}_{tstamp.strftime('%Y%m%d%H%M%S')}"

                    if row_id in existing_ids:
                        skipped += 1
                    else:
                        values_to_insert.append((
                            row_id,
                            tstamp.strftime("%Y-%m-%d %H:%M:%S"),
                            row["cpe"],
                            row["potencia_ativa"],
                            row["potencia_reativa_ind"],
                            row["potencia_reativa_cap"]
                        ))
                        inserts += 1

                if values_to_insert:
                    sql = (
                        "INSERT INTO balcao_digital "
                        "(id, tstamp, cpe, potencia_ativa, potencia_reativa_ind, potencia_reativa_cap) "
                        "VALUES (?, ?, ?, ?, ?, ?)"
                    )
                    cursor.executemany(sql, values_to_insert)
                    connection.commit()
                    cursor.execute("REFRESH TABLE balcao_digital")

                clts.elapt[f"... {inserts} inserted, {skipped} skipped @ {db}"] = clts.deltat(tstart)
                print(f"... {inserts} inserted, {skipped} skipped @ {db}")

            except Exception as e:
                print("Error:", e)
                clts.elapt[f"... error inserting into `{db}`: `{e}`"] = clts.deltat(tstart)

        print("Connection closing....")
        connection.close()
        clts.elapt[f"... connection to `{db}` closed"] = clts.deltat(tstart)

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
                message["From"] = credsgmail["UserFrom"]
                message["To"] = ", ".join(email_addresses)

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