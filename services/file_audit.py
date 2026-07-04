import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from crate import client


# Hash utilities
def compute_file_hash(file_bytes: bytes) -> str:
    """
    Return the SHA-256 hash of a file's binary content.
    """
    return hashlib.sha256(file_bytes).hexdigest()


# Audit row builder
def build_file_audit_row(
    filename: str,
    source: str,
    file_bytes: bytes,
    status: str = "processed"
) -> dict:
    """
    Build one audit row matching the file_audit table structure.
    """
    return {
        "file_hash": compute_file_hash(file_bytes),
        "filename": filename,
        "source": source,
        "size_bytes": len(file_bytes),
        "processed_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "status": status,
    }


# CrateDB connection
def get_connection(db_type: str = "crate"):
    """
    Open a CrateDB or TiDB connection using secrets.
    """
    secrets_dir = Path(__file__).resolve().parent.parent / "secrets"

    if db_type.lower() == "tidb":
        import pymysql

        secrets_path = secrets_dir / "ACTC-tidb.json"
        with open(secrets_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        return pymysql.connect(
            host=cfg["host"],
            port=int(cfg.get("port", 4000)),
            user=cfg["username"],
            password=cfg["password"],
            database=cfg["database"],
            autocommit=False,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.Cursor,
        )

    secrets_path = secrets_dir / "ACTC-crate.json"
    with open(secrets_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    return client.connect(
        cfg["dest_host"],
        username=cfg["username"],
        password=cfg["password"],
        timeout=cfg.get("timeout", 20),
        verify_ssl_cert=True,
    )


# Duplicate check
def file_hash_exists(connection, file_hash: str) -> bool:
    """
    Check whether a file hash already exists in file_audit.
    """
    cursor = connection.cursor()
    try:
        if connection.__class__.__module__.lower().startswith("pymysql"):
            cursor.execute(
                "SELECT file_hash FROM file_audit WHERE file_hash = %s",
                (file_hash,)
            )
        else:
            cursor.execute(
                "SELECT file_hash FROM file_audit WHERE file_hash = ?",
                (file_hash,)
            )
        result = cursor.fetchone()
        return result is not None
    finally:
        cursor.close()


# Insert audit row
def insert_file_audit_row(connection, row: dict):
    """
    Insert one audit row into file_audit.
    """
    cursor = connection.cursor()
    try:
        if connection.__class__.__module__.lower().startswith("pymysql"):
            cursor.execute(
                """
                INSERT IGNORE INTO file_audit (
                    file_hash,
                    filename,
                    source,
                    size_bytes,
                    processed_at,
                    status
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row["file_hash"],
                    row["filename"],
                    row["source"],
                    row["size_bytes"],
                    row["processed_at"],
                    row["status"],
                )
            )
            connection.commit()
        else:
            cursor.execute(
                """
                INSERT INTO file_audit (
                    file_hash,
                    filename,
                    source,
                    size_bytes,
                    processed_at,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["file_hash"],
                    row["filename"],
                    row["source"],
                    row["size_bytes"],
                    row["processed_at"],
                    row["status"],
                )
            )
    finally:
        cursor.close()


# Optional helper
def prepare_file_audit(filename: str, source: str, file_bytes: bytes) -> dict:
    """
    Prepare an audit row without inserting it yet.
    """
    return build_file_audit_row(
        filename=filename,
        source=source,
        file_bytes=file_bytes,
        status="processed"
    )