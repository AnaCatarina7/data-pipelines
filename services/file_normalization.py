import os
import re
from pathlib import Path

import pandas as pd


def _safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def _extract_cpe_from_filename(filename):
    match = re.search(r"(PT\d{16}[A-Z]{2})", filename, re.IGNORECASE)
    return match.group(1).upper() if match else None


def _extract_file_day_from_filename(filename):
    digits = re.findall(r"(\d{14})", filename)
    if digits:
        stamp = digits[-1]
        try:
            return f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
        except Exception:
            return None
    return None


def _normalize_text_for_filename(text):
    text = _safe_str(text).lower()
    text = text.replace("ã", "a").replace("á", "a").replace("à", "a").replace("â", "a")
    text = text.replace("é", "e").replace("ê", "e")
    text = text.replace("í", "i")
    text = text.replace("ó", "o").replace("ô", "o").replace("õ", "o")
    text = text.replace("ú", "u")
    text = text.replace("ç", "c")
    text = re.sub(r"[^a-z0-9()]+", "_", text)
    return text.strip("_")


def _extract_date_from_metadata_rows(df_meta):
    for _, row in df_meta.iterrows():
        values = [str(v).strip() for v in row.tolist() if pd.notna(v)]
        joined = " ".join(values)

        m = re.search(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?", joined)
        if m:
            try:
                date_str = m.group(1)
                time_str = m.group(2) or "00:00:00"
                if len(time_str) == 5:
                    time_str = f"{time_str}:00"
                dt = pd.to_datetime(f"{date_str} {time_str}", dayfirst=True, errors="coerce")
                if pd.notna(dt):
                    return dt
            except Exception:
                pass

    return None


def _read_local_file_date(file_path):
    meta_df = pd.read_excel(file_path, header=None, nrows=12)
    return _extract_date_from_metadata_rows(meta_df)


def build_normalized_filename(file_day, cpe=None, original_suffix=".xlsx"):
    date_part = file_day if file_day else "sem_data"
    cpe_part = f" ({cpe})" if cpe else ""
    base_name = f"balcao_digital_{date_part}{cpe_part}"
    return f"{_normalize_text_for_filename(base_name)}{original_suffix}"


def build_display_title(file_day, cpe=None):
    date_part = file_day if file_day else "Sem data"
    cpe_part = f" ({cpe})" if cpe else ""
    return f"Balcão Digital - {date_part}{cpe_part}"


def normalize_file_metadata(file_ref):
    path = Path(file_ref)
    original_name = path.name
    original_suffix = path.suffix if path.suffix else ".xlsx"

    result = {
        "original_name": original_name,
        "original_path": str(path),
        "cpe": _extract_cpe_from_filename(original_name),
        "file_day": None,
        "normalized_filename": original_name,
        "display_title": original_name,
        "status": "ok",
        "error": None,
    }

    try:
        dt = None

        if os.path.exists(file_ref):
            dt = _read_local_file_date(file_ref)

        file_day = dt.strftime("%Y-%m-%d") if dt is not None else _extract_file_day_from_filename(original_name)

        result["file_day"] = file_day
        result["normalized_filename"] = build_normalized_filename(
            file_day=file_day,
            cpe=result["cpe"],
            original_suffix=original_suffix
        )
        result["display_title"] = build_display_title(file_day, result["cpe"])

        return result

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return result


def normalize_files_metadata(file_refs):
    return [normalize_file_metadata(file_ref) for file_ref in file_refs]