#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Schema Profiler — simple y elegante.
Autor: (tú)
Descripción:
- Recorre recursivamente una ruta, abre todos los Excel (.xlsx, .xlsm, .xls),
- Lee todas las hojas, detecta automáticamente la fila de encabezados
  como la primera fila que contiene al menos una celda no vacía,
- Infere el tipo de dato por columna y estima una probabilidad basada
  en el porcentaje de celdas válidas para ese tipo.
- Exporta CSV (y opcionalmente JSON) con el perfil de columnas.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

# --------- Utilidades de I/O ---------

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}

def iter_excel_files(root: str) -> List[str]:
    files = []
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext in EXCEL_EXTS:
                files.append(os.path.join(dirpath, fname))
    return sorted(files)

# --------- Detección de encabezado ---------

def first_nonempty_row(df: pd.DataFrame) -> Optional[int]:
    """
    Devuelve el índice (0-based) de la primera fila que tenga al menos
    una celda no nula/no vacía. Si no encuentra, devuelve None.
    """
    if df.empty:
        return None
    # Considerar vacíos: NaN, "", "   "
    mask_nonempty = df.applymap(lambda x: (pd.notna(x)) and (str(x).strip() != ""))
    nonempty_rows = mask_nonempty.any(axis=1)
    idx = nonempty_rows.idxmax() if nonempty_rows.any() else None
    if isinstance(idx, (np.integer, int)):
        return int(idx)
    return None

def normalize_headers(row_vals: List[Any]) -> List[str]:
    """
    Convierte los valores de encabezado en strings legibles y únicos.
    """
    headers = []
    seen = {}
    for i, v in enumerate(row_vals):
        name = str(v).strip() if pd.notna(v) and str(v).strip() != "" else f"col_{i+1}"
        # Asegurar unicidad
        base = name
        k = 1
        while name in seen:
            k += 1
            name = f"{base}__{k}"
        seen[name] = True
        headers.append(name)
    return headers

# --------- Inferencia de tipos ---------

def try_parse_bool(v: Any) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in {"true", "false", "1", "0", "yes", "no", "si", "sí"}

def try_parse_int(v: Any) -> bool:
    if pd.isna(v):
        return False
    # Numérico no string con parte decimal 0
    if isinstance(v, (int, np.integer)):
        return True
    # Floats con .0 exacto
    if isinstance(v, (float, np.floating)):
        return float(v).is_integer()
    # String
    s = str(v).strip()
    # Permite signo
    if s.count(",") > 0 and s.count(".") == 0:
        # Caso 1.234 (coma miles) común en ES: remover separadores
        s_clean = s.replace(".", "").replace(",", "")
    else:
        s_clean = s.replace(",", "")
    if s_clean.startswith(("+", "-")):
        s_num = s_clean[1:]
    else:
        s_num = s_clean
    return s_num.isdigit()

def try_parse_float(v: Any) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, (int, np.integer, float, np.floating)):
        return True
    s = str(v).strip().replace(" ", "")
    s = s.replace(",", ".")  # Soportar 1,23
    try:
        float(s)
        return True
    except Exception:
        return False

def try_parse_datetime(v: Any) -> bool:
    if pd.isna(v):
        return False
    # Si ya es datetime
    if isinstance(v, (datetime, np.datetime64, pd.Timestamp)):
        return True
    # Intento flexible
    try:
        # pandas maneja muchos formatos; errors='coerce' devuelve NaT si falla
        ts = pd.to_datetime(v, errors="coerce", dayfirst=True, infer_datetime_format=True)
        return pd.notna(ts)
    except Exception:
        return False

TYPE_CHECKS = [
    ("boolean", try_parse_bool),
    ("integer", try_parse_int),
    ("float",   try_parse_float),
    ("datetime", try_parse_datetime),
    # "text" se infiere por descarte
]

def infer_type_and_prob(series: pd.Series, sample_max: int = 1000) -> Tuple[str, float, int, float, List[str]]:
    """
    Devuelve (best_type, probability, n_samples, null_ratio, examples)
    - probability: fracción de valores no nulos que calzan con el tipo elegido.
    - examples: hasta 3 valores distintos (como texto) para inspeccionar.
    """
    # Valores
    vals = series.dropna()
    # También descartar strings vacíos
    vals = vals[[str(x).strip() != "" for x in vals]]
    n_nonnull = len(vals)

    # Preparar ejemplos
    examples = []
    if n_nonnull > 0:
        for v in vals.head(50):
            s = str(v)
            if s not in examples:
                examples.append(s)
            if len(examples) >= 3:
                break

    # Si no hay datos, es "text" vacío
    if n_nonnull == 0:
        null_ratio = 1.0
        return ("text", 1.0, 0, null_ratio, examples)

    # Muestreo para performance (si la columna es muy grande)
    if n_nonnull > sample_max:
        vals = vals.sample(sample_max, random_state=42)
    n_sample = len(vals)

    # Scoring por tipo
    scores: Dict[str, int] = {t: 0 for t, _ in TYPE_CHECKS}
    for v in vals:
        for tname, checker in TYPE_CHECKS:
            try:
                if checker(v):
                    scores[tname] += 1
            except Exception:
                # Un valor que rompa el checker no debe botar el proceso
                pass

    # Candidatos numéricos/fecha/boolean
    best_type = None
    best_ratio = -1.0
    for tname in ["boolean", "integer", "float", "datetime"]:
        ratio = scores[tname] / n_sample if n_sample else 0.0
        if ratio > best_ratio:
            best_ratio = ratio
            best_type = tname

    # Texto por descarte si ningún tipo gana significativamente
    # Si menos del 50% calza con algo, consideramos 'text'
    if best_ratio < 0.5:
        best_type = "text"
        best_ratio = 1.0 - (scores["boolean"] + scores["integer"] + scores["float"] + scores["datetime"]) / n_sample

    # Refinamiento: si 'float' gana pero todos los válidos son enteros (p.ej. 3.0), preferir 'integer'
    if best_type == "float" and scores["float"] == scores["integer"] and scores["float"] > 0:
        best_type = "integer"

    null_ratio = 1.0 - (n_nonnull / len(series)) if len(series) else 0.0
    return (best_type, float(best_ratio), n_sample, float(null_ratio), examples)

# --------- Procesamiento de una hoja ---------

def profile_sheet(xl_path: str, sheet_name: str) -> List[Dict[str, Any]]:
    """
    Lee una hoja sin encabezado, detecta la primera fila con datos como header,
    reconstruye el DataFrame con esas columnas y perfila cada columna.
    """
    try:
        # Leemos sin encabezado para poder detectar la fila con datos
        raw = pd.read_excel(
            xl_path,
            sheet_name=sheet_name,
            header=None,
            dtype=object,
            engine=None  # que pandas elija: openpyxl/xlrd según corresponda
        )
    except Exception as e:
        return [{
            "file_path": xl_path,
            "sheet_name": sheet_name,
            "error": f"Error leyendo hoja: {e}"
        }]

    if raw.empty:
        return [{
            "file_path": xl_path,
            "sheet_name": sheet_name,
            "error": "Hoja vacía"
        }]

    hdr_row = first_nonempty_row(raw)
    if hdr_row is None:
        return [{
            "file_path": xl_path,
            "sheet_name": sheet_name,
            "error": "No se encontró fila de encabezados (ninguna fila con datos)"
        }]

    headers = normalize_headers(raw.iloc[hdr_row].tolist())
    data = raw.iloc[hdr_row + 1 :].reset_index(drop=True)
    data.columns = headers

    results = []
    for idx, col in enumerate(data.columns, start=1):
        col_ser = data[col]
        inferred, prob, n_samp, null_ratio, examples = infer_type_and_prob(col_ser)

        results.append({
            "file_path": xl_path,
            "sheet_name": sheet_name,
            "header_row_index_1based": hdr_row + 1,
            "column_index_1based": idx,
            "column_name": col,
            "inferred_type": inferred,
            "probability": round(prob, 4),
            "probability_percent": round(prob * 100.0, 2),
            "n_samples_used": n_samp,
            "null_ratio": round(null_ratio, 4),
            "null_percent": round(null_ratio * 100.0, 2),
            "examples": "; ".join(examples)
        })

    return results

# --------- Procesamiento de un archivo ---------

def profile_file(xl_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        xls = pd.ExcelFile(xl_path, engine=None)
        sheet_names = xls.sheet_names
    except Exception as e:
        return [{
            "file_path": xl_path,
            "sheet_name": None,
            "error": f"No se pudo abrir el archivo: {e}"
        }]

    for sh in sheet_names:
        rows.extend(profile_sheet(xl_path, sh))
    return rows

# --------- CLI ---------

def run(root: str, out_csv: str, out_json: Optional[str] = None) -> None:
    files = iter_excel_files(root)
    if not files:
        print(f"[INFO] No se encontraron Excel en: {root}", file=sys.stderr)
        return

    all_rows: List[Dict[str, Any]] = []
    for i, f in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] Perfilando: {f}")
        all_rows.extend(profile_file(f))

    df = pd.DataFrame(all_rows)

    # Ordenar salida de forma agradable
    order_cols = [
        "file_path", "sheet_name", "header_row_index_1based",
        "column_index_1based", "column_name",
        "inferred_type", "probability", "probability_percent",
        "n_samples_used", "null_ratio", "null_percent", "examples", "error"
    ]
    # Asegurar que todas existan
    for c in order_cols:
        if c not in df.columns:
            df[c] = np.nan
    df = df[order_cols]

    # Guardar CSV
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[OK] Reporte CSV guardado en: {out_csv}")

    # Guardar JSON si lo piden
    if out_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(df.fillna("").to_dict(orient="records"), f, ensure_ascii=False, indent=2)
        print(f"[OK] Reporte JSON guardado en: {out_json}")

    # Resumen en consola
    resumen = (
        df.loc[df["error"].isna()]
          .groupby("inferred_type", dropna=True)["column_name"]
          .count()
          .sort_values(ascending=False)
    )
    print("\nResumen por tipo inferido:")
    if len(resumen) == 0:
        print("  (No hay columnas válidas; revisa los errores en el CSV).")
    else:
        for t, cnt in resumen.items():
            print(f"  - {t}: {cnt} columnas")
    err_cnt = df["error"].notna().sum()
    if err_cnt:
        print(f"\nAviso: {err_cnt} filas con 'error'. Revisa el CSV para más detalle.")

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="excel_schema_profiler",
        description="Perfila encabezados y tipos probables en todos los Excel de una ruta."
    )
    p.add_argument("--root", required=True, help="Ruta base a analizar (carpeta).")
    p.add_argument("--out", default="excel_schema_report.csv", help="Ruta del CSV de salida.")
    p.add_argument("--json", default=None, help="Ruta opcional de salida JSON.")
    return p.parse_args(argv)

if __name__ == "__main__":
    args = parse_args()
    run(args.root, args.out, args.json)
