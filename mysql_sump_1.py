python#!/usr/bin/env python3

"""

mysql_sump.py

─────────────────────────────────────────────────────────────────────────────

Loads a CSV or Excel file into MySQL and exports a .sql dump.



Supports : .csv, .xlsx, .xls

Platform : macOS, Linux, Windows

Python   : 3.8+



Usage (CLI):

  python3 mysql_sump.py <file> [db_name] [table_name]



Usage (interactive — just run with no args):

  python3 mysql_sump.py

─────────────────────────────────────────────────────────────────────────────

"""



import sys

import os

import re

import subprocess

import getpass

import shutil

import platform

from datetime import datetime



# ── Auto-install dependencies ─────────────────────────────────────────────────

REQUIRED = ["pandas", "mysql-connector-python", "openpyxl"]



def install_packages():

    pip_args = [sys.executable, "-m", "pip", "install", "--quiet"]

    # macOS/Linux Homebrew managed environments need this flag

    if platform.system() in ("Darwin", "Linux"):

        pip_args.append("--break-system-packages")

    pip_args += REQUIRED

    print("📦  Installing required packages...")

    try:

        subprocess.check_call(pip_args)

    except subprocess.CalledProcessError:

        # Fallback: try without the flag (Windows / venv)

        pip_args_fallback = [sys.executable, "-m", "pip", "install", "--quiet"] + REQUIRED

        subprocess.check_call(pip_args_fallback)



try:

    import pandas as pd

    import mysql.connector

    from openpyxl import load_workbook

except ImportError:

    install_packages()

    import pandas as pd

    import mysql.connector



# ── Defaults (override via CLI args or interactive prompts) ───────────────────

DEFAULT_DB    = "mtn_proggie"

DEFAULT_TABLE = "proggie_trade"

DEFAULT_USER  = "root"

DEFAULT_HOST  = "127.0.0.1"

DEFAULT_PORT  = 3306

BATCH_SIZE    = 1000

# ─────────────────────────────────────────────────────────────────────────────



def banner():

    print("=" * 60)

    print("   MySQL File Loader & Dumper")

    print("   CSV / Excel  →  MySQL  →  .sql dump")

    print("=" * 60)



def prompt(label, default=None, secret=False):

    """Interactive prompt with optional default value."""

    hint = f" [{default}]" if default else ""

    display = f"{label}{hint}: "

    if secret:

        val = getpass.getpass(display)

    else:

        val = input(display).strip()

    return val if val else default



def sanitize_col(col):

    """Make column name MySQL-safe, stripping all non-alphanumeric/garbage chars."""

    col = col.strip().lower()

    col = re.sub(r'[^a-z0-9_]', '', col)   # strip ?, ?, special chars

    col = re.sub(r'_+', '_', col)           # collapse multiple underscores

    col = col.strip('_')                    # remove leading/trailing underscores

    return col or "col_unnamed"             # fallback if name becomes empty



def col_to_sql_type(series):

    """Infer MySQL column type from a pandas Series."""

    dtype = str(series.dtype)

    if "datetime" in dtype:

        return "DATETIME"

    if "int" in dtype:

        return "BIGINT"

    if "float" in dtype:

        return "DOUBLE"

    max_len = series.dropna().astype(str).str.len().max()

    max_len = int(max_len) if max_len == max_len else 255   # NaN guard

    max_len = max(max_len + 20, 100)                        # padding

    return f"VARCHAR({max_len})"



def read_file(path):

    """Read CSV or Excel into a DataFrame, handling common encoding issues."""

    ext = os.path.splitext(path)[1].lower()

    print(f"\n[1/4] Reading file: {os.path.basename(path)}")



    if ext == ".csv":

        # Try UTF-8 first, fall back to latin-1 (handles telecom/legacy data)

        for enc in ("utf-8", "latin-1", "cp1252"):

            try:

                df = pd.read_csv(path, encoding=enc)

                print(f"      Encoding: {enc}")

                return df

            except UnicodeDecodeError:

                continue

        sys.exit("❌  Could not decode CSV — unknown encoding.")



    elif ext in (".xlsx", ".xls"):

        return pd.read_excel(path, engine="openpyxl")



    else:

        sys.exit(f"❌  Unsupported file type: '{ext}'  →  use .csv / .xlsx / .xls")



def check_mysqldump():

    """Warn if mysqldump is not on PATH."""

    if not shutil.which("mysqldump"):

        print("⚠️   'mysqldump' not found on PATH — dump step will be skipped.")

        print("     Install MySQL client tools to enable dumps.")

        return False

    return True



def main():

    banner()



    # ── Collect inputs ────────────────────────────────────────────────────────

    if len(sys.argv) > 1:

        input_file = os.path.expanduser(sys.argv[1])

        db_name    = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB

        table_name = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_TABLE

        mysql_host = DEFAULT_HOST

        mysql_port = DEFAULT_PORT

        mysql_user = DEFAULT_USER

        mysql_pass = getpass.getpass(f"MySQL password for {mysql_user}: ")

    else:

        print("\n── Connection ───────────────────────────────────────────")

        input_file = os.path.expanduser(prompt("File path (csv/xlsx)"))

        db_name    = prompt("Database name", DEFAULT_DB)

        table_name = prompt("Table name",    DEFAULT_TABLE)

        mysql_host = prompt("MySQL host",    DEFAULT_HOST)

        mysql_port = int(prompt("MySQL port", str(DEFAULT_PORT)))

        mysql_user = prompt("MySQL user",    DEFAULT_USER)

        mysql_pass = prompt("MySQL password", secret=True)



    dump_dir  = os.path.dirname(os.path.abspath(input_file))

    dump_file = os.path.join(

        dump_dir,

        f"{db_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"

    )



    print(f"\n{'─'*60}")

    print(f"  File      : {input_file}")

    print(f"  Database  : {db_name}")

    print(f"  Table     : {table_name}")

    print(f"  Host      : {mysql_host}:{mysql_port}")

    print(f"  Dump to   : {dump_file}")

    print(f"{'─'*60}")



    # ── Step 1: Read file ─────────────────────────────────────────────────────

    df = read_file(input_file)



    # Sanitize column names

    original_cols = list(df.columns)

    df.columns = [sanitize_col(c) for c in df.columns]

    renamed = {o: n for o, n in zip(original_cols, df.columns) if o != n}

    if renamed:

        print(f"      Renamed columns: {renamed}")



    # Auto-parse datetime columns

    for col in df.columns:

        if "time" in col or "date" in col:

            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)



    print(f"      ✓ {len(df):,} rows × {len(df.columns)} columns")

    print(f"      Columns: {list(df.columns)}")



    # ── Step 2: Build DDL ─────────────────────────────────────────────────────

    col_defs = [f"  `{col}` {col_to_sql_type(df[col])}" for col in df.columns]

    create_sql = (

        f"CREATE TABLE `{table_name}` (\n"

        + ",\n".join(col_defs)

        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    )



    # ── Step 3: Connect & setup DB ────────────────────────────────────────────

    print("\n[2/4] Setting up MySQL database...")

    try:

        conn = mysql.connector.connect(

            host=mysql_host, port=mysql_port,

            user=mysql_user, password=mysql_pass

        )

    except mysql.connector.Error as e:

        sys.exit(f"❌  MySQL connection failed: {e}")



    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")

    cur.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

    cur.execute(f"USE `{db_name}`")

    cur.execute(create_sql)

    conn.commit()

    print(f"      ✓ Database '{db_name}' and table '{table_name}' created")

    print(f"\n      DDL:\n{create_sql}\n")



    # ── Step 4: Insert rows in batches ────────────────────────────────────────

    print("[3/4] Inserting rows...")

    cols_escaped = ", ".join(f"`{c}`" for c in df.columns)

    placeholders = ", ".join(["%s"] * len(df.columns))

    insert_sql   = f"INSERT INTO `{table_name}` ({cols_escaped}) VALUES ({placeholders})"



    rows = [

        tuple(None if pd.isna(v) else v for v in row)

        for row in df.itertuples(index=False)

    ]



    for i in range(0, len(rows), BATCH_SIZE):

        batch = rows[i:i + BATCH_SIZE]

        cur.executemany(insert_sql, batch)

        conn.commit()

        done = min(i + BATCH_SIZE, len(rows))

        pct  = int(done / len(rows) * 100)

        bar  = ("█" * (pct // 5)).ljust(20)

        print(f"      [{bar}] {pct:>3}%  {done:,}/{len(rows):,} rows", end="\r")



    cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")

    count = cur.fetchone()[0]

    print(f"\n      ✓ {count:,} rows inserted")



    cur.close()

    conn.close()



    # ── Step 5: mysqldump ─────────────────────────────────────────────────────

    print("\n[4/4] Taking SQL dump...")

    if check_mysqldump():

        dump_cmd = [

            "mysqldump",

            f"-u{mysql_user}",

            f"-p{mysql_pass}",

            f"-h{mysql_host}",

            f"-P{mysql_port}",

            "--no-tablespaces",

            "--result-file", dump_file,

            db_name

        ]

        result = subprocess.run(dump_cmd, capture_output=True, text=True)

        if result.returncode != 0:

            print(f"⚠️   Dump warning: {result.stderr.strip()}")

        size = os.path.getsize(dump_file)

        print(f"      ✓ Dump saved: {dump_file} ({size // 1024:,}KB)")

    else:

        print("      ⏭  Dump skipped (mysqldump not available)")



    print(f"\n{'='*60}")

    print(f"  ✅  Done!  {count:,} rows loaded into {db_name}.{table_name}")

    print(f"{'='*60}\n")



if __name__ == "__main__":

    main()
