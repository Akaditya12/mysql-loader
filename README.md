# MySQL File Loader & Dumper v2.0

**One-file Python tool that loads CSV/Excel files into MySQL and takes `.sql` dumps.**
Zero setup — it checks everything for you and tells you exactly what to install.

---

## Prerequisites

| Requirement | Minimum | How to check |
|---|---|---|
| **Python** | 3.8+ | `python3 --version` |
| **MySQL Server** | 5.7+ / 8.x | `mysql --version` |
| **pip** | any | `python3 -m pip --version` |

> **That's it.** Python packages (`pandas`, `mysql-connector-python`, `openpyxl`) are auto-detected and auto-installed by the script if missing.

### Quick install per OS

**macOS**
```bash
brew install python3 mysql
brew services start mysql
```

**Ubuntu / Debian**
```bash
sudo apt update
sudo apt install python3 python3-pip mysql-server mysql-client
sudo systemctl start mysql
```

**RHEL / CentOS**
```bash
sudo yum install python3 python3-pip mysql-server mysql
sudo systemctl start mysqld
```

**Windows**
- Python: https://www.python.org/downloads/
- MySQL: https://dev.mysql.com/downloads/installer/

---

## How to Run

```bash
# Full interactive mode — walks you through everything
python3 mysql_loader.py

# Just check if your system is ready (no file needed)
python3 mysql_loader.py --check
```

---

## What It Does — Step by Step

```
╔══════════════════════════════════════════════════════════╗
║      MySQL File Loader & Dumper  v2.0                   ║
║      CSV / Excel  →  MySQL  →  .sql dump                ║
╚══════════════════════════════════════════════════════════╝

── Pre-flight checks ──────────────────────────────────────
  ✓ Python 3.9.6
  ✓ Python packages  (pandas, mysql-connector-python, openpyxl)
  ✓ mysql client  →  /opt/homebrew/bin/mysql
  ✓ mysqldump     →  /opt/homebrew/bin/mysqldump
  ✓ MySQL server is running

── What would you like to do? ──────────────────────────────
  1  Load file into MySQL only
  2  Load file into MySQL  +  take .sql dump
  3  Take .sql dump of an existing database

── MySQL connection ────────────────────────────────────────
  → Host [127.0.0.1]:
  → Port [3306]:
  → User [root]:
  → Password: ********

── Input file ─────────────────────────────────────────────
  → File path: /path/to/data.csv

── Column map (auto-detected) ─────────────────────────────
  #  Original name       MySQL name          Type
  1  Customer ID         customer_id         BIGINT
  2  Full Name           full_name           VARCHAR(47)
  3  Start Date          start_date          DATETIME
  ...

── Summary — review before running ────────────────────────
  File:       data.csv
  Rows:       110,240
  Database:   my_data
  Table:      my_data
  Mode:       Load + Dump
  → Proceed? [yes]:

── Inserting rows ──────────────────────────────────────────
  [████████████████████] 100%  110,240/110,240  18,540 rows/s  done

  ✅  All done!
```

---

## Function Reference

### Pre-flight & Dependency Checks

| Function | What it does |
|---|---|
| `check_python_version()` | Exits if Python < 3.8 |
| `check_pip()` | Checks if `pip` is installed. If not, prints OS-specific fix commands (`ensurepip`, `apt install python3-pip`, etc.) |
| `check_python_packages()` | Checks if `pandas`, `mysql-connector-python`, `openpyxl` are importable. If missing, offers to auto-install, then **verifies** the imports actually work after install |
| `install_python_packages()` | Runs `pip install` with a **120-second timeout** (no infinite hang on bad internet). Shows actual error output on failure. Auto-detects virtualenv vs system Python to use correct pip flags |
| `check_mysql_client()` | Checks if `mysql` binary is on PATH |
| `check_mysqldump()` | Checks if `mysqldump` binary is on PATH |
| `check_mysql_server()` | Pings `127.0.0.1` via `mysqladmin` to confirm the server is running. Prints start commands if it's down |
| `preflight()` | Runs all checks above in order. Used by `--check` flag |

### User Prompts

| Function | What it does |
|---|---|
| `_prompt(label, default, secret)` | Interactive input with default values. Strips drag-and-drop quotes. Uses `getpass` for passwords (hidden input) |
| `ask_file_path()` | Prompts for file path. Validates file exists and extension is `.csv` / `.xlsx` / `.xls`. Loops until valid |
| `ask_connection()` | Prompts for MySQL host, port, user, password with sensible defaults |
| `ask_db_table(filename)` | Prompts for database and table name. **Smart default**: derives names from the filename (e.g., `sales_2024.csv` → db: `sales_2024`, table: `sales_2024`) |
| `ask_mode(dump_available)` | Shows mode menu (Load / Load+Dump / Dump only). Greys out dump options if `mysqldump` isn't installed |

### File Reading & Schema Detection

| Function | What it does |
|---|---|
| `read_file(path)` | Reads CSV or Excel into a pandas DataFrame. For CSV, auto-tries 3 encodings: `utf-8` → `latin-1` → `cp1252` (handles telecom/legacy data with special characters) |
| `sanitize_col(col)` | Cleans column names for MySQL: lowercases, strips special chars (`???`, spaces, symbols), collapses underscores. `"Customer ID???"` → `"customer_id"` |
| `col_to_sql_type(series)` | Infers MySQL column type from data: `int` → `BIGINT`, `float` → `DOUBLE`, datetime → `DATETIME`, strings → `VARCHAR(n)` with padded length |
| `preview_dataframe(df, original_cols)` | Prints a table showing: original column name → cleaned MySQL name → inferred SQL type. Flags renamed columns with `← renamed` |

### MySQL Operations

| Function | What it does |
|---|---|
| `connect_mysql(host, port, user, pwd, retry_password)` | Connects with a **10-second timeout** (no infinite hang on unreachable hosts). Catches specific MySQL errors: **2003** = server not running (shows start command), **1045** = wrong password (retries up to 3 times), **2005** = bad hostname. Returns `(connection, password)` so corrected passwords propagate |
| `setup_database(conn, db, table, df)` | Creates database (utf8mb4), creates table with auto-detected schema, prints the DDL preview |
| `insert_rows(conn, db, table, df)` | Batch inserts in chunks of 2000 rows. Shows live progress bar with `rows/sec` and ETA. Handles NaN/NULL values |
| `run_dump(host, port, user, pwd, db, dir)` | Runs `mysqldump` with `--result-file` (avoids stdout pollution). Timestamped output file: `dbname_20240408_154400.sql` |

### Flow Control

| Function | What it does |
|---|---|
| `confirm_summary(cfg)` | Shows all settings (file, rows, columns, db, table, mode) for review before executing. User must type `yes` to proceed |
| `flow_dump_only(...)` | Dump-only mode: lists existing databases, prompts which to dump, runs `mysqldump` |
| `main()` | Orchestrates the full flow: banner → preflight → mode → connection → file → preview → confirm → execute |

---

## Error Handling Summary

| Problem | What you'll see | What to do |
|---|---|---|
| Python too old | `✗ Python 3.8+ required` | Upgrade Python |
| pip missing | `✗ pip is not installed` + fix commands | Run the suggested command |
| No internet during install | `✗ pip timed out after 120s` | Fix network, or `pip install pandas mysql-connector-python openpyxl` manually |
| pip permission denied | Actual pip error shown (last 10 lines) | Use `sudo` or a virtualenv |
| Install says OK but imports fail | `✗ Install reported success but imports still fail` + shows which Python | Use `python3 -m pip install ...` targeting the right Python |
| MySQL server not running | `✗ MySQL server is not reachable` + start command | `brew services start mysql` / `sudo systemctl start mysql` |
| Wrong password | `✗ Wrong password (attempt 1/3)` → re-prompt | Re-type password (3 chances) |
| Unreachable host | Times out in 10s | Check hostname |
| Bad CSV encoding | Auto-tries utf-8 → latin-1 → cp1252 | Usually self-resolves |
| Garbage column names | Auto-cleaned: `"???ID???"` → `"id"` | Shown in column preview |

---

## Files

| File | Purpose |
|---|---|
| `mysql_loader.py` | The tool — this is the only file you need |
| `test_data.csv` | 10-row sample CSV for testing (optional) |
| `mysql_sump_1.py` | Original v1 script (kept for reference) |

---

## Quick Test

```bash
# 1. Check your system
python3 mysql_loader.py --check

# 2. Run with the test file
python3 mysql_loader.py
#   → Choose mode 1 or 2
#   → Enter MySQL credentials
#   → File path: test_data.csv
#   → Hit enter for defaults
#   → Type "yes" to confirm
```
