#!/usr/bin/env python3
"""
mysql_loader.py
─────────────────────────────────────────────────────────────────────────────
Load a CSV / Excel file into MySQL and optionally export a .sql dump.

Platform : macOS · Linux · Windows
Python   : 3.8+
─────────────────────────────────────────────────────────────────────────────
"""

import sys
import os
import re
import subprocess
import getpass
import shutil
import platform
import time
import gzip
from datetime import datetime

# ─── Terminal colours (graceful fallback on Windows without colorama) ─────────
try:
    import ctypes
    if platform.system() == "Windows":
        ctypes.windll.kernel32.SetConsoleMode(
            ctypes.windll.kernel32.GetStdHandle(-11), 7
        )
    C = {
        "reset": "\033[0m", "bold": "\033[1m",
        "green": "\033[32m", "red": "\033[31m",
        "yellow": "\033[33m", "cyan": "\033[36m",
        "dim": "\033[2m",
    }
except Exception:
    C = {k: "" for k in ("reset","bold","green","red","yellow","cyan","dim")}

def green(s):  return f"{C['green']}{s}{C['reset']}"
def red(s):    return f"{C['red']}{s}{C['reset']}"
def yellow(s): return f"{C['yellow']}{s}{C['reset']}"
def cyan(s):   return f"{C['cyan']}{s}{C['reset']}"
def bold(s):   return f"{C['bold']}{s}{C['reset']}"
def dim(s):    return f"{C['dim']}{s}{C['reset']}"

def _format_size(n):
    """Convert bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

# ─── Config ───────────────────────────────────────────────────────────────────
BATCH_SIZE   = 2000          # rows per INSERT batch
REQUIRED_PY  = ["pandas", "mysql-connector-python", "openpyxl"]
IMPORT_MAP   = {
    "pandas":                 "pandas",
    "mysql-connector-python": "mysql.connector",
    "openpyxl":               "openpyxl",
}
SUPPORTED_EXT = (".csv", ".tsv", ".xlsx", ".xls")
PIP_TIMEOUT  = 120           # seconds before pip install is killed

# ─────────────────────────────────────────────────────────────────────────────
# BANNER
# ─────────────────────────────────────────────────────────────────────────────
def banner():
    print()
    print(bold("╔══════════════════════════════════════════════════════════╗"))
    print(bold("║") + cyan("      MySQL File Loader & Dumper  v2.0                  ") + bold("║"))
    print(bold("║") + dim("      CSV / Excel  →  MySQL  →  .sql dump               ") + bold("║"))
    print(bold("╚══════════════════════════════════════════════════════════╝"))
    print()


# ─── Discovered MySQL binary paths (populated by preflight) ──────────────────
MYSQL_BINS = {"mysql": None, "mysqldump": None, "mysqladmin": None}


def _find_mysql_binary(name):
    """
    Find a MySQL binary by name. Search order:
      1. PATH  (shutil.which)
      2. Common install directories per platform
      3. Running mysqld process → derive bin/ from its path
    Returns the full path or None.
    """
    # 1. PATH
    found = shutil.which(name)
    if found:
        return found

    # 2. Common install directories
    sys_name = platform.system()
    search_dirs = []

    if sys_name == "Darwin":
        search_dirs = [
            "/usr/local/mysql/bin",                 # Official DMG installer
            "/opt/homebrew/bin",                     # Homebrew Apple Silicon
            "/usr/local/bin",                        # Homebrew Intel
            "/opt/local/bin",                        # MacPorts
            "/Applications/MAMP/Library/bin",        # MAMP
            "/Applications/XAMPP/bin",               # XAMPP
        ]
        # Homebrew cellar (versioned)
        cellar_dirs = ["/opt/homebrew/Cellar/mysql", "/usr/local/Cellar/mysql"]
        for cellar in cellar_dirs:
            if os.path.isdir(cellar):
                for ver in sorted(os.listdir(cellar), reverse=True):
                    search_dirs.append(os.path.join(cellar, ver, "bin"))
        # Anaconda / Miniconda
        for conda_root in [os.path.expanduser("~/anaconda3"), os.path.expanduser("~/miniconda3"),
                           "/opt/anaconda3", "/opt/miniconda3"]:
            if os.path.isdir(os.path.join(conda_root, "bin")):
                search_dirs.append(os.path.join(conda_root, "bin"))

    elif sys_name == "Linux":
        search_dirs = [
            "/usr/bin",
            "/usr/sbin",
            "/usr/local/bin",
            "/usr/local/mysql/bin",                  # Tarball install
            "/opt/mysql/bin",
            "/snap/bin",
        ]

    elif sys_name == "Windows":
        # Scan Program Files for MySQL Server
        for pf in [os.environ.get("ProgramFiles", r"C:\Program Files"),
                    os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")]:
            mysql_root = os.path.join(pf, "MySQL")
            if os.path.isdir(mysql_root):
                for entry in sorted(os.listdir(mysql_root), reverse=True):
                    search_dirs.append(os.path.join(mysql_root, entry, "bin"))

    for d in search_dirs:
        candidate = os.path.join(d, name)
        # On Windows, check .exe too
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
        if sys_name == "Windows" and os.path.isfile(candidate + ".exe"):
            return candidate + ".exe"

    # 3. Detect from running mysqld process (macOS / Linux only)
    if sys_name in ("Darwin", "Linux"):
        try:
            result = subprocess.run(
                ["ps", "-eo", "args"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if "mysqld" in line and "/" in line:
                    # Extract the binary path, then look in same dir
                    parts = line.strip().split()
                    for part in parts:
                        if "/mysqld" in part:
                            bin_dir = os.path.dirname(part)
                            candidate = os.path.join(bin_dir, name)
                            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                                return candidate
                            break
        except Exception:
            pass

    return None


def _discover_mysql_binaries():
    """Find mysql, mysqldump, and mysqladmin. Store in MYSQL_BINS."""
    for name in MYSQL_BINS:
        MYSQL_BINS[name] = _find_mysql_binary(name)


# ─────────────────────────────────────────────────────────────────────────────
# PRE-FLIGHT CHECKS
# ─────────────────────────────────────────────────────────────────────────────
def check_python_version():
    if sys.version_info < (3, 8):
        print(red("  ✗ Python 3.8+ required. You have: ") + sys.version)
        sys.exit(1)
    print(green("  ✓") + f" Python {sys.version.split()[0]}")


def _in_virtualenv():
    """Detect if running inside a virtualenv / venv / conda env."""
    return (
        sys.prefix != sys.base_prefix
        or os.environ.get("VIRTUAL_ENV")
        or os.environ.get("CONDA_DEFAULT_ENV")
    )


def check_pip():
    """Verify pip is available. Returns True if usable, False otherwise."""
    result = subprocess.run(
        [sys.executable, "-m", "pip", "--version"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        ver_line = result.stdout.strip().split("\n")[0]
        print(green("  ✓") + f" pip  →  {ver_line}")
        return True

    print(red("  ✗ pip is not installed"))
    sys_name = platform.system()
    print(dim("    Fix with one of:"))
    if sys_name == "Darwin":
        print(dim("      python3 -m ensurepip --upgrade"))
        print(dim("      brew install python3"))
    elif sys_name == "Linux":
        print(dim("      python3 -m ensurepip --upgrade"))
        print(dim("      sudo apt install python3-pip   (Debian/Ubuntu)"))
        print(dim("      sudo yum install python3-pip   (RHEL/CentOS)"))
    else:
        print(dim("      python -m ensurepip --upgrade"))
    return False


def check_mysql_client():
    mysql_bin = MYSQL_BINS["mysql"]
    if mysql_bin:
        on_path = shutil.which("mysql") == mysql_bin
        label = mysql_bin if on_path else f"{mysql_bin}  {dim('(not on PATH — auto-detected)')}"
        print(green("  ✓") + f" mysql client  →  {label}")
        return True
    print(yellow("  ⚠") + " mysql client not found")
    _print_mysql_install_hint()
    return False


def check_mysqldump():
    dump_bin = MYSQL_BINS["mysqldump"]
    if dump_bin:
        on_path = shutil.which("mysqldump") == dump_bin
        label = dump_bin if on_path else f"{dump_bin}  {dim('(not on PATH — auto-detected)')}"
        print(green("  ✓") + f" mysqldump     →  {label}")
        return True
    print(yellow("  ⚠") + " mysqldump not found  (dump will be unavailable)")
    _print_mysql_install_hint()
    return False


def check_mysql_server():
    """Quick probe: is the MySQL server reachable on localhost?"""
    mysqladmin = MYSQL_BINS["mysqladmin"]
    if not mysqladmin:
        # No mysqladmin binary, but check if we can detect a running process
        if platform.system() in ("Darwin", "Linux"):
            try:
                result = subprocess.run(
                    ["pgrep", "-f", "mysqld"], capture_output=True, timeout=3,
                )
                if result.returncode == 0:
                    print(green("  ✓") + " MySQL server is running  " + dim("(detected via process)"))
                    return True
            except Exception:
                pass
        print(yellow("  ⚠") + " Cannot verify MySQL server status (mysqladmin not found)")
        return None

    result = subprocess.run(
        [mysqladmin, "ping", "-h", "127.0.0.1", "--connect-timeout=3"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(green("  ✓") + " MySQL server is running")
        return True
    # Access denied still means the server IS running
    if "Access denied" in (result.stderr or ""):
        print(green("  ✓") + " MySQL server is running  " + dim("(auth required)"))
        return True
    print(yellow("  ⚠") + " MySQL server is not responding on 127.0.0.1")
    sys_name = platform.system()
    if sys_name == "Darwin":
        print(dim("    Start with:  brew services start mysql"))
        print(dim("              or sudo /usr/local/mysql/support-files/mysql.server start"))
    elif sys_name == "Linux":
        print(dim("    Start with:  sudo systemctl start mysql"))
        print(dim("              or sudo service mysql start"))
    else:
        print(dim("    Start the MySQL service from Services panel or:"))
        print(dim("      net start MySQL"))
    return False


def _print_mysql_install_hint():
    sys_name = platform.system()
    if sys_name == "Darwin":
        print(dim("    Install:  brew install mysql"))
    elif sys_name == "Linux":
        print(dim("    Install:  sudo apt install mysql-server mysql-client  (Debian/Ubuntu)"))
        print(dim("              sudo yum install mysql-server mysql         (RHEL/CentOS)"))
    elif sys_name == "Windows":
        print(dim("    Install:  https://dev.mysql.com/downloads/installer/"))


def _get_pip_flags():
    """Build the right pip install flags for the current environment."""
    flags = []
    if not _in_virtualenv() and platform.system() in ("Darwin", "Linux"):
        flags.append("--break-system-packages")
    return flags


def install_python_packages(packages=None):
    """Install packages via pip with timeout and visible error output."""
    packages = packages or REQUIRED_PY
    pip_cmd = [sys.executable, "-m", "pip", "install"] + _get_pip_flags() + packages

    env_label = "virtualenv" if _in_virtualenv() else "system"
    print(dim(f"    Installing into {env_label} Python: ") + ", ".join(packages))

    try:
        result = subprocess.run(
            pip_cmd,
            capture_output=True, text=True,
            timeout=PIP_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(red(f"  ✗ pip timed out after {PIP_TIMEOUT}s"))
        print(yellow("    Check your internet connection and try again."))
        print(dim(f"    Or install manually:  pip install {' '.join(packages)}"))
        return False

    if result.returncode != 0:
        print(red("  ✗ pip install failed:"))
        # Show the last 10 lines of stderr (most useful part)
        stderr_lines = (result.stderr or "").strip().splitlines()
        for line in stderr_lines[-10:]:
            print(red(f"    {line}"))
        print()
        print(dim(f"    Install manually:  pip install {' '.join(packages)}"))
        return False

    return True


def _check_imports():
    """Try importing each required package. Returns list of missing pip names."""
    import importlib
    missing = []
    for pkg in REQUIRED_PY:
        import_name = IMPORT_MAP.get(pkg, pkg)
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(pkg)
    return missing


def check_python_packages():
    """Check required packages, offer to install, and verify after install."""
    missing = _check_imports()

    if not missing:
        print(green("  ✓") + f" Python packages  ({', '.join(REQUIRED_PY)})")
        return True

    print(yellow("  ⚠") + f" Missing Python packages: " + bold(", ".join(missing)))
    answer = _prompt("  Install them now?", default="y")
    if answer.lower() not in ("y", "yes", ""):
        print(red("  ✗ Cannot continue without: ") + ", ".join(missing))
        print(dim(f"    Install manually:  pip install {' '.join(missing)}"))
        sys.exit(1)

    # Check pip first
    if not check_pip():
        print(red("  ✗ Cannot install packages without pip. Fix pip first, then re-run."))
        sys.exit(1)

    # Attempt install
    if not install_python_packages(missing):
        sys.exit(1)

    # VERIFY — re-check imports to confirm they actually work now
    still_missing = _check_imports()
    if still_missing:
        print(red("  ✗ Install reported success but imports still fail:"))
        for pkg in still_missing:
            print(red(f"    - {pkg}"))
        print()
        print(yellow("    This can happen when pip installs to a different Python."))
        print(dim(f"    Your Python: {sys.executable}"))
        print(dim(f"    Try:  {sys.executable} -m pip install {' '.join(still_missing)}"))
        sys.exit(1)

    print(green("  ✓") + " Packages installed and verified")
    return True


def preflight():
    print(bold("\n── Pre-flight checks ──────────────────────────────────────"))
    check_python_version()
    check_python_packages()
    print(dim("  Scanning for MySQL binaries..."))
    _discover_mysql_binaries()
    mysql_ok = check_mysql_client()
    dump_ok  = check_mysqldump()
    check_mysql_server()

    if not mysql_ok:
        print()
        print(yellow("  MySQL client tools are required. Install them and re-run."))
        ans = _prompt("  Continue anyway?", default="n")
        if ans.lower() not in ("y", "yes"):
            sys.exit(1)

    print()
    return mysql_ok, dump_ok


# ─────────────────────────────────────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────────────────────────────────────
def _prompt(label, default=None, secret=False, required=False):
    hint   = f" {dim(f'[{default}]')}" if default is not None else ""
    prefix = f"  {cyan('→')} {label}{hint}: "
    while True:
        if secret:
            val = getpass.getpass(prefix)
        else:
            val = input(prefix).strip()
        # Strip surrounding quotes (drag-and-drop on macOS adds them)
        val = val.strip("'\"")
        if val:
            return val
        if default is not None:
            return str(default)
        if required:
            print(red("    Value required, please try again."))
        else:
            return ""


def ask_file_path():
    print(bold("\n── Input file ─────────────────────────────────────────────"))
    print(dim("  Supported: .csv  .tsv  .xlsx  .xls"))
    print(dim("  Tip: drag & drop the file into the terminal\n"))
    while True:
        path = _prompt("File path", required=True)
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            print(red(f"    File not found: {path}"))
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext not in SUPPORTED_EXT:
            print(red(f"    Unsupported type '{ext}' — use {SUPPORTED_EXT}"))
            continue
        return path


def ask_connection():
    print(bold("\n── MySQL connection ────────────────────────────────────────"))
    host = _prompt("Host",     default="127.0.0.1")
    port = _prompt("Port",     default="3306")
    user = _prompt("User",     default="root")
    pwd  = _prompt("Password", secret=True)
    return host, int(port), user, pwd


def ask_db_table(filename, host=None, port=None, user=None, pwd=None):
    base       = os.path.splitext(os.path.basename(filename))[0]
    safe_base  = re.sub(r'[^a-z0-9]', '_', base.lower()).strip('_') or "import_db"
    print(bold("\n── Database / table ────────────────────────────────────────"))

    # Show existing databases if connection info is available
    if host and pwd:
        try:
            import mysql.connector
            tmp = mysql.connector.connect(host=host, port=port, user=user,
                                          password=pwd, connection_timeout=5)
            cur = tmp.cursor()
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN "
                "('information_schema','performance_schema','mysql','sys')"
            )
            dbs = [row[0] for row in cur.fetchall()]
            cur.close()
            tmp.close()
            if dbs:
                print(f"  {dim('Existing databases:')}  " + "  ".join(cyan(d) for d in dbs))
        except Exception:
            pass

    db    = _prompt("Database name", default=safe_base)
    table = _prompt("Table name",    default=safe_base)
    return db, table


def ask_mode(dump_available, is_remote=False, mysql_client_ok=True):
    print(bold("\n── What would you like to do? ──────────────────────────────"))
    print(f"  {cyan('1')}  Load CSV / TSV / Excel into MySQL")
    if dump_available:
        print(f"  {cyan('2')}  Load file into MySQL  +  take .sql dump")
        print(f"  {cyan('3')}  Take .sql dump of an existing database")
        remote_hint = f"  {yellow('← recommended for remote')}" if is_remote else ""
        print(f"  {cyan('4')}  Backup databases (selective, compressed .sql.gz){remote_hint}")
    else:
        print(dim("  2  Load + Dump  (mysqldump not available)"))
        print(dim("  3  Dump only    (mysqldump not available)"))
        print(dim("  4  Backup       (mysqldump not available)"))
    if mysql_client_ok:
        print(f"  {cyan('5')}  Restore a .sql / .sql.gz dump into MySQL")
    else:
        print(dim("  5  Restore .sql  (mysql client not available)"))
    print()
    default = "4" if is_remote and dump_available else ("2" if dump_available else "1")
    while True:
        choice = _prompt("Choice", default=default)
        if choice in ("1", "2", "3", "4", "5"):
            if choice in ("2", "3", "4") and not dump_available:
                print(yellow("    mysqldump is not installed — pick another option."))
                continue
            if choice == "5" and not mysql_client_ok:
                print(yellow("    mysql client is not installed — pick another option."))
                continue
            return choice
        print(red("    Enter 1, 2, 3, 4, or 5"))


def parse_selection(choice_str, max_count):
    """Parse '1,3,5-8,A' into a sorted list of 0-based indices."""
    if choice_str.strip().upper() == "A":
        return list(range(max_count))
    indices = set()
    for part in choice_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-", 1)
                for n in range(int(start), int(end) + 1):
                    if 1 <= n <= max_count:
                        indices.add(n - 1)
            except ValueError:
                print(yellow(f"    Skipping invalid range: {part}"))
        elif part.isdigit():
            n = int(part)
            if 1 <= n <= max_count:
                indices.add(n - 1)
            else:
                print(yellow(f"    Skipping out-of-range: {part}"))
        elif part:
            print(yellow(f"    Skipping invalid: {part}"))
    return sorted(indices)


def ask_dump_output_dir(default_dir):
    path = _prompt("Dump output directory", default=default_dir)
    path = os.path.expanduser(path)
    os.makedirs(path, exist_ok=True)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# FILE READING
# ─────────────────────────────────────────────────────────────────────────────
def read_file(path):
    """
    Read a CSV/TSV/Excel file. For Excel files with multiple sheets,
    returns a list of (sheet_name, DataFrame) tuples.
    For CSV/TSV returns a single-item list: [("filename", DataFrame)].
    """
    import pandas as pd
    ext = os.path.splitext(path)[1].lower()
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"\n  {dim('File size:')} {size_mb:.1f} MB")

    if ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        label = "TSV" if ext == ".tsv" else "CSV"
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
                print(f"  {dim('Format:')}   {label}  {dim('Encoding:')} {enc}")
                return [df]
            except UnicodeDecodeError:
                continue
        print(red(f"✗ Could not decode {label}. Tried utf-8, latin-1, cp1252."))
        sys.exit(1)
    else:
        # Excel — check for multiple sheets
        xls = pd.ExcelFile(path, engine="openpyxl")
        sheet_names = xls.sheet_names

        if len(sheet_names) == 1:
            df = pd.read_excel(xls, sheet_name=sheet_names[0])
            print(f"  {dim('Format:')}   Excel  {dim('Sheet:')} {sheet_names[0]}")
            return [df]

        # Multiple sheets found — let user choose
        print(f"  {dim('Format:')}   Excel  {dim('Sheets found:')} {len(sheet_names)}")
        print()
        for i, name in enumerate(sheet_names, 1):
            # Quick row count peek
            df_peek = pd.read_excel(xls, sheet_name=name, nrows=0)
            col_count = len(df_peek.columns)
            print(f"    {cyan(str(i))}  {name}  {dim(f'({col_count} columns)')}")
        print(f"    {cyan('A')}  All sheets")
        print()

        choice = _prompt("  Which sheets to load? (comma-separated numbers, or A for all)", default="A")

        if choice.upper() == "A":
            selected = sheet_names
        else:
            indices = []
            for part in choice.split(","):
                part = part.strip()
                if part.isdigit() and 1 <= int(part) <= len(sheet_names):
                    indices.append(int(part) - 1)
                else:
                    print(yellow(f"    Skipping invalid choice: {part}"))
            selected = [sheet_names[i] for i in indices]
            if not selected:
                print(yellow("    No valid sheets selected, loading all."))
                selected = sheet_names

        results = []
        for name in selected:
            df = pd.read_excel(xls, sheet_name=name)
            print(f"  {green('✓')} Sheet {bold(name)}: {len(df):,} rows × {len(df.columns)} columns")
            results.append((name, df))
        return results


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN / SCHEMA HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def sanitize_col(col):
    col = str(col).strip().lower()
    col = re.sub(r'[^a-z0-9_]', '', col)
    col = re.sub(r'_+', '_', col).strip('_')
    return col or "col_unnamed"


def col_to_sql_type(series):
    import pandas as pd
    dtype = str(series.dtype)
    if "datetime" in dtype:
        return "DATETIME"
    if "int" in dtype:
        return "BIGINT"
    if "float" in dtype:
        return "DOUBLE"
    max_len = series.dropna().astype(str).str.len().max()
    max_len = int(max_len) if (max_len == max_len) else 255
    return f"VARCHAR({min(max_len + 30, 65535)})"


def preview_dataframe(df, original_cols):
    import pandas as pd
    print(f"\n  {green('✓')} {len(df):,} rows  ×  {len(df.columns)} columns detected")
    print(f"\n  {bold('Column map:')}")
    print(f"  {'#':<4} {'Original name':<30} {'MySQL name':<30} {'Type'}")
    print(f"  {dim('─'*80)}")
    for i, (orig, new) in enumerate(zip(original_cols, df.columns), 1):
        sql_type = col_to_sql_type(df[new])
        changed  = yellow(" ← renamed") if orig != new else ""
        print(f"  {dim(str(i)):<4} {dim(orig):<30} {cyan(new):<30} {dim(sql_type)}{changed}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MYSQL OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
def connect_mysql(host, port, user, pwd, db=None, retry_password=False):
    """
    Connect to MySQL. Returns (connection, password) tuple.
    password is returned so callers get the corrected value after retries.
    Returns (None, pwd) on failure.
    """
    import mysql.connector
    kwargs = dict(host=host, port=port, user=user, password=pwd,
                  connection_timeout=10)
    if db:
        kwargs["database"] = db

    max_attempts = 3 if retry_password else 1
    for attempt in range(1, max_attempts + 1):
        try:
            conn = mysql.connector.connect(**kwargs)
            return conn, kwargs["password"]
        except mysql.connector.Error as e:
            code = e.errno if hasattr(e, 'errno') else None

            if code == 2003:  # Can't connect to server
                print(red(f"  ✗ MySQL server is not reachable at {host}:{port}"))
                sys_name = platform.system()
                if sys_name == "Darwin":
                    print(dim("    Start with:  brew services start mysql"))
                elif sys_name == "Linux":
                    print(dim("    Start with:  sudo systemctl start mysql"))
                else:
                    print(dim("    Start the MySQL service and try again."))
                return None, pwd

            elif code == 1045:  # Access denied
                if attempt < max_attempts:
                    print(yellow(f"  ✗ Wrong password for '{user}'@'{host}'  (attempt {attempt}/{max_attempts})"))
                    kwargs["password"] = getpass.getpass(f"  {cyan('→')} Re-enter password: ")
                    continue
                else:
                    print(red(f"  ✗ Access denied for '{user}'@'{host}' after {max_attempts} attempts"))
                    print(dim(f"    Check your MySQL credentials and try again."))
                    return None, pwd

            elif code == 2005:  # Unknown host
                print(red(f"  ✗ Cannot resolve host: {host}"))
                print(dim(f"    Check the hostname and try again."))
                return None, pwd

            else:
                print(red(f"  ✗ MySQL error ({code}): {e}"))
                return None, pwd
    return None, pwd


def setup_database(conn, db_name, table_name, df):
    import pandas as pd
    cur = conn.cursor()

    # Check if database already exists
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name = %s", (db_name,)
    )
    db_exists = cur.fetchone() is not None

    if db_exists:
        print(f"\n  {yellow('⚠')} Database {bold(db_name)} already exists")

        # Show existing tables
        cur.execute(f"USE `{db_name}`")
        cur.execute("SHOW TABLES")
        existing_tables = [row[0] for row in cur.fetchall()]
        if existing_tables:
            print(f"  {dim('Existing tables:')}  " + "  ".join(cyan(t) for t in existing_tables))

        ans = _prompt("  Use existing database? (yes = add table, no = recreate from scratch)", default="yes")
        if ans.lower() not in ("y", "yes"):
            cur.execute(f"DROP DATABASE `{db_name}`")
            cur.execute(
                f"CREATE DATABASE `{db_name}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            cur.execute(f"USE `{db_name}`")
            print(f"  {green('✓')} Database {bold(db_name)} recreated (clean)")
        else:
            print(f"  {green('✓')} Using existing database {bold(db_name)}")

            # Check if table name conflicts
            if table_name in existing_tables:
                # Show existing row count
                cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                existing_count = cur.fetchone()[0]
                print(f"  {yellow('⚠')} Table {bold(table_name)} already exists  ({existing_count:,} rows)")
                print(f"    {cyan('1')}  Append — add rows to existing table (keep current data)")
                print(f"    {cyan('2')}  Overwrite — drop table & recreate from scratch")
                print(f"    {cyan('3')}  New name — create a different table")
                tbl_ans = _prompt("  Choice", default="1")

                if tbl_ans == "1":
                    # Append mode: skip CREATE TABLE, just insert later
                    conn.commit()
                    cur.close()
                    print(f"  {green('✓')} Will append to {bold(table_name)}  ({existing_count:,} existing rows kept)")
                    return None, table_name

                elif tbl_ans == "2":
                    cur.execute(f"DROP TABLE `{table_name}`")
                    print(f"  {dim('Dropped old table')} {table_name}")

                else:
                    while table_name in existing_tables:
                        table_name = _prompt("  New table name", required=True)
                        if table_name in existing_tables:
                            print(yellow(f"    '{table_name}' also exists. Try another name."))
                    print(f"  {green('✓')} Will create table {bold(table_name)}")
    else:
        cur.execute(
            f"CREATE DATABASE `{db_name}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cur.execute(f"USE `{db_name}`")
        print(f"\n  {green('✓')} Database {bold(db_name)} created")

    # Create the table
    col_defs = [f"  `{col}` {col_to_sql_type(df[col])}" for col in df.columns]
    create_sql = (
        f"CREATE TABLE `{table_name}` (\n"
        + ",\n".join(col_defs)
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )
    cur.execute(create_sql)
    conn.commit()
    cur.close()

    print(f"  {green('✓')} Table    {bold(table_name)} created")
    print(f"\n  {dim('DDL preview:')}")
    for line in create_sql.splitlines():
        print(f"  {dim(line)}")
    return create_sql, table_name


def insert_rows(conn, db_name, table_name, df):
    import pandas as pd
    cur = conn.cursor()
    cur.execute(f"USE `{db_name}`")

    cols_esc    = ", ".join(f"`{c}`" for c in df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql  = f"INSERT INTO `{table_name}` ({cols_esc}) VALUES ({placeholders})"

    rows = [
        tuple(None if (hasattr(v, '__float__') and v != v) or
              (hasattr(pd, 'isna') and pd.isna(v)) else v
              for v in row)
        for row in df.itertuples(index=False)
    ]

    total     = len(rows)
    t_start   = time.time()
    inserted  = 0

    print()
    for i in range(0, total, BATCH_SIZE):
        batch    = rows[i : i + BATCH_SIZE]
        cur.executemany(insert_sql, batch)
        conn.commit()
        inserted = min(i + BATCH_SIZE, total)
        elapsed  = time.time() - t_start
        rps      = int(inserted / elapsed) if elapsed > 0 else 0
        pct      = int(inserted / total * 100)
        bar      = (green("█") * (pct // 5)).ljust(20 + len(C["green"]) + len(C["reset"]))
        eta      = int((total - inserted) / rps) if rps > 0 else 0
        eta_str  = f"ETA {eta}s" if eta > 0 else "done"
        print(
            f"  [{bar}] {pct:>3}%  {inserted:,}/{total:,}  "
            f"{dim(str(rps) + ' rows/s')}  {dim(eta_str)}",
            end="\r"
        )

    cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cur.fetchone()[0]
    elapsed = time.time() - t_start
    print(
        f"  [{green('█' * 20)}] 100%  {count:,}/{total:,}  "
        f"{dim(str(int(count/elapsed)) + ' rows/s')}  {green('done')}    "
    )
    cur.close()
    return count


def run_dump(host, port, user, pwd, db_name, output_dir):
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_file = os.path.join(output_dir, f"{db_name}_{ts}.sql")

    mysqldump_bin = MYSQL_BINS.get("mysqldump") or "mysqldump"
    cmd = [
        mysqldump_bin,
        f"-u{user}",
        f"-p{pwd}",
        f"-h{host}",
        f"-P{str(port)}",
        "--no-tablespaces",
        "--result-file", dump_file,
        db_name,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        # mysqldump often prints warnings even on success
        if "Warning" in stderr or "warning" in stderr:
            print(yellow(f"  ⚠  mysqldump warning: {stderr}"))
        else:
            print(red(f"  ✗  mysqldump error: {stderr}"))
            return None

    if not os.path.isfile(dump_file):
        print(red("  ✗  Dump file was not created."))
        return None

    size_kb = os.path.getsize(dump_file) // 1024
    print(f"  {green('✓')} Dump saved  →  {bold(dump_file)}  {dim(f'({size_kb:,} KB)')}")
    return dump_file


def run_dump_compressed(host, port, user, pwd, db_name, output_dir):
    """
    Dump a database to a compressed .sql.gz file using safe read-only flags.
    Streams mysqldump stdout through Python gzip (detects errors properly).
    Returns (file_path, compressed_size) on success, (None, 0) on failure.
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    gz_file  = os.path.join(output_dir, f"{db_name}_{ts}.sql.gz")

    mysqldump_bin = MYSQL_BINS.get("mysqldump") or "mysqldump"
    cmd = [
        mysqldump_bin,
        f"-u{user}",
        f"-p{pwd}",
        f"-h{host}",
        f"-P{str(port)}",
        "--single-transaction",     # consistent snapshot, no locks on InnoDB
        "--quick",                  # row-by-row fetch, no client memory bloat
        "--lock-tables=false",      # no LOCK TABLES even for MyISAM
        "--set-gtid-purged=OFF",    # avoid GTID errors on restricted users
        "--no-tablespaces",         # avoid PROCESS privilege requirement
        db_name,
    ]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with gzip.open(gz_file, "wb", compresslevel=6) as gz:
            while True:
                chunk = proc.stdout.read(65536)  # 64KB chunks
                if not chunk:
                    break
                gz.write(chunk)

        proc.wait()
        stderr = proc.stderr.read().decode("utf-8", errors="replace").strip()

        if proc.returncode != 0:
            # Filter out harmless warnings
            if "Warning" in stderr or "warning" in stderr:
                pass  # warnings are OK
            else:
                # Real error — clean up partial file
                if os.path.isfile(gz_file):
                    os.remove(gz_file)
                return None, stderr

        if not os.path.isfile(gz_file):
            return None, "Dump file was not created"

        size = os.path.getsize(gz_file)
        return gz_file, size

    except Exception as e:
        # Clean up partial file on any error
        if os.path.isfile(gz_file):
            os.remove(gz_file)
        return None, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIRM SCREEN
# ─────────────────────────────────────────────────────────────────────────────
def confirm_summary(cfg):
    host_str = f"{cfg.get('host')}:{cfg.get('port')}"
    is_remote = cfg.get("host") not in ("127.0.0.1", "localhost", "::1")
    if is_remote:
        host_str = yellow(f"{host_str}  ⚠ REMOTE SERVER")

    print(bold("\n── Summary — review before running ────────────────────────"))
    rows = [
        ("File",      cfg.get("file", "—")),
        ("Rows",      cfg.get("rows", "—")),
        ("Columns",   cfg.get("cols", "—")),
        ("Database",  cfg.get("db",   "—")),
        ("Table",     cfg.get("table","—")),
        ("Host",      host_str),
        ("User",      cfg.get("user", "—")),
        ("Mode",      cfg.get("mode", "—")),
    ]
    if cfg.get("dump_dir"):
        rows.append(("Dump dir", cfg["dump_dir"]))
    for k, v in rows:
        print(f"  {cyan(k+':'): <18} {v}")

    print()
    ans = _prompt("Proceed? (yes/no)", default="yes")
    return ans.lower() in ("y", "yes")


# ─────────────────────────────────────────────────────────────────────────────
# DUMP-ONLY FLOW (no file load)
# ─────────────────────────────────────────────────────────────────────────────
def flow_dump_only(host, port, user, pwd, dump_available):
    if not dump_available:
        print(red("✗ mysqldump is not installed. Cannot take a dump."))
        sys.exit(1)

    print(bold("\n── Dump an existing database ───────────────────────────────"))
    # List available databases
    conn, _ = connect_mysql(host, port, user, pwd)
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT IN "
        "('information_schema','performance_schema','mysql','sys')"
    )
    dbs = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if dbs:
        print(f"  {dim('Available databases:')}  " + "  ".join(cyan(d) for d in dbs))
    else:
        print(yellow("  No user databases found."))

    db_name    = _prompt("Database to dump", required=True)
    dump_dir   = ask_dump_output_dir(os.path.expanduser("~"))

    print(bold("\n── Taking dump ─────────────────────────────────────────────"))
    dump_file  = run_dump(host, port, user, pwd, db_name, dump_dir)
    return dump_file


def flow_selective_backup(host, port, user, pwd, dump_available):
    """Mode 4: Selective database backup with .sql.gz compression."""
    if not dump_available:
        print(red("  ✗ mysqldump is not installed. Cannot take backups."))
        sys.exit(1)

    print(bold("\n── Selective database backup ───────────────────────────────"))
    print(dim("  Safe mode: read-only, no locks on the server\n"))

    # Connect and list databases with sizes
    conn, _ = connect_mysql(host, port, user, pwd)
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    cur.execute(
        "SELECT table_schema, "
        "  COALESCE(SUM(data_length + index_length), 0) AS size_bytes "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN "
        "  ('information_schema','performance_schema','mysql','sys') "
        "GROUP BY table_schema "
        "ORDER BY table_schema"
    )
    db_list = [(row[0], int(row[1])) for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not db_list:
        print(yellow("  No user databases found."))
        return []

    # Display numbered list with sizes
    for i, (name, size) in enumerate(db_list, 1):
        print(f"    {cyan(str(i)):>6}  {name:<35} {dim(f'({_format_size(size)})')}")
    print(f"    {cyan('A'):>6}  All databases")
    print()

    # Selection
    choice = _prompt("Select databases (numbers, ranges 1-3, or A for all)", default="A")
    selected_idx = parse_selection(choice, len(db_list))
    if not selected_idx:
        print(yellow("  No databases selected."))
        return []

    selected = [db_list[i] for i in selected_idx]
    total_est = sum(s for _, s in selected)

    # Output directory
    dump_dir = ask_dump_output_dir(os.path.join(os.path.expanduser("~"), "mysql_backups"))

    # Confirm
    print(bold("\n── Backup summary ─────────────────────────────────────────"))
    print(f"  {cyan('Server:')}     {host}")
    print(f"  {cyan('Databases:')}  {len(selected)}")
    for name, size in selected:
        print(f"    - {name}  {dim(f'(~{_format_size(size)})')}")
    print(f"  {cyan('Est. size:')}  ~{_format_size(total_est)}  {dim('(uncompressed)')}")
    print(f"  {cyan('Output:')}     {dump_dir}")
    print(f"  {cyan('Format:')}     .sql.gz  {dim('(gzip compressed)')}")
    print(f"  {cyan('Safety:')}     {dim('--single-transaction --quick --lock-tables=false')}")
    print()
    ans = _prompt("Proceed with backup?", default="yes")
    if ans.lower() not in ("y", "yes"):
        print(yellow("  Aborted."))
        return []

    # Dump loop
    print(bold("\n── Backing up ─────────────────────────────────────────────"))
    results  = []   # (db_name, file_path, size)
    failures = []   # (db_name, error)
    current_gz = None

    try:
        for i, (db_name, _) in enumerate(selected, 1):
            print(f"  Dumping {i}/{len(selected)}: {bold(db_name)}...", end=" ", flush=True)
            current_gz = os.path.join(dump_dir,
                f"{db_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql.gz")

            gz_path, result = run_dump_compressed(host, port, user, pwd, db_name, dump_dir)
            current_gz = None  # no longer in-progress

            if gz_path:
                print(green("done") + f"  {dim(f'({_format_size(result)})')}")
                results.append((db_name, gz_path, result))
            else:
                print(red("failed"))
                print(red(f"    {result}"))
                failures.append((db_name, result))

    except KeyboardInterrupt:
        print(f"\n\n{yellow('  Interrupted — cleaning up...')}")
        if current_gz and os.path.isfile(current_gz):
            os.remove(current_gz)
            print(dim(f"    Removed partial: {current_gz}"))

    # Summary
    print(bold(f"\n{'═'*60}"))
    if results:
        total_compressed = sum(s for _, _, s in results)
        print(green("  ✅  Backup complete!"))
        print(f"  {cyan('Output:')}  {dump_dir}")
        for db_name, gz_path, size in results:
            fname = os.path.basename(gz_path)
            print(f"    {fname:<50} {dim(_format_size(size))}")
        print(f"  {cyan('Total:')}   {_format_size(total_compressed)} compressed")
    if failures:
        print(yellow(f"\n  ⚠  {len(failures)} database(s) failed:"))
        for db_name, err in failures:
            print(f"    {red(db_name)}: {err}")
    if not results and not failures:
        print(yellow("  No backups completed."))
    print(bold(f"{'═'*60}\n"))
    return results


def _peek_dump_database(path):
    """
    Peek into a .sql or .sql.gz file to find the embedded database name.
    Returns the database name from the first USE `dbname` statement, or None.
    """
    try:
        if path.endswith(".sql.gz"):
            f = gzip.open(path, "rb")
        else:
            f = open(path, "rb")
        with f:
            head = f.read(8192).decode("utf-8", errors="replace")
        for line in head.splitlines():
            line = line.strip()
            match = re.match(r'^USE\s+`([^`]+)`', line, re.IGNORECASE)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None


def _rewrite_db_in_stream(data, old_db, new_db):
    """Replace database references in a SQL dump chunk (bytes)."""
    old_use    = f"USE `{old_db}`".encode()
    new_use    = f"USE `{new_db}`".encode()
    old_create = f"`{old_db}`".encode()
    new_create = f"`{new_db}`".encode()
    data = data.replace(old_use, new_use)
    # Only replace CREATE DATABASE lines (not random occurrences in data)
    data = data.replace(
        f"CREATE DATABASE".encode() + b" /*!32312 IF NOT EXISTS*/ " + old_create,
        f"CREATE DATABASE".encode() + b" /*!32312 IF NOT EXISTS*/ " + new_create,
    )
    return data


def flow_restore_sql(host, port, user, pwd):
    """Mode 5: Restore a .sql or .sql.gz dump file into MySQL."""
    mysql_bin = MYSQL_BINS.get("mysql")
    if not mysql_bin:
        print(red("  ✗ mysql client is not installed. Cannot restore."))
        sys.exit(1)

    print(bold("\n── Restore SQL dump ────────────────────────────────────────"))
    print(dim("  Supported: .sql  .sql.gz"))
    print(dim("  Tip: drag & drop the file into the terminal\n"))

    # Ask for file
    while True:
        path = _prompt("SQL file path", required=True)
        path = os.path.expanduser(path.strip("'\""))
        if not os.path.isfile(path):
            print(red(f"    File not found: {path}"))
            continue
        ext = os.path.splitext(path)[1].lower()
        if path.endswith(".sql.gz"):
            ext = ".sql.gz"
        if ext not in (".sql", ".sql.gz"):
            print(red(f"    Unsupported type '{ext}' — use .sql or .sql.gz"))
            continue
        break

    is_gz     = path.endswith(".sql.gz")
    file_size = os.path.getsize(path)
    print(f"\n  {dim('File:')}   {os.path.basename(path)}")
    print(f"  {dim('Size:')}   {_format_size(file_size)}" +
          (f"  {dim('(gzip compressed)')}" if is_gz else ""))

    # Peek for embedded database name
    embedded_db = _peek_dump_database(path)
    if embedded_db:
        print(f"  {dim('Embedded DB:')} {cyan(embedded_db)}  {dim('(found inside the dump)')}")

    # List existing databases and ask target
    conn, _ = connect_mysql(host, port, user, pwd)
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT IN "
        "('information_schema','performance_schema','mysql','sys')"
    )
    existing_dbs = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if existing_dbs:
        print(f"  {dim('Existing databases:')}  " + "  ".join(cyan(d) for d in existing_dbs))

    # Derive default DB name: prefer embedded name, then filename
    if embedded_db:
        safe_default = embedded_db
    else:
        base = os.path.basename(path)
        if base.endswith(".sql.gz"):
            base = base[:-7]
        elif base.endswith(".sql"):
            base = base[:-4]
        base = re.sub(r'_\d{8}_\d{6}$', '', base)
        safe_default = re.sub(r'[^a-z0-9]', '_', base.lower()).strip('_') or "restore_db"

    print()
    db_name = _prompt("Target database", default=safe_default)
    is_remote = host not in ("127.0.0.1", "localhost", "::1")

    # Detect if we need to rewrite the embedded DB name
    needs_rewrite = embedded_db and embedded_db != db_name
    if needs_rewrite:
        print(f"  {dim('Will rewrite')} USE `{embedded_db}` → USE `{db_name}` {dim('in the SQL stream')}")

    # Check if DB exists
    create_db = True
    if db_name in existing_dbs:
        print(f"\n  {yellow('⚠')} Database {bold(db_name)} already exists")
        ans = _prompt("  Drop and recreate it? (yes = fresh restore, no = restore into existing)", default="no")
        if ans.lower() in ("y", "yes"):
            create_db = True
        else:
            create_db = False

    # Confirm
    host_str = f"{host}:{port}"
    if is_remote:
        host_str = yellow(f"{host_str}  ⚠ REMOTE SERVER")

    print(bold("\n── Restore summary ────────────────────────────────────────"))
    print(f"  {cyan('File:')}      {os.path.basename(path)}  {dim(f'({_format_size(file_size)})')}")
    print(f"  {cyan('Database:')}  {db_name}" +
          (f"  {dim('(drop & recreate)')}" if create_db and db_name in existing_dbs else
           f"  {dim('(new)')}" if create_db else f"  {dim('(into existing)')}"))
    if needs_rewrite:
        print(f"  {cyan('Rewrite:')}   {embedded_db} → {db_name}")
    print(f"  {cyan('Host:')}      {host_str}")
    print()
    ans = _prompt("Proceed with restore?", default="yes")
    if ans.lower() not in ("y", "yes"):
        print(yellow("  Aborted."))
        return None

    # Create/recreate database if needed
    print(bold("\n── Restoring ──────────────────────────────────────────────"))
    conn, _ = connect_mysql(host, port, user, pwd)
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    if create_db:
        if db_name in existing_dbs:
            cur.execute(f"DROP DATABASE `{db_name}`")
        cur.execute(
            f"CREATE DATABASE `{db_name}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        print(f"  {green('✓')} Database {bold(db_name)} created")
    conn.commit()
    cur.close()
    conn.close()

    # Build mysql command (don't pass db name — we handle it in the SQL stream)
    cmd = [
        mysql_bin,
        f"-u{user}",
        f"-p{pwd}",
        f"-h{host}",
        f"-P{str(port)}",
        db_name,
    ]

    # Execute: pipe file through stream, rewriting DB name if needed
    print(f"  Restoring {bold(os.path.basename(path))} into {bold(db_name)}...", end=" ", flush=True)
    t_start = time.time()

    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

        if is_gz:
            source = gzip.open(path, "rb")
        else:
            source = open(path, "rb")

        with source:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                if needs_rewrite:
                    chunk = _rewrite_db_in_stream(chunk, embedded_db, db_name)
                proc.stdin.write(chunk)

        proc.stdin.close()
        proc.wait()
        stderr = proc.stderr.read().decode("utf-8", errors="replace").strip()

    except Exception as e:
        print(red(f"failed\n  ✗ {e}"))
        return None

    elapsed = time.time() - t_start

    if proc.returncode != 0:
        if stderr and not all(
            "warning" in line.lower() for line in stderr.splitlines() if line.strip()
        ):
            print(red("failed"))
            print(red(f"  ✗ mysql error: {stderr}"))
            return None

    if stderr and ("warning" in stderr.lower()):
        print(green("done") + f"  {dim(f'({elapsed:.1f}s)')}")
        print(yellow(f"  ⚠  {stderr}"))
    else:
        print(green("done") + f"  {dim(f'({elapsed:.1f}s)')}")

    # Verify: count tables
    conn, _ = connect_mysql(host, port, user, pwd, db=db_name)
    if conn:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"  {green('✓')} Restored {bold(str(len(tables)))} tables into {bold(db_name)}")
        if len(tables) <= 20:
            print(f"  {dim('Tables:')}  " + "  ".join(cyan(t) for t in tables))

    # Final summary
    print(bold(f"\n{'═'*60}"))
    print(green("  ✅  Restore complete!"))
    print(f"  {cyan('Database:')}  {db_name}")
    print(f"  {cyan('Source:')}    {os.path.basename(path)}")
    print(f"  {cyan('Time:')}      {elapsed:.1f}s")
    print(bold(f"{'═'*60}\n"))
    return db_name


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    banner()

    # 0. --check flag: run preflight only
    if len(sys.argv) > 1 and sys.argv[1] in ("--check", "-c"):
        preflight()
        print(green("  Environment check complete.\n"))
        return

    # 1. Pre-flight (installs packages if missing, so import pandas after this)
    mysql_ok, dump_available = preflight()
    import pandas as pd

    # 2. Connection details (ask first — all modes need it, and mode 4 needs is_remote)
    host, port, user, pwd = ask_connection()

    # 3. Remote server warning
    is_remote = host not in ("127.0.0.1", "localhost", "::1")
    if is_remote:
        print()
        print(yellow(f"  ⚠  You are connecting to a REMOTE server: {bold(host)}"))
        print(dim("    • All operations (create DB, drop table, insert) will run on this server"))
        print(dim("    • Make sure you have the right permissions"))
        print(dim("    • Double-check the database name before confirming"))
        confirm = _prompt("  Continue with remote server?", default="yes")
        if confirm.lower() not in ("y", "yes"):
            print(yellow("  Aborted."))
            return

    # 4. Quick connectivity test (with password retry on typo)
    print(f"\n  {dim('Testing connection...')}")
    test_conn, pwd = connect_mysql(host, port, user, pwd, retry_password=True)
    if not test_conn:
        sys.exit(1)
    test_conn.close()
    print(green("  ✓ Connected to MySQL") + (f"  ({host})" if is_remote else ""))

    # 5. Mode selection (after connection — mode 4 highlights "recommended" for remote)
    mode = ask_mode(dump_available, is_remote=is_remote, mysql_client_ok=mysql_ok)

    # ── RESTORE SQL ───────────────────────────────────────────────────────────
    if mode == "5":
        flow_restore_sql(host, port, user, pwd)
        return

    # ── DUMP ONLY ─────────────────────────────────────────────────────────────
    if mode == "3":
        dump_file = flow_dump_only(host, port, user, pwd, dump_available)
        print(bold(f"\n{'═'*60}"))
        if dump_file:
            print(green("  ✅  Dump complete!") + f"  →  {bold(dump_file)}")
        else:
            print(red("  ✗  Dump failed."))
        print(bold(f"{'═'*60}\n"))
        return

    # ── SELECTIVE BACKUP ──────────────────────────────────────────────────────
    if mode == "4":
        flow_selective_backup(host, port, user, pwd, dump_available)
        return

    # ── LOAD (modes 1 & 2) ────────────────────────────────────────────────────
    loaded_tables = []   # track all tables loaded in this session
    db_name       = None
    first_table   = None  # table name from ask_db_table (used for first file)
    dump_dir      = None
    file_number   = 0

    while True:
        file_number += 1
        if file_number > 1:
            print(bold(f"\n── File #{file_number} ──────────────────────────────────────────"))

        file_path = ask_file_path()

        # DB name: ask on first file, reuse after that
        if db_name is None:
            db_name, first_table = ask_db_table(file_path, host, port, user, pwd)
            if mode == "2":
                dump_dir = ask_dump_output_dir(os.path.dirname(os.path.abspath(file_path)))

        # Read file (may return multiple sheets for Excel)
        print(bold("\n── Reading file ────────────────────────────────────────────"))
        sheets = read_file(file_path)

        # Normalize: ensure every item is (sheet_name, df)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        base_safe = re.sub(r'[^a-z0-9]', '_', base_name.lower()).strip('_') or "import_table"
        normalized = []
        for item in sheets:
            if isinstance(item, tuple):
                sheet_name, df = item
                safe_sheet = re.sub(r'[^a-z0-9]', '_', sheet_name.lower()).strip('_') or "sheet"
                normalized.append((safe_sheet, df))
            else:
                normalized.append((base_safe, item))

        # Process each sheet/dataframe
        for sheet_idx, (default_table, df) in enumerate(normalized):
            if len(normalized) > 1:
                print(bold(f"\n── Sheet {sheet_idx+1}/{len(normalized)}: {cyan(default_table)} ────────────────────────"))

            # Table name prompt
            if len(normalized) == 1 and file_number == 1 and first_table:
                # First file, single sheet — already asked in ask_db_table
                table_name = first_table
                first_table = None  # consumed
            else:
                print(bold(f"\n── Table name (into database: {cyan(db_name)}) ───────────────────"))
                table_name = _prompt("Table name", default=default_table)

            original_cols = list(df.columns)
            df.columns    = [sanitize_col(c) for c in df.columns]

            import warnings
            for col in df.columns:
                if any(kw in col for kw in ("time", "date", "created", "updated")):
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

            preview_dataframe(df, original_cols)

            # Confirm
            mode_label = {"1": "Load only", "2": "Load + Dump"}[mode]
            cfg = dict(
                file  = os.path.basename(file_path),
                rows  = f"{len(df):,}",
                cols  = str(len(df.columns)),
                db    = db_name,
                table = table_name,
                host  = host,
                port  = port,
                user  = user,
                mode  = mode_label,
                dump_dir = dump_dir,
            )
            if len(normalized) > 1:
                cfg["sheet"] = default_table
            if not confirm_summary(cfg):
                print(yellow("  Skipped."))
                continue

            # Setup DB & insert
            print(bold("\n── Setting up database ─────────────────────────────────────"))
            conn, _ = connect_mysql(host, port, user, pwd)
            if not conn:
                sys.exit(1)

            _, table_name = setup_database(conn, db_name, table_name, df)

            print(bold("\n── Inserting rows ──────────────────────────────────────────"))
            count = insert_rows(conn, db_name, table_name, df)
            conn.close()
            print(f"\n  {green('✓')} {count:,} rows loaded into {bold(db_name)}.{bold(table_name)}")
            loaded_tables.append((table_name, count))

        # Ask to load another file
        print()
        again = _prompt("Load another file into the same database?", default="no")
        if again.lower() not in ("y", "yes"):
            break

    # ── Dump (mode 2) — once at the end, covers all tables ────────────────
    dump_file = None
    if mode == "2" and loaded_tables:
        print(bold("\n── Taking SQL dump ─────────────────────────────────────────"))
        dump_file = run_dump(host, port, user, pwd, db_name, dump_dir)

    # ── Final summary ─────────────────────────────────────────────────────
    if not loaded_tables:
        print(yellow("\n  No files were loaded.\n"))
        return

    total_rows = sum(c for _, c in loaded_tables)
    print(bold(f"\n{'═'*60}"))
    print(green("  ✅  All done!"))
    print(f"  {cyan('Database:  ')}  {db_name}")
    for tbl, cnt in loaded_tables:
        print(f"  {cyan('  table:')}    {tbl}  ({cnt:,} rows)")
    print(f"  {cyan('Total rows:')}  {total_rows:,}")
    if dump_file:
        print(f"  {cyan('SQL dump:  ')}  {dump_file}")
    print(bold(f"{'═'*60}\n"))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{yellow('  Interrupted by user.')}\n")
        sys.exit(0)
