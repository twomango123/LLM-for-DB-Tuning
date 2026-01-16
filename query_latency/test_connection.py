#!/usr/bin/env python3
import argparse
import configparser
import sys
from pathlib import Path


def _opt_str(cfg, key, default=None):
    val = cfg.get(key, fallback=default)
    if val is None:
        return None
    s = str(val).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1]
    return s


def build_driver_config(cfg_section):
    return {
        "host": _opt_str(cfg_section, "host", default="localhost"),
        "port": cfg_section.getint("port", fallback=3306),
        "user": _opt_str(cfg_section, "user"),
        "password": _opt_str(cfg_section, "password", default=""),
        "database": _opt_str(cfg_section, "database"),
    }


def main():
    parser = argparse.ArgumentParser(description="Test MySQL connection using project MySQLDriver")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("db_config.ini")),
        help="Path to INI config with [mysql] section",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from DataBase.MySQLDriver import MySQLDriver

    cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=())
    ini_path = Path(args.config)
    if not ini_path.exists():
        print(f"[ERROR] Config not found: {ini_path}")
        sys.exit(1)
    cfg.read(ini_path)
    if "mysql" not in cfg:
        print("[ERROR] Config must contain [mysql] section")
        sys.exit(1)

    db_config = build_driver_config(cfg["mysql"])
    pw_flag = "YES" if db_config.get("password") else "NO"
    print(
        f"[INFO] Connecting {db_config.get('user')}@{db_config.get('host')}:{db_config.get('port')} "
        f"db={db_config.get('database')} password_provided={pw_flag}"
    )

    driver = MySQLDriver(db_config)
    if not driver.connect():
        print("[FAIL] Connection failed via MySQLDriver")
        sys.exit(2)

    try:
        rows = driver.execute_query("SELECT 1 AS ok")
        print(f"[OK] Connected. Probe query result: {rows}")
        sys.exit(0)
    except Exception as e:
        print(f"[FAIL] Connected but probe query failed: {e}")
        sys.exit(3)
    finally:
        try:
            driver.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()

