#!/usr/bin/env python3
import argparse
import configparser
import csv
import re
import sys
import time
from pathlib import Path

# 使脚本可从仓库根路径运行并导入项目内驱动
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from DataBase.MySQLDriver import MySQLDriver


def _opt_str(cfg, key, default=None):
    val = cfg.get(key, fallback=default)
    if val is None:
        return None
    s = str(val).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1]
    return s


def read_sql_statement(sql_path: Path) -> str:
    text = sql_path.read_text(encoding="utf-8", errors="ignore")
    text = text.lstrip("\ufeff\n\r ")
    parts = [p.strip() for p in text.split(";")]
    for p in parts:
        if p:
            return p
    return text.strip()


def natural_query_key(path: Path):
    name = path.stem.lower()
    m = re.search(r"query[_-]?(\d+)", name)
    if m:
        return (int(m.group(1)), name)
    m2 = re.search(r"(\d+)", name)
    if m2:
        return (int(m2.group(1)), name)
    return (sys.maxsize, name)


def is_select_like(sql: str) -> bool:
    head = re.split(r"\s+", sql.strip(), 1)[0].upper() if sql.strip() else ""
    return head in {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"}


def measure_query(driver: MySQLDriver, sql: str) -> float:
    start = time.perf_counter()
    if is_select_like(sql):
        driver.execute_query(sql)
    else:
        ok = driver.execute_statement(sql)
        if not ok:
            raise RuntimeError("statement execution failed")
    end = time.perf_counter()
    return end - start


def build_driver_config(cfg_section):
    return {
        "host": _opt_str(cfg_section, "host", default="localhost"),
        "port": cfg_section.getint("port", fallback=3306),
        "user": _opt_str(cfg_section, "user"),
        "password": _opt_str(cfg_section, "password", default=""),
        "database": _opt_str(cfg_section, "database"),
    }


def main():
    parser = argparse.ArgumentParser(description="收集指定目录下 .sql 查询的执行延迟")
    parser.add_argument("--sql-dir", required=True, help="包含 .sql 文件的目录（递归扫描）")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("db_config.ini")),
        help="包含 [mysql] 段的 INI 配置文件路径",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).with_name("latency_results.csv")),
        help="成功结果 CSV 输出路径",
    )
    parser.add_argument(
        "--error-output",
        default=str(Path(__file__).with_name("latency_errors.csv")),
        help="失败结果 CSV 输出路径",
    )
    args = parser.parse_args()

    sql_root = Path(args.sql_dir)
    if not sql_root.exists():
        print(f"[ERROR] SQL 目录不存在: {sql_root}")
        sys.exit(1)

    # 读取配置（关闭插值与内联注释，确保特殊字符被保留）
    config = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=())
    ini_path = Path(args.config)
    if not ini_path.exists():
        print(f"[ERROR] 配置文件不存在: {ini_path}")
        sys.exit(1)
    config.read(ini_path)
    if "mysql" not in config:
        print("[ERROR] 配置中缺少 [mysql] 段")
        sys.exit(1)

    db_config = build_driver_config(config["mysql"])
    pw_flag = "YES" if db_config.get("password") else "NO"
    print(
        f"[INFO] Connecting {db_config.get('user')}@{db_config.get('host')}:{db_config.get('port')} "
        f"db={db_config.get('database')} password_provided={pw_flag}"
    )

    driver = MySQLDriver(db_config)
    if not driver.connect():
        print("[ERROR] 通过 MySQLDriver 连接数据库失败", file=sys.stderr)
        sys.exit(2)

    # 仅评测形如 queryN.sql 的文件，避免误扫 schema/insert 等脚本
    import re as _re
    _qre = _re.compile(r"^query\d+\.sql$", _re.IGNORECASE)
    sql_files = sorted((p for p in sql_root.rglob("*.sql") if _qre.match(p.name)), key=natural_query_key)
    if not sql_files:
        print(f"[WARN] 未在 {sql_root} 下找到 .sql 文件")

    ok_rows = []
    err_rows = []

    for path in sql_files:
        stem = path.stem
        m = re.search(r"(query[_-]?\d+)", stem.lower())
        query_id = m.group(1) if m else stem

        try:
            sql = read_sql_statement(path)
            if not sql:
                raise ValueError("空的 SQL 语句")
            elapsed_s = measure_query(driver, sql)
            elapsed_ms = int(round(elapsed_s * 1000))
            ok_rows.append((query_id, str(elapsed_ms)))
            print(f"[OK] {query_id} -> {elapsed_ms} ms  ({path})")
        except Exception as e:
            err_rows.append((query_id, str(e)))
            print(f"[ERR] {query_id} 失败: {e}  ({path})", file=sys.stderr)

    # 关闭连接
    try:
        driver.disconnect()
    except Exception:
        pass

    # 写出结果
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query_id", "elapsed_ms"])
        writer.writerows(ok_rows)

    err_path = Path(args.error_output)
    with err_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query_id", "error"])
        writer.writerows(err_rows)

    print(f"\n[RESULT] 写入 {len(ok_rows)} 条耗时到 {out_path}")
    print(f"[RESULT] 写入 {len(err_rows)} 条失败到 {err_path}")


if __name__ == "__main__":
    main()
