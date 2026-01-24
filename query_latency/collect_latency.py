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
        "--all-sql",
        action="store_true",
        help="包含目录下所有 .sql 文件（默认仅执行形如 queryN.sql）",
    )
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
    parser.add_argument(
        "--frequencies",
        default=None,
        help="可选，CSV 文件，包含列 file, relative_frequency_percent；按比例多次执行并统计总耗时",
    )
    parser.add_argument(
        "--total-runs",
        type=int,
        default=100,
        help="在 --frequencies 模式下的总执行次数（默认 100，对应百分比为次数）",
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

    ok_rows = []
    err_rows = []

    # 频率模式：读取 CSV 并按比例重复执行，统计总耗时
    if args.frequencies:
        freq_path = Path(args.frequencies)
        if not freq_path.exists():
            print(f"[ERROR] 频率文件不存在: {freq_path}")
            sys.exit(1)

        # 解析 CSV：期望列 file, relative_frequency_percent（容忍大小写与空白）
        freq_entries = []  # (Path, weight)
        with freq_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            # 容错列名与降级策略：
            # 1) 识别 file 列别名：file/filename/path/sql/name
            # 2) 识别权重列别名：relative_frequency_percent/frequency/percent/pct/count/weight/ratio
            # 3) 若未识别，回退为第1列=文件名，第2列=数值
            def _norm(s):
                return s.strip().lower() if isinstance(s, str) else s
            file_key = None
            weight_key = None
            if reader.fieldnames:
                names = [_norm(n) for n in reader.fieldnames]
                # file 列
                for cand in ("file", "filename", "path", "sql", "name"):
                    if cand in names:
                        file_key = reader.fieldnames[names.index(cand)]
                        break
                # 权重/次数列
                for cand in ("relative_frequency_percent", "relative_frequency", "frequency", "freq", "percent", "pct", "count", "weight", "ratio"):
                    if cand in names:
                        weight_key = reader.fieldnames[names.index(cand)]
                        break
                # 回退：前两列作为 file/weight
                if file_key is None and len(reader.fieldnames) >= 1:
                    file_key = reader.fieldnames[0]
                if weight_key is None and len(reader.fieldnames) >= 2:
                    weight_key = reader.fieldnames[1]

            for row in reader:
                file_name = row.get(file_key) if file_key else None
                if file_name is None:
                    # 尝试首列
                    try:
                        file_name = list(row.values())[0]
                    except Exception:
                        file_name = None
                if not file_name:
                    continue

                weight_val = row.get(weight_key) if weight_key else None
                w = None
                try:
                    if weight_val is not None:
                        w = float(str(weight_val).strip())
                except Exception:
                    w = None
                if w is None:
                    # 回退：尝试第二列
                    try:
                        vals = list(row.values())
                        if len(vals) >= 2:
                            w = float(str(vals[1]).strip())
                    except Exception:
                        w = None
                if w is None:
                    w = 0.0
                if w <= 0:
                    continue
                path = (sql_root / str(file_name).strip()).resolve()
                freq_entries.append((path, w))

        if not freq_entries:
            print(f"[ERROR] 频率文件为空或未解析到有效记录: {freq_path}")
            sys.exit(1)

        total_weight = sum(w for _, w in freq_entries)
        if total_weight <= 0:
            print(f"[ERROR] 频率总和为 0: {freq_path}")
            sys.exit(1)

        # 计算每条 SQL 的执行次数（四舍五入并分配余数，保证总数为 total_runs）
        target_total = max(1, int(args.total_runs))
        alloc = []  # (path, query_id, base_count, frac)
        import math
        for path, w in freq_entries:
            raw = (w / total_weight) * target_total
            base = math.floor(raw)
            frac = raw - base
            stem = path.stem
            m = re.search(r"(query[_-]?\d+)", stem.lower())
            query_id = m.group(1) if m else stem
            alloc.append((path, query_id, base, frac))
        assigned = sum(a[2] for a in alloc)
        # 按小数部分从大到小分配剩余次数
        remainder = target_total - assigned
        if remainder > 0:
            alloc.sort(key=lambda x: x[3], reverse=True)
            for i in range(remainder):
                path, qid, base, frac = alloc[i % len(alloc)]
                alloc[i % len(alloc)] = (path, qid, base + 1, frac)

        # 执行并累计耗时
        totals = {}  # qid -> {"count": int, "total_ms": int}
        for path, qid, count, _ in alloc:
            if count <= 0:
                continue
            if not path.exists():
                err_rows.append((qid, f"SQL 文件不存在: {path}"))
                print(f"[ERR] {qid} 失败: 文件不存在 ({path})", file=sys.stderr)
                continue
            for idx in range(count):
                try:
                    sql = read_sql_statement(path)
                    if not sql:
                        raise ValueError("空的 SQL 语句")
                    elapsed_s = measure_query(driver, sql)
                    elapsed_ms = int(round(elapsed_s * 1000))
                    rec = totals.setdefault(qid, {"count": 0, "total_ms": 0})
                    rec["count"] += 1
                    rec["total_ms"] += elapsed_ms
                    print(f"[OK] {qid} run#{rec['count']} -> {elapsed_ms} ms  ({path})")
                except Exception as e:
                    err_rows.append((qid, str(e)))
                    print(f"[ERR] {qid} 失败: {e}  ({path})", file=sys.stderr)

        # 写出聚合结果：query_id,total_elapsed_ms,count,avg_elapsed_ms
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["query_id", "total_elapsed_ms", "count", "avg_elapsed_ms"])
            for qid, rec in sorted(totals.items()):
                cnt = rec["count"]
                total_ms = rec["total_ms"]
                avg_ms = int(round(total_ms / cnt)) if cnt > 0 else 0
                writer.writerow([qid, str(total_ms), str(cnt), str(avg_ms)])
    else:
        # 根据开关选择：默认仅评测形如 queryN.sql；--all-sql 时包含所有 .sql 文件
        import re as _re
        if args.all_sql:
            sql_files = sorted(sql_root.rglob("*.sql"), key=natural_query_key)
        else:
            _qre = _re.compile(r"^query\d+\.sql$", _re.IGNORECASE)
            sql_files = sorted((p for p in sql_root.rglob("*.sql") if _qre.match(p.name)), key=natural_query_key)

        if not sql_files:
            scope = "所有 .sql" if args.all_sql else "匹配 queryN.sql 的 .sql"
            print(f"[WARN] 未在 {sql_root} 下找到 {scope} 文件")

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

    # 写出结果（仅在默认模式下；频率模式已写聚合结果）
    if not args.frequencies:
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

    if args.frequencies:
        # 在频率模式中，ok 条数为聚合后的键数，不易等同于执行次数
        print(f"\n[RESULT] 频率模式结果写入 {args.output}")
        print(f"[RESULT] 写入 {len(err_rows)} 条失败到 {err_path}")
    else:
        print(f"\n[RESULT] 写入 {len(ok_rows)} 条耗时到 {out_path}")
        print(f"[RESULT] 写入 {len(err_rows)} 条失败到 {err_path}")


if __name__ == "__main__":
    main()
