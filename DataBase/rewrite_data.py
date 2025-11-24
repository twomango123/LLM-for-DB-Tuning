import pandas as pd
import re
from pathlib import Path

# -------------------------------
# 配置区域
# -------------------------------
OLD_SCHEMA_FILE = "old_schema.sql"
NEW_SCHEMA_FILE = "new_schema.sql"
DATA_FOLDER = Path("./data")  # 旧 CSV/TBL 文件夹
OUTPUT_FOLDER = Path("./output")  # 输出新 CSV/TBL 文件夹
OUTPUT_FOLDER.mkdir(exist_ok=True)

# -------------------------------
# 工具函数
# -------------------------------
def parse_create_table(sql_file):
    """
    解析 CREATE TABLE 语句，返回 {table_name: [columns]} 字典
    """
    with open(sql_file, "r") as f:
        sql = f.read()

    tables = {}
    create_table_blocks = re.findall(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", sql, re.S | re.I)
    for table_name, cols_block in create_table_blocks:
        cols = []
        for line in cols_block.split(","):
            line = line.strip()
            if not line or line.upper().startswith("PRIMARY KEY") or line.upper().startswith("FOREIGN KEY"):
                continue
            col_name = line.split()[0]
            cols.append(col_name)
        tables[table_name] = cols
    return tables

def detect_schema_changes(old_tables, new_tables):
    """
    对比旧表和新表，识别变化类型
    返回字典 {table_name: {change_type, old_cols, new_cols}}
    """
    changes = {}
    for table in new_tables:
        if table not in old_tables:
            changes[table] = {"change_type": "new_table", "old_cols": [], "new_cols": new_tables[table]}
        else:
            old_cols = old_tables[table]
            new_cols = new_tables[table]
            added = [c for c in new_cols if c not in old_cols]
            removed = [c for c in old_cols if c not in new_cols]
            if added or removed:
                changes[table] = {"change_type": "column_change", "old_cols": old_cols, "new_cols": new_cols,
                                  "added": added, "removed": removed}
    for table in old_tables:
        if table not in new_tables:
            changes[table] = {"change_type": "dropped_table", "old_cols": old_tables[table], "new_cols": []}
    return changes

def migrate_table(table_name, change_info):
    """
    按照变化类型迁移数据
    """
    old_file = DATA_FOLDER / f"{table_name}.tbl"
    if not old_file.exists():
        print(f"旧表文件 {old_file} 不存在，跳过")
        return

    old_cols = change_info.get("old_cols", [])
    new_cols = change_info.get("new_cols", [])

    df_old = pd.read_csv(old_file, sep="|", names=old_cols, dtype=str)

    if change_info["change_type"] == "new_table":
        # 新表直接创建空文件
        df_new = pd.DataFrame(columns=new_cols)
    elif change_info["change_type"] == "dropped_table":
        print(f"表 {table_name} 被删除，数据不迁移")
        return
    elif change_info["change_type"] == "column_change":
        # 新增字段填空，删除字段忽略
        df_new = pd.DataFrame()
        for col in new_cols:
            if col in df_old.columns:
                df_new[col] = df_old[col]
            else:
                df_new[col] = ""  # 新增字段填空
    else:
        df_new = df_old[new_cols]  # 基本情况

    # 输出新 CSV/TBL 文件
    output_file = OUTPUT_FOLDER / f"{table_name}.tbl"
    df_new.to_csv(output_file, sep="|", index=False, header=False)
    print(f"{table_name} 数据迁移完成 -> {output_file}")

# -------------------------------
# 主程序
# -------------------------------
if __name__ == "__main__":
    old_tables = parse_create_table(OLD_SCHEMA_FILE)
    new_tables = parse_create_table(NEW_SCHEMA_FILE)
    changes = detect_schema_changes(old_tables, new_tables)

    for table, change_info in changes.items():
        migrate_table(table, change_info)
