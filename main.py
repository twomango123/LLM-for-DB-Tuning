from DataBase.DatabaseDriver import DatabaseDriver
from DataBase.MySQLDriver import MySQLDriver
from Data.DataPreparation.DataRecom import DataRecom
import subprocess
from pathlib import Path


def chbenchmark_origin_csv():
    # 生成初始数据
    chb_path = Path("./ch-benchmark/chBenchmark")  # C++可执行文件路径
    cmd_csv = [
        str(chb_path),
        "-csv",
        "-wh", "1",
        "-pa", str('/var/lib/mysql-files')
    ]
    
    # 生成CSV
    subprocess.run(cmd_csv, check=True, text=True)

def chbenchmark_first_test():
    chb_path = Path("./ch-benchmark/chBenchmark")
    # 测试AP latency
    cmd_run = [
        str(chb_path),
        "-run",
        "-dsn", 'mysql-bench',
        "-usr", 'root',
        "-pwd", '123!@#200',
        "-a", "1",
        "-t", "0",
        "-wd", "30",
        "-td", "200",
        "-pa", '/var/lib/mysql-files',
        "-op", '/var/lib/mysql-files'
    ]
    
    subprocess.run(cmd_run, check=True, text=True)

    # 测试TP latency
    cmd_run = [
        str(chb_path),
        "-run",
        "-dsn", 'mysql-bench',
        "-usr", 'root',
        "-pwd", '123!@#200',
        "-a", "0",
        "-t", "1",
        "-wd", "30",
        "-td", "200",
        "-pa", '/var/lib/mysql-files',
        "-op", '/var/lib/mysql-files'
    ]
    subprocess.run(cmd_run, check=True, text=True)

    # 测试吞吐
    cmd_run = [
        str(chb_path),
        "-run",
        "-dsn", 'mysql-bench',
        "-usr", 'root',
        "-pwd", '123!@#200',
        "-a", "5",
        "-t", "10",
        "-wd", "60",
        "-td", "300",
        "-pa", '/var/lib/mysql-files',
        "-op", '/var/lib/mysql-files'
    ]
    subprocess.run(cmd_run, check=True, text=True)

def chbenchmark_schema_update_test():
    pass
def run_chbenchmark(csv_path: Path, output_path: Path, db_config, warmup=60, duration=300):
    chb_path = Path("/path/to/chBenchmark")  # C++可执行文件路径
    cmd_csv = [
        str(chb_path),
        "-csv",
        "-wh", "1",
        "-pa", str(csv_path)
    ]
    
    # 生成CSV
    subprocess.run(cmd_csv, check=True)
    
    # 运行测试
    cmd_run = [
        str(chb_path),
        "-run",
        "-dsn", db_config['dsn'],
        "-usr", db_config['user'],
        "-pwd", db_config['password'],
        "-a", "5",
        "-t", "10",
        "-wd", str(warmup),
        "-td", str(duration),
        "-pa", str(csv_path),
        "-op", str(output_path)
    ]
    
    subprocess.run(cmd_run, check=True)
    print(f"✅ 测试完成，结果输出到 {output_path}")
def suggest_schema():
    pass

def compare_schema():
    # 涉及到的SMO操作类型
def apply_schema():

def rewrite_data():

def rewrite_sql():
    # 遍历sql文件，逐组语句进行rewrite

    # 
def main():

    # 数据库连接
    
    # 原始数据准备
    chbenchmark_origin_csv()
    # 源数据导入并进行L0基准测试
    chbenchmark_first_test()

    # 开始进行 Schema 优化 (L3)
    # suggest_schema()
    # 调试使用给定Schema

    new_schema_sql = rewrite_sql()

    # 准备 rewrite后的数据
    
    rewritten_data = DataRecom(new_schema_sql)
    
    rewritten_data.start_rewrite()

    # 进行 evaluation 测试
    chbenchmark_schema_update_test(new_schema_sql)

    return 0


       


if __name__ == "__main__":
    main()

