from DataBase.DatabaseDriver import DatabaseDriver
from DataBase.MySQLDriver import MySQLDriver
from Data.DataPreparation.DataPreparation import DataPreparation
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

def rewrite_sql():
    pass
def main():

    # 数据库连接
    # 参数行提供的数据库参数 连接
    # 调试时先使用以下默认数据库参数
    
    db = MySQLDriver(config={
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "947722",
        "database": "tpcch"
    })

    if not db.connect():
        print("数据库连接失败，程序退出")
        return
    # 原始数据准备
    # 参数行提供的源数据位置 导入
    # 调试时先使用以下给定路径数据
    original_data_path = './tpcc_data'
    original_sql_path = './Data/DataPreparation/schema_sql.sql'
    origin_data = DataPreparation(db, original_data_path, original_sql_path)
    origin_data.prepare_origin_data()
    

    # 开始进行L0 基准测试
    tpcc_queries_path = './DataBase/cleaned_sql/TPC-C'
    tpch_queries_path = './DataBase/cleaned_sql/TPC-H'
    db.evaluation(tpcc_queries_path, tpch_queries_path, physical_schema='tpcch')
    
    
    # 开始进行 Schema 优化 (L3)
    # suggest_schema()
    # 调试使用给定Schema

    sql = rewrite_sql()

    # 准备 rewrite后的数据
    
    rewritten_data = DataPreparation(new_db, )

    # 进行 evaluation 测试

    new_db.evaluation(tpcc_queries_path, tpch_queries_path, physical_schema='new_tpcch')

    new_db.disconnect()
    

    


if __name__ == "__main__":
    main()

