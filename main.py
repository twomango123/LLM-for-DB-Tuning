from DataBase.DatabaseDriver import DatabaseDriver
from DataBase.MySQLDriver import MySQLDriver
from Data.DataPreparation.DataPreparation import DataPreparation
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

    new_db = MySQLDriver(config={
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "947722",
        "database": "new_tpcch"
    })

    if not new_db.connect():
        print("数据库连接失败，程序退出")
        return
    
    rewritten_data = DataPreparation(new_db, )
    rewritten_data.prepare_rewritten_data(db)

    db.disconnect()

    # 进行 evaluation 测试

    new_db.evaluation(tpcc_queries_path, tpch_queries_path, physical_schema='new_tpcch')

    new_db.disconnect()
    

    


if __name__ == "__main__":
    main()

