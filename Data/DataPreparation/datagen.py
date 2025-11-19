import sys
import os
sys.path.append(os.path.abspath("../"))  # 将上一级目录加入模块搜索路径

import argparse
from DataSource.DataSource import DataSource
from TPCHgen import TupleGen
#默认一个仓库数据
WAREHOUSE_COUNT = 1

def dbgen(output_path: str, warehouse_count: int):
    """
    Python 等价版本：
    bool dbgen()
    """

    print("Initializing data source...")
    DataSource.initialize()

    print(f"Opening CSV output files in {output_path} ...")
    os.makedirs(output_path, exist_ok=True)
    TupleGen.open_output_files(output_path)

    # --------------------------
    #  ITEM 表
    # --------------------------
    print("Generating ITEM table...")
    for i_id in range(1, 100000 + 1):
        TupleGen.gen_item(i_id)

    # 订单计数控制
    customer_time = ""
    order_time = ""

    print(f"Configured warehouse count = {warehouse_count}")
    # --------------------------
    #  仓库级别数据
    # --------------------------
    for w_id in range(1, warehouse_count + 1):

        print(f"Generating WAREHOUSE {w_id} ...")

        # Warehouse
        TupleGen.gen_warehouse(w_id)

        # Stock
        print(f"  Generating STOCK for warehouse {w_id} ...")
        for i_id in range(1, 100000 + 1):
            TupleGen.gen_stock(i_id, w_id)

        # District + Customer + Orders
        for d_id in range(1, 10 + 1):
            print(f"    Generating DISTRICT {d_id} ...")
            TupleGen.gen_district(d_id, w_id)

            for c_id in range(1, 3000 + 1):

                # Customer
                if customer_time == "":
                    customer_time = DataSource.get_current_time_string()

                TupleGen.gen_customer(c_id, d_id, w_id, customer_time)

                # History
                TupleGen.gen_history(c_id, d_id, w_id)

                # Order
                o_id = DataSource.permute(c_id, 1, 3000)
                ol_count = DataSource.next_orderline_count()
                order_time = DataSource.get_current_time_string()

                TupleGen.gen_order(o_id, d_id, w_id, c_id, ol_count, order_time)

                # Orderline
                for ol_number in range(1, ol_count + 1):
                    TupleGen.gen_orderline(o_id, d_id, w_id, ol_number, order_time)

                # NewOrder
                if o_id > 2100:
                    TupleGen.gen_neworder(o_id, d_id, w_id)

    # --------------------------
    # Region
    # --------------------------
    print("Generating REGION...")
    for r_id in range(5):
        TupleGen.gen_region(r_id, DataSource.get_region(r_id))

    # --------------------------
    # Nation
    # --------------------------
    print("Generating NATION...")
    for i in range(62):
        TupleGen.gen_nation(DataSource.get_nation(i))

    # --------------------------
    # Supplier
    # --------------------------
    print("Generating SUPPLIER...")
    for su_id in range(10000):
        TupleGen.gen_supplier(su_id)

    # --------------------------
    # CLOSE OUTPUT
    # --------------------------
    print("Closing CSV files...")
    TupleGen.close_output_files()

    print("TPC-C dbgen completed successfully.")

    return True

# ============================================================
#               命令行 main()
# ============================================================

def main():
    

    parser = argparse.ArgumentParser(description="Python TPC-C data generator")

    parser.add_argument(
        "--wh",
        "--warehouse",
        type=int,
        default=1,
        help="Number of warehouses"
    )

    parser.add_argument("-out", "--output", type=str, required=True, help="Output path for CSV files")
    
    args = parser.parse_args()

    # 直接设置全局变量
    warehouse_count = int(args.wh)
    output_path = args.output
    print(f"Running dbgen with WAREHOUSE_COUNT = {warehouse_count}")

    dbgen(output_path, warehouse_count)


if __name__ == "__main__":
    main()