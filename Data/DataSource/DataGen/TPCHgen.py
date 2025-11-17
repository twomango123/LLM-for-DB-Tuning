import os
from DataSource import DataSource

class TupleGen:
    csv_delim = "|"

    # 输出文件句柄
    warehouse = None
    district = None
    customer = None
    history = None
    neworder = None
    order = None
    orderline = None
    item = None
    stock = None
    nation = None
    supplier = None
    region = None

    @staticmethod
    def open_output_files(base_path: str):
        

        os.makedirs(base_path, exist_ok=True)

        def openf(name):
            return open(os.path.join(base_path, name + ".tbl"), "w", encoding="utf-8")

        TupleGen.warehouse  = openf("WAREHOUSE")
        TupleGen.district   = openf("DISTRICT")
        TupleGen.customer   = openf("CUSTOMER")
        TupleGen.history    = openf("HISTORY")
        TupleGen.neworder   = openf("NEWORDER")
        TupleGen.order      = openf("ORDER")
        TupleGen.orderline  = openf("ORDERLINE")
        TupleGen.item       = openf("ITEM")
        TupleGen.stock      = openf("STOCK")
        TupleGen.nation     = openf("NATION")
        TupleGen.supplier   = openf("SUPPLIER")
        TupleGen.region     = openf("REGION")

    @staticmethod
    def close_output_files():
        for f in [
            TupleGen.warehouse, TupleGen.district, TupleGen.customer,
            TupleGen.history, TupleGen.neworder, TupleGen.order,
            TupleGen.orderline, TupleGen.item, TupleGen.stock,
            TupleGen.nation, TupleGen.supplier, TupleGen.region
        ]:
            if f:
                f.close()

    # ------------------ WAREHOUSE ------------------
    @staticmethod
    def gen_warehouse(w_id: int):
        f = TupleGen.warehouse
        d = TupleGen.csv_delim

        f.write(
            f"{w_id}{d}"
        )
        DataSource.add_alphanumeric64(6, 10, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric62(2, f, end_delim=True)
        DataSource.add_wdc_zip(f, end_delim=True)
        DataSource.add_double(0.0, 0.2, 4, f, end_delim=True)
        f.write("300000.00\n")

    # ------------------ DISTRICT ------------------
    @staticmethod
    def gen_district(d_id: int, w_id: int):
        f = TupleGen.district
        d = TupleGen.csv_delim

        f.write(f"{d_id}{d}{w_id}{d}")
        DataSource.add_alphanumeric64(6, 10, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric62(2, f, end_delim=True)
        DataSource.add_wdc_zip(f, end_delim=True)
        DataSource.add_double(0.0, 0.2, 4, f, end_delim=True)

        f.write(f"30000.00{d}3001\n")

    # ------------------ CUSTOMER ------------------
    @staticmethod
    def gen_customer(c_id: int, d_id: int, w_id: int, customer_time: str):
        f = TupleGen.customer
        d = TupleGen.csv_delim

        # C_LAST special logic
        if c_id <= 1000:
            c_last = DataSource.gen_c_last(c_id - 1)
        else:
            c_last = DataSource.random_c_last()

        c_state = DataSource.random_alphanumeric62(2)

        f.write(f"{c_id}{d}{d_id}{d}{w_id}{d}")

        DataSource.add_alphanumeric64(8, 16, f, end_delim=True)
        f.write(f"OE{d}")
        f.write(f"{c_last}{d}")
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)
        DataSource.add_alphanumeric64(10, 20, f, end_delim=True)

        f.write(f"{c_state}{d}")
        DataSource.add_wdc_zip(f, end_delim=True)
        DataSource.add_numeric(16, f, end_delim=True)

        f.write(f"{customer_time}{d}")
        f.write(("BC" if DataSource.random_true(0.1) else "GC") + d)
        f.write(f"50000.00{d}")

        DataSource.add_double(0.0, 0.5, 4, f, end_delim=True)

        f.write(f"-10.00{d}10.00{d}1{d}0{d}")

        DataSource.add_alphanumeric64(300, 500, f, end_delim=True)

        # C_N_NATIONKEY = first char ASCII
        nationkey = ord(c_state[0])
        f.write(f"{nationkey}\n")

    # ------------------ HISTORY ------------------
    @staticmethod
    def gen_history(c_id: int, d_id: int, w_id: int):
        f = TupleGen.history
        d = TupleGen.csv_delim

        f.write(
            f"{c_id}{d}{d_id}{d}{w_id}{d}{d_id}{d}{w_id}{d}"
        )
        f.write(DataSource.get_current_time_string() + d)
        f.write("10.00" + d)
        DataSource.add_alphanumeric64(12, 24, f, end_delim=False)
        f.write("\n")

    # ------------------ NEWORDER ------------------
    @staticmethod
    def gen_neworder(o_id: int, d_id: int, w_id: int):
        f = TupleGen.neworder
        d = TupleGen.csv_delim

        f.write(f"{o_id}{d}{d_id}{d}{w_id}\n")

    # ------------------ ORDER ------------------
    @staticmethod
    def gen_order(o_id: int, d_id: int, w_id: int, c_id: int, ol_cnt: int, order_time: str):
        f = TupleGen.order
        d = TupleGen.csv_delim

        f.write(f"{o_id}{d}{d_id}{d}{w_id}{d}{c_id}{d}{order_time}{d}")

        if o_id <= 2100:
            DataSource.add_int(1, 10, f, end_delim=True)
        else:
            f.write(d)

        f.write(f"{ol_cnt}{d}1\n")

    # ------------------ ORDERLINE ------------------
    @staticmethod
    def gen_orderline(o_id: int, d_id: int, w_id: int, ol_number: int, order_time: str):
        f = TupleGen.orderline
        d = TupleGen.csv_delim

        f.write(f"{o_id}{d}{d_id}{d}{w_id}{d}{ol_number}{d}")
        DataSource.add_int(1, 100000, f, end_delim=True)
        f.write(f"{w_id}{d}")

        # delivery date
        f.write((order_time if o_id <= 2100 else "") + d)

        f.write("5" + d)

        if o_id <= 2100:
            f.write("0.00" + d)
        else:
            DataSource.add_double(0.01, 9999.99, 2, f, end_delim=True)

        DataSource.add_alphanumeric64(24, 24, f, end_delim=False)
        f.write("\n")

    # ------------------ ITEM ------------------
    @staticmethod
    def gen_item(i_id: int):
        f = TupleGen.item
        d = TupleGen.csv_delim

        f.write(f"{i_id}{d}")
        DataSource.addInt(1, 10000, f, end_delim=True)
        DataSource.addAlphanumeric64(14, 24, f, end_delim=True)
        DataSource.add_double(1.0, 100.0, 2, f, end_delim=True)

        if DataSource.random_true(0.1):
            DataSource.add_alphanumeric64_original(26, 50, f, end_delim=False)
        else:
            DataSource.add_alphanumeric64(26, 50, f, end_delim=False)

        f.write("\n")

    # ------------------ STOCK ------------------
    @staticmethod
    def gen_stock(i_id: int, w_id: int):
        f = TupleGen.stock
        d = TupleGen.csv_delim

        f.write(f"{i_id}{d}{w_id}{d}")
        DataSource.add_int(10, 100, f, end_delim=True)

        for _ in range(10):
            DataSource.add_alphanumeric64(24, 24, f, end_delim=True)

        f.write("0" + d + "0" + d + "0" + d)

        if DataSource.random_true(0.1):
            DataSource.add_alphanumeric64_original(26, 50, f, end_delim=True)
        else:
            DataSource.add_alphanumeric64(26, 50, f, end_delim=True)

        f.write(f"{(i_id * w_id) % 10000}\n")

    # ------------------ NATION ------------------
    @staticmethod
    def gen_nation(n):
        f = TupleGen.nation
        d = TupleGen.csv_delim

        f.write(f"{n.id}{d}{n.name}{d}{n.rId}{d}")
        DataSource.add_text_string(31, 114, f, end_delim=False)
        f.write("\n")

    # ------------------ SUPPLIER ------------------
    @staticmethod
    def gen_supplier(su_id: int):
        f = TupleGen.supplier
        d = TupleGen.csv_delim

        f.write(f"{su_id}{d}Supplier#{str(su_id).zfill(9)}{d}")

        DataSource.add_alphanumeric64(10, 40, f, end_delim=True)
        DataSource.add_nid(f, end_delim=True)
        DataSource.add_su_phone(su_id, f, end_delim=True)
        DataSource.add_double(-999.99, 9999.99, 2, f, end_delim=True)

        if (su_id + 7) % 1893 == 0:
            DataSource.add_text_string_customer(25, 100, "Complaints", f, end_delim=False)
        elif (su_id + 13) % 1983 == 0:
            DataSource.add_text_string_customer(25, 100, "Recommends", f, end_delim=False)
        else:
            DataSource.add_text_string(25, 100, f, end_delim=False)

        f.write("\n")

    # ------------------ REGION ------------------
    @staticmethod
    def gen_region(r_id: int, r_name: str):
        f = TupleGen.region
        d = TupleGen.csv_delim

        f.write(f"{r_id}{d}{r_name}{d}")
        DataSource.add_text_string(31, 115, f, end_delim=False)
        f.write("\n")
