/*
Copyright 2019 Materialize, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef MYSQLDIALECT_H
#define MYSQLDIALECT_H

#include "Dialect.h"

#include <string>
#include <vector>
#include <fstream>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <limits.h>

class MySqlDialect : public Dialect {

  private:
    // Resolve binary directory (Linux) for robust relative paths
    static std::string getExeDir() {
        char buf[PATH_MAX] = {0};
        ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
        if (len > 0) {
            buf[len] = '\0';
            std::string p(buf);
            size_t pos = p.find_last_of('/');
            if (pos != std::string::npos) return p.substr(0, pos);
            return p;
        }
        return std::string(".");
    }

    // Read an entire file into a std::string with fallbacks
    static std::string readFileToString(const std::string& path) {
        auto read_once = [](const std::string& p) -> std::string {
            std::ifstream f(p);
            if (!f.is_open()) return std::string();
            std::stringstream ss; ss << f.rdbuf();
            return ss.str();
        };

        // 1) As-is
        std::string content = read_once(path);
        if (!content.empty()) return content;

        // 2) If path is absolute like "/LLM-for-DB-Tuning/...", try prefixing the binary dir
        if (!path.empty() && path[0] == '/') {
            std::string exeDir = getExeDir();
            // join exeDir + "/.." + path (without leading '/'), to land in workspace
            std::string without_leading = path.substr(1);
            std::string candidate = exeDir + "/.."; // up to LLM-for-DB-Tuning
            candidate += "/" + without_leading;
            content = read_once(candidate);
            if (!content.empty()) return content;
        }

        // 3) If the file name is just the basename under query_sql/, try exeDir/../DataBase/...
        size_t last_slash = path.find_last_of('/');
        std::string baseName = (last_slash == std::string::npos) ? path : path.substr(last_slash + 1);
        {
            std::string exeDir = getExeDir();
            std::string candidate = exeDir + "/../DataBase/cleaned_sql/query_sql/" + baseName;
            content = read_once(candidate);
            if (!content.empty()) return content;
        }

        // As a last resort, return empty
        return std::string();
    }
    std::vector<const char*> dropExistingSchemaStatements = {
        "DROP DATABASE IF EXISTS tpcch"};

    

    std::vector<const char*> createSchemaStatements = {
        "DROP DATABASE IF EXISTS tpcch",
        
        "CREATE DATABASE tpcch",

        "CREATE TABLE tpcch.warehouse (\n"
        "	w_id integer,\n"
        "	w_name char(10),\n"
        "	w_street_1 char(20),\n"
        "	w_street_2 char(20),\n"
        "	w_city char(20),\n"
        "	w_state char(2),\n"
        "	w_zip char(9),\n"
        "	w_tax decimal(4,4),\n"
        "	w_ytd decimal(12,2),\n"
        "	PRIMARY KEY (w_id)\n"
        ")",

        "CREATE TABLE tpcch.district (\n"
        "	d_id tinyint,\n"
        "	d_w_id integer,\n"
        "	d_name char(10),\n"
        "	d_street_1 char(20),\n"
        "	d_street_2 char(20),\n"
        "	d_city char(20),\n"
        "	d_state char(2),\n"
        "	d_zip char(9),\n"
        "	d_tax decimal(4,4),\n"
        "	d_ytd decimal(12,2),\n"
        "	d_next_o_id integer,\n"
        "	PRIMARY KEY (d_w_id, d_id) \n"
        ")",

        "CREATE INDEX fk_district_warehouse ON tpcch.district (d_w_id ASC)",

        "CREATE TABLE tpcch.customer (\n"
        "	c_id smallint,\n"
        "	c_d_id tinyint,\n"
        "	c_w_id integer,\n"
        "	c_first char(16),\n"
        "	c_middle char(2),\n"
        "	c_last char(16),\n"
        "	c_street_1 char(20),\n"
        "	c_street_2 char(20),\n"
        "	c_city char(20),\n"
        "	c_state char(2),\n"
        "	c_zip char(9),\n"
        "	c_phone char(16),\n"
        "	c_since DATE,\n"
        "	c_credit char(2),\n"
        "	c_credit_lim decimal(12,2),\n"
        "	c_discount decimal(4,4),\n"
        "	c_balance decimal(12,2),\n"
        "	c_ytd_payment decimal(12,2),\n"
        "	c_payment_cnt smallint,\n"
        "	c_delivery_cnt smallint,\n"
        "	c_data text,\n"
        "	c_n_nationkey integer,\n"
        "	PRIMARY KEY(c_w_id, c_d_id, c_id)\n"
        ")",

        "CREATE INDEX fk_customer_district ON tpcch.customer"
        "(c_w_id ASC, c_d_id ASC)",

        "CREATE TABLE tpcch.history (\n"
        "	h_c_id smallint,\n"
        "	h_c_d_id tinyint,\n"
        "	h_c_w_id integer,\n"
        "	h_d_id tinyint,\n"
        "	h_w_id integer,\n"
        "	h_date date,\n"
        "	h_amount decimal(6,2),\n"
        "	h_data char(24)\n"
        ")",

        "CREATE INDEX fk_history_customer ON tpcch.history "
        "(h_c_w_id ASC, h_c_d_id ASC, h_c_id ASC)",

        "CREATE INDEX fk_history_district ON tpcch.history "
        "(h_w_id ASC, h_d_id ASC)",

        "CREATE TABLE tpcch.neworder (\n"
        "	no_o_id integer,\n"
        "	no_d_id tinyint,\n"
        "	no_w_id integer,\n"
        "	PRIMARY KEY (no_w_id, no_d_id, no_o_id)\n"
        ")",

        "CREATE TABLE tpcch.orders (\n"
        "	o_id integer,\n"
        "	o_d_id tinyint,\n"
        "	o_w_id integer,\n"
        "	o_c_id smallint,\n"
        "	o_entry_d date,\n"
        "	o_carrier_id tinyint,\n"
        "	o_ol_cnt tinyint,\n"
        "	o_all_local tinyint,\n"
        "	PRIMARY KEY (o_w_id, o_d_id, o_id)\n"
        ")",

        "CREATE INDEX fk_order_customer ON tpcch.orders "
        "(o_w_id ASC, o_d_id ASC, o_c_id ASC)",

        "CREATE TABLE tpcch.orderline (\n"
        "	ol_o_id integer,\n"
        "	ol_d_id tinyint,\n"
        "	ol_w_id integer,\n"
        "	ol_number tinyint,\n"
        "	ol_i_id integer,\n"
        "	ol_supply_w_id integer,\n"
        "	ol_delivery_d date,\n"
        "	ol_quantity smallint,\n"
        "	ol_amount decimal(6,2),\n"
        "	ol_dist_info char(24),\n"
        "	PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)\n"
        ")",

        "CREATE INDEX fk_orderline_order ON tpcch.orderline "
        "(ol_w_id ASC, ol_d_id ASC, ol_o_id ASC)",

        "CREATE INDEX fk_orderline_stock ON tpcch.orderline "
        "(ol_supply_w_id ASC, ol_i_id ASC)",

        "CREATE TABLE tpcch.item (\n"
        "	i_id integer,\n"
        "	i_im_id smallint,\n"
        "	i_name char(24),\n"
        "	i_price decimal(5,2),\n"
        "	i_data char(50),\n"
        "	PRIMARY KEY (i_id)\n"
        ")",

        "CREATE TABLE tpcch.stock (\n"
        "	s_i_id integer,\n"
        "	s_w_id integer,\n"
        "	s_quantity integer,\n"
        "	s_dist_01 char(24),\n"
        "	s_dist_02 char(24),\n"
        "	s_dist_03 char(24),\n"
        "	s_dist_04 char(24),\n"
        "	s_dist_05 char(24),\n"
        "	s_dist_06 char(24),\n"
        "	s_dist_07 char(24),\n"
        "	s_dist_08 char(24),\n"
        "	s_dist_09 char(24),\n"
        "	s_dist_10 char(24),\n"
        "	s_ytd integer,\n"
        "	s_order_cnt integer,\n"
        "	s_remote_cnt integer,\n"
        "	s_data char(50),\n"
        "	s_su_suppkey integer,\n"
        "	PRIMARY KEY (s_w_id, s_i_id)\n"
        ")",

        "CREATE INDEX fk_stock_warehouse ON tpcch.stock (s_w_id ASC)",

        "CREATE INDEX fk_stock_item ON tpcch.stock (s_i_id ASC)",

        "CREATE TABLE tpcch.nation (\n"
        "	n_nationkey tinyint NOT NULL,\n"
        "	n_name char(25) NOT NULL,\n"
        "	n_regionkey tinyint NOT NULL,\n"
        "	n_comment char(152) NOT NULL,\n"
        "	PRIMARY KEY (n_nationkey)\n"
        ")",

        "CREATE TABLE tpcch.supplier (\n"
        "	su_suppkey smallint NOT NULL,\n"
        "	su_name char(25) NOT NULL,\n"
        "	su_address char(40) NOT NULL,\n"
        "	su_nationkey tinyint NOT NULL,\n"
        "	su_phone char(15) NOT NULL,\n"
        "	su_acctbal decimal(12,2) NOT NULL,\n"
        "	su_comment char(101) NOT NULL,\n"
        "	PRIMARY KEY (su_suppkey)\n"
        ")",

        "CREATE TABLE tpcch.region (\n"
        "	r_regionkey tinyint NOT NULL,\n"
        "	r_name char(55) NOT NULL,\n"
        "	r_comment char(152) NOT NULL,\n"
        "	PRIMARY KEY (r_regionkey)\n"
        ")"};

    std::vector<const char*> additionalPreparationStatements = {};

    std::vector<const char*> importPrefixStrings = {
        "LOAD DATA INFILE '", "LOAD DATA INFILE '", "LOAD DATA INFILE '",
        "LOAD DATA INFILE '", "LOAD DATA INFILE '", "LOAD DATA INFILE '",
        "LOAD DATA INFILE '", "LOAD DATA INFILE '", "LOAD DATA INFILE '",
        "LOAD DATA INFILE '", "LOAD DATA INFILE '", "LOAD DATA INFILE '"};

    std::vector<const char*> importSuffixStrings = {
        "/warehouse.tbl' INTO TABLE tpcch.warehouse FIELDS TERMINATED BY '|'",
        "/district.tbl' INTO TABLE tpcch.district FIELDS TERMINATED BY '|'",
        "/customer.tbl' INTO TABLE tpcch.customer FIELDS TERMINATED BY '|'",
        "/history.tbl' INTO TABLE tpcch.history FIELDS TERMINATED BY '|'",
        "/neworder.tbl' INTO TABLE tpcch.neworder FIELDS TERMINATED BY '|'",
        "/orders.tbl' INTO TABLE tpcch.orders FIELDS TERMINATED BY '|' "
        "  (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, @x, o_ol_cnt, o_all_local) "
        "  SET o_carrier_id = IF(@x = '', NULL, @x)",
        "/orderline.tbl' INTO TABLE tpcch.orderline FIELDS TERMINATED BY '|'"
        "  (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, @x, ol_quantity, ol_amount, ol_dist_info) "
        "  SET ol_delivery_d = IF(@x = '', NULL, @x)",
        "/item.tbl' INTO TABLE tpcch.item FIELDS TERMINATED BY '|'",
        "/stock.tbl' INTO TABLE tpcch.stock FIELDS TERMINATED BY '|'",
        "/nation.tbl' INTO TABLE tpcch.nation FIELDS TERMINATED BY '|'",
        "/supplier.tbl' INTO TABLE tpcch.supplier FIELDS TERMINATED BY '|'",
        "/region.tbl' INTO TABLE tpcch.region FIELDS TERMINATED BY '|'"};


    std::vector<std::string> tpchQueryFiles = {
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_01.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_02.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_03.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_04.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_05.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_06.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_07.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_08.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_09.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_10.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_11.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_12.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_13.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_14.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_15.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_16.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_17.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_18.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_19.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_20.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_21.sql",
        "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/query_22.sql"
    };


  public:
    // Auto-load SQL texts on construction
    MySqlDialect() {
        init();
    }
    std::string getSelectCountWarehouseString;
    std::string getSelectCountDistrictString;
    std::string getSelectCountCustomerString;
    std::string getSelectCountOrderString;
    std::string getSelectCountOrderlineString;
    std::string getSelectCountNeworderString;
    std::string getSelectCountHistoryString;
    std::string getSelectCountStockString;
    std::string getSelectCountItemString;
    std::string getSelectCountSupplierString;
    std::string getSelectCountNationString;
    std::string getSelectCountRegionString;
    std::string getNoWarehouseSelectString;
    std::string getNoDistrictSelectString;
    std::string getNoCustomerSelectString;
    std::string getNoItemSelectString;
    std::string getNoStockSelect01String;
    std::string getNoStockSelect02String;
    std::string getNoStockSelect03String;
    std::string getNoStockSelect04String;
    std::string getNoStockSelect05String;
    std::string getNoStockSelect06String;
    std::string getNoStockSelect07String;
    std::string getNoStockSelect08String;
    std::string getNoStockSelect09String;
    std::string getNoStockSelect10String;
    std::string getPmWarehouseSelectString;
    std::string getPmDistrictSelectString;
    std::string getPmCustomerSelect1String;
    std::string getPmCustomerSelect2String;
    std::string getPmCustomerSelect3String;
    std::string getPmCustomerSelect4String;
    std::string getOsCustomerSelect1String;
    std::string getOsCustomerSelect2String;
    std::string getOsCustomerSelect3String;
    std::string getOsOrderSelectString;
    std::string getOsOrderlineSelectString;
    std::string getDlNewOrderSelectString;
    std::string getDlOrderSelectString;
    std::string getDlOrderlineSelectString;
    std::string getSlDistrictSelectString;
    std::string getSlStockSelectString;

    bool init(){
        const std::string base = "/LLM-for-DB-Tuning/DataBase/cleaned_sql/query_sql/";
        const std::string baseupdate = "/LLM-for-DB-Tuning/DataBase/cleaned_sql/update_sql/";

        // Database check
        getSelectCountWarehouseString = readFileToString(base + "getSelectCountWarehouse.sql");
        getSelectCountDistrictString  = readFileToString(base + "getSelectCountDistrict.sql");
        getSelectCountCustomerString  = readFileToString(base + "getSelectCountCustomer.sql");
        getSelectCountOrderString     = readFileToString(base + "getSelectCountOrder.sql");
        getSelectCountOrderlineString = readFileToString(base + "getSelectCountOrderline.sql");
        getSelectCountNeworderString  = readFileToString(base + "getSelectCountNeworder.sql");
        getSelectCountHistoryString   = readFileToString(base + "getSelectCountHistory.sql");
        getSelectCountStockString     = readFileToString(base + "getSelectCountStock.sql");
        getSelectCountItemString      = readFileToString(base + "getSelectCountItem.sql");
        getSelectCountSupplierString  = readFileToString(base + "getSelectCountSupplier.sql");
        getSelectCountNationString    = readFileToString(base + "getSelectCountNation.sql");
        getSelectCountRegionString    = readFileToString(base + "getSelectCountRegion.sql");

        // NewOrder
        getNoWarehouseSelectString = readFileToString(base + "getNoWarehouseSelect.sql");
        getNoDistrictSelectString  = readFileToString(base + "getNoDistrictSelect.sql");
        getNoCustomerSelectString  = readFileToString(base + "getNoCustomerSelect.sql");
        getNoItemSelectString      = readFileToString(base + "getNoItemSelect.sql");
        getNoStockSelect01String   = readFileToString(base + "getNoStockSelect01.sql");
        getNoStockSelect02String   = readFileToString(base + "getNoStockSelect02.sql");
        getNoStockSelect03String   = readFileToString(base + "getNoStockSelect03.sql");
        getNoStockSelect04String   = readFileToString(base + "getNoStockSelect04.sql");
        getNoStockSelect05String   = readFileToString(base + "getNoStockSelect05.sql");
        getNoStockSelect06String   = readFileToString(base + "getNoStockSelect06.sql");
        getNoStockSelect07String   = readFileToString(base + "getNoStockSelect07.sql");
        getNoStockSelect08String   = readFileToString(base + "getNoStockSelect08.sql");
        getNoStockSelect09String   = readFileToString(base + "getNoStockSelect09.sql");
        getNoStockSelect10String   = readFileToString(base + "getNoStockSelect10.sql");

        // Payment
        getPmWarehouseSelectString = readFileToString(base + "getPmWarehouseSelect.sql");
        getPmDistrictSelectString  = readFileToString(base + "getPmDistrictSelect.sql");
        getPmCustomerSelect1String = readFileToString(base + "getPmCustomerSelect1.sql");
        getPmCustomerSelect2String = readFileToString(base + "getPmCustomerSelect2.sql");
        getPmCustomerSelect3String = readFileToString(base + "getPmCustomerSelect3.sql");
        getPmCustomerSelect4String = readFileToString(base + "getPmCustomerSelect4.sql");

        // OrderStatus
        getOsCustomerSelect1String = readFileToString(base + "getOsCustomerSelect1.sql");
        getOsCustomerSelect2String = readFileToString(base + "getOsCustomerSelect2.sql");
        getOsCustomerSelect3String = readFileToString(base + "getOsCustomerSelect3.sql");
        getOsOrderSelectString     = readFileToString(base + "getOsOrderSelect.sql");
        getOsOrderlineSelectString = readFileToString(base + "getOsOrderlineSelect.sql");

        // Delivery
        getDlNewOrderSelectString  = readFileToString(base + "getDlNewOrderSelect.sql");
        getDlOrderSelectString     = readFileToString(base + "getDlOrderSelect.sql");
        getDlOrderlineSelectString = readFileToString(base + "getDlOrderlineSelect.sql");

        // StockLevel
        getSlDistrictSelectString  = readFileToString(base + "getSlDistrictSelect.sql");
        getSlStockSelectString     = readFileToString(base + "getSlStockSelect.sql");

        return true;
    }
    // Strings to create initial database
    virtual std::vector<const char*>& getDropExistingSchemaStatements() {
        return dropExistingSchemaStatements;
    }

    virtual std::vector<const char*>& getCreateSchemaStatements() {
        return createSchemaStatements;
    }

    virtual std::vector<const char*>& getImportPrefix() {
        return importPrefixStrings;
    }

    virtual std::vector<const char*>& getImportSuffix() {
        return importSuffixStrings;
    }

    virtual std::vector<const char*>& getAdditionalPreparationStatements() {
        return additionalPreparationStatements;
    }

    std::vector<std::string> loadSQLFiles(const std::vector<std::string>& paths) {
        std::vector<std::string> queries;
        queries.reserve(paths.size());
        for (const auto& path : paths) {
            queries.push_back(readFileToString(path));
        }
        return queries;
    }
    const char* loadSQLFile(const std::string& path) {
        static std::string cache;  // 静态变量，生命周期长
        
        std::ifstream file(path);
        if (!file.is_open()) {
            cache.clear();
            return nullptr;
        }
        
        std::stringstream buffer;
        buffer << file.rdbuf();
        cache = buffer.str();
        
        return cache.c_str();  // 指向静态变量，安全
    }

    // 22 adjusted TPC-H OLAP query strings
    virtual std::vector<const char*>& getTpchQueryStrings() {

        static std::vector<const char*> tpchQueryStrings;
        static std::vector<char*> allocatedMemory;  // 跟踪分配的内存以便清理
        
        // 如果已经初始化，直接返回
        if (!tpchQueryStrings.empty()) {
            return tpchQueryStrings;
        }
        
        // 加载所有查询
        auto queries = loadSQLFiles(tpchQueryFiles);
        
        // 转换为C风格字符串数组
        tpchQueryStrings.reserve(queries.size());
        allocatedMemory.reserve(queries.size());
        
        for (const auto& query : queries) {
            // 为每个查询创建新的C字符串
            char* cstr = new char[query.length() + 1];
            std::strcpy(cstr, query.c_str());
            tpchQueryStrings.push_back(cstr);
            allocatedMemory.push_back(cstr);  // 记录以便后续清理
        }
        
        return tpchQueryStrings;
    }

    // Strings for database check
    virtual const char* getSelectCountWarehouse() {
        return getSelectCountWarehouseString.c_str();
    }

    virtual const char* getSelectCountDistrict() {
        return getSelectCountDistrictString.c_str();
    }

    virtual const char* getSelectCountCustomer() {
        return getSelectCountCustomerString.c_str();
    }

    virtual const char* getSelectCountOrder() {
        return getSelectCountOrderString.c_str();
    }

    virtual const char* getSelectCountOrderline() {
        return getSelectCountOrderlineString.c_str();
    }

    virtual const char* getSelectCountNeworder() {
        return getSelectCountNeworderString.c_str();
    }

    virtual const char* getSelectCountHistory() {
        return getSelectCountHistoryString.c_str();
    }

    virtual const char* getSelectCountStock() {
        return getSelectCountStockString.c_str();
    }

    virtual const char* getSelectCountItem() {
        return getSelectCountItemString.c_str();
    }

    virtual const char* getSelectCountSupplier() {
        return getSelectCountSupplierString.c_str();
    }

    virtual const char* getSelectCountNation() {
        return getSelectCountNationString.c_str();
    }

    virtual const char* getSelectCountRegion() {
        return getSelectCountRegionString.c_str();
    }

    // // TPC-C transaction strings
    // // NewOrder:
    virtual const char* getNoWarehouseSelect() {
        return getNoWarehouseSelectString.c_str();
    }

    virtual const char* getNoDistrictSelect() {
        return getNoDistrictSelectString.c_str();
    }

    virtual const char* getNoDistrictUpdate() {
        return "update tpcch.district set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?";
    }

    virtual const char* getNoCustomerSelect() {
        return getNoCustomerSelectString.c_str();
    }

    virtual const char* getNoOrderInsert() {
        return "insert into tpcch.orders values (?,?,?,?,?,NULL,?,?)";
    }

    virtual const char* getNoNewOrderInsert() {
        return "insert into tpcch.neworder values(?,?,?)";
    }

    virtual const char* getNoItemSelect() {
        return getNoItemSelectString.c_str();
    }

    virtual const char* getNoStockSelect01() {
        return getNoStockSelect01String.c_str();
    }

    virtual const char* getNoStockSelect02() {
        return getNoStockSelect02String.c_str();
    }

    virtual const char* getNoStockSelect03() {
        return getNoStockSelect03String.c_str();
    }

    virtual const char* getNoStockSelect04() {
        return getNoStockSelect04String.c_str();
    }

    virtual const char* getNoStockSelect05() {
        return getNoStockSelect05String.c_str();
    }

    virtual const char* getNoStockSelect06() {
        return getNoStockSelect06String.c_str();
    }

    virtual const char* getNoStockSelect07() {
        return getNoStockSelect07String.c_str();
    }

    virtual const char* getNoStockSelect08() {
        return getNoStockSelect08String.c_str();
    }

    virtual const char* getNoStockSelect09() {
        return getNoStockSelect09String.c_str();
    }

    virtual const char* getNoStockSelect10() {
        return getNoStockSelect10String.c_str();
    }

    virtual const char* getNoStockUpdate01() {
        return "update tpcch.stock set S_YTD=156 where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockUpdate02() {
        return "update tpcch.stock set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=?, S_REMOTE_CNT=S_REMOTE_CNT+1 where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoOrderlineInsert() {
        return "insert into tpcch.orderline values (?,?,?,?,?,?,NULL,?,?,?)";
    }

    // Payment:
    virtual const char* getPmWarehouseSelect() {
        return getPmWarehouseSelectString.c_str();
    }

    virtual const char* getPmWarehouseUpdate() {
        return "update tpcch.warehouse set W_YTD=W_YTD+? where W_ID=?";
    }

    virtual const char* getPmDistrictSelect() {
        return getPmDistrictSelectString.c_str();
    }

    virtual const char* getPmDistrictUpdate() {
        return "update tpcch.district set D_YTD=D_YTD+? where D_W_ID=? and D_ID=?";
    }

    virtual const char* getPmCustomerSelect1() {
        return getPmCustomerSelect1String.c_str();
    }

    virtual const char* getPmCustomerSelect2() {
        return getPmCustomerSelect2String.c_str();
    }

    virtual const char* getPmCustomerSelect3() {
        return getPmCustomerSelect3String.c_str();
    }

    virtual const char* getPmCustomerUpdate1() {
        return "update tpcch.customer set C_BALANCE=C_BALANCE-?, C_YTD_PAYMENT=C_YTD_PAYMENT+?, C_PAYMENT_CNT=C_PAYMENT_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmCustomerSelect4() {
        return getPmCustomerSelect4String.c_str();
    }

    virtual const char* getPmCustomerUpdate2() {
        return "update tpcch.customer set C_DATA=? where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmHistoryInsert() {
        return "insert into tpcch.history values (?,?,?,?,?,?,?,?)";
    }

    // OrderStatus:
    virtual const char* getOsCustomerSelect1() {
        return getOsCustomerSelect1String.c_str();
    }

    virtual const char* getOsCustomerSelect2() {
        return getOsCustomerSelect2String.c_str();
    }

    virtual const char* getOsCustomerSelect3() {
        return getOsCustomerSelect3String.c_str();
    }

    virtual const char* getOsOrderSelect() {
        return getOsOrderSelectString.c_str();
    }

    virtual const char* getOsOrderlineSelect() {
        return getOsOrderlineSelectString.c_str();
    }

    // Delivery:
    virtual const char* getDlNewOrderSelect() {
        return getDlNewOrderSelectString.c_str();
    }

    virtual const char* getDlNewOrderDelete() {
        return "delete from tpcch.neworder where NO_W_ID=? and NO_D_ID=? and NO_O_ID=?";
    }

    virtual const char* getDlOrderSelect() {
        return getDlOrderSelectString.c_str();
    }

    virtual const char* getDlOrderUpdate() {
        return "update tpcch.orders set O_CARRIER_ID=? where O_W_ID=? and O_D_ID=? and O_ID=?";
    }

    virtual const char* getDlOrderlineUpdate() {
        return "update tpcch.orderline set OL_DELIVERY_D=? where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
    }

    virtual const char* getDlOrderlineSelect() {
        return getDlOrderlineSelectString.c_str();
    }

    virtual const char* getDlCustomerUpdate() {
        return "update tpcch.customer set C_BALANCE=C_BALANCE+?, C_DELIVERY_CNT=C_DELIVERY_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    // StockLevel:
    virtual const char* getSlDistrictSelect() {
        return getSlDistrictSelectString.c_str();
    }

    virtual const char* getSlStockSelect() {
        return getSlStockSelectString.c_str();
    }
};


#endif
