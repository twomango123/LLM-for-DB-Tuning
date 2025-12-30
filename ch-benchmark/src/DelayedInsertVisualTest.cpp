// Visual demo for delayed insert behavior in NewOrder.
// Prints what happens when merge is required (defer orders/neworder, flush at
// last orderline) vs when merge is not required (direct rewrites for each table).

#define REWRITE_NO_ODBC
#include "RewriteDML.h"
#include <iostream>
#include <map>
#include <string>
#include <vector>

static void printPlan(const std::vector<RewrittenStmt>& plan){
    for(size_t i=0;i<plan.size();++i){
        std::cout << "  [" << i << "] " << plan[i].sql << "\n";
    }
}

static void scenarioNoMerge(){
    std::cout << "== Scenario: no merge required ==\n";
    // Original SQLs (as used by MySqlDialect)
    const std::string ins_orders    = "insert into tpcch.orders(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local) values (?,?,?,?,?,?,?)";
    const std::string ins_neworder  = "insert into tpcch.neworder values (?,?,?)";
    const std::string ins_orderline = "insert into tpcch.orderline values (?,?,?,?,?,?,NULL,?,?,?)";

    std::cout << "  original orders:    " << ins_orders << "\n";
    std::cout << "  original neworder:  " << ins_neworder << "\n";
    std::cout << "  original orderline: " << ins_orderline << "\n";

    // Mapping: three separate targets (no intersection with orderline)
    std::map<std::string, std::vector<std::string>> m;
    std::map<std::string, std::vector<std::string>> cols;

    m["orders"]    = {"tpcch.orders_target"};
    cols["tpcch.orders_target"] = {"o_id","o_d_id","o_w_id","o_c_id","o_entry_d","o_ol_cnt","o_all_local"};

    m["neworder"]  = {"tpcch.neworder_target"};
    cols["tpcch.neworder_target"] = {"no_o_id","no_d_id","no_w_id"};

    m["orderline"] = {"tpcch.orderline_target"};
    cols["tpcch.orderline_target"] = {"ol_o_id","ol_d_id","ol_w_id","ol_number","ol_i_id","ol_supply_w_id","ol_quantity","ol_amount","ol_dist_info"};

    auto p1 = RewriteDML::planInsertRewrite("orders",    ins_orders,    m, cols);
    auto p2 = RewriteDML::planInsertRewrite("neworder",  ins_neworder,  m, cols);
    auto p3 = RewriteDML::planInsertRewrite("orderline", ins_orderline, m, cols);

    std::cout << "  rewritten orders:    \n";    printPlan(p1);
    std::cout << "  rewritten neworder:  \n";    printPlan(p2);
    std::cout << "  rewritten orderline: \n";    printPlan(p3);
}

struct Line { int ol; int i_id; int s_w; int qty; double amt; std::string dist; };

static void scenarioMergeRequired(){
    std::cout << "\n== Scenario: merge required (target: tpcch.orders_orderline) ==\n";
    const std::string ins_orders    = "insert into tpcch.orders(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local) values (?,?,?,?,?,?,?)";
    const std::string ins_neworder  = "insert into tpcch.neworder values (?,?,?)";
    const std::string ins_orderline = "insert into tpcch.orderline values (?,?,?,?,?,?,NULL,?,?,?)";

    std::cout << "  original orders:    " << ins_orders << "\n";
    std::cout << "  original neworder:  " << ins_neworder << "\n";
    std::cout << "  original orderline: " << ins_orderline << "\n";

    // Header values (outside loop)
    int w=1, d=2, o=1001, c_id=300, ol_cnt=3, all_local=1; std::string entry_d="2025-01-01 10:00:00";
    // neworder header
    int no_w=w, no_d=d, no_o=o;

    // Stage headers (defer)
    std::cout << "  [defer] orders into merge target 'tpcch.orders_orderline'\n";
    std::cout << "          staged: {o_w_id="<<w<<", o_d_id="<<d<<", o_id="<<o<<", o_c_id="<<c_id
              <<", o_entry_d='"<<entry_d<<"', o_ol_cnt="<<ol_cnt<<", o_all_local="<<all_local<<"}\n";
    std::cout << "  [defer] neworder into merge target 'tpcch.orders_orderline'\n";
    std::cout << "          staged: {no_w_id="<<no_w<<", no_d_id="<<no_d<<", no_o_id="<<no_o<<"}\n";

    // Loop lines; flush at the last one
    std::vector<Line> lines = {
        {1, 5001, 1, 3, 29.97, "DIST-01"},
        {2, 5002, 1, 1,  9.99, "DIST-02"},
        {3, 5003, 1, 2, 19.98, "DIST-03"}
    };

    for(size_t i=0;i<lines.size();++i){
        const auto& ln = lines[i];
        if (i+1 < lines.size()){
            std::cout << "  [stage] orderline ol_number="<<ln.ol<<" (no flush yet)\n";
        } else {
            std::cout << "  [flush] last orderline -> emit merged inserts: \n";
            for(const auto& lx : lines){
                // Compose merged INSERT SQL into the single target
                std::cout << "    insert into tpcch.orders_orderline("
                          << "o_w_id,o_d_id,o_id,ol_number,o_c_id,o_entry_d,o_ol_cnt,o_all_local,"
                             "no_w_id,no_d_id,no_o_id,"
                             "ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_dist_info) values ("
                          << w << "," << d << "," << o << "," << lx.ol << ","
                          << c_id << ",'" << entry_d << "'," << ol_cnt << "," << all_local << ","
                          << no_w << "," << no_d << "," << no_o << ","
                          << lx.i_id << "," << lx.s_w << "," << lx.qty << "," << lx.amt << "," << "'" << lx.dist << "'" << ")";
                std::cout << "\n";
            }
        }
    }
}

int main(){
    scenarioNoMerge();
    scenarioMergeRequired();
    return 0;
}
