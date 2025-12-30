// Lightweight offline test for district UPDATE rewrite planning.
// This test does not require a live DB; it prints the generated SQL and
// parameter mappings so you can visually verify.

#include "RewriteDML.h"
#include <iostream>

static void printPlan(const std::vector<RewrittenStmt>& plan) {
    std::cout << "plan size: " << plan.size() << "\n";
    for (size_t i = 0; i < plan.size(); ++i) {
        std::cout << "[" << i << "] sql: " << plan[i].sql << "\n";
        std::cout << "     map: ";
        for (size_t j = 0; j < plan[i].newToOldIndex.size(); ++j) {
            if (j) std::cout << ",";
            std::cout << plan[i].newToOldIndex[j];
        }
        std::cout << "\n";
    }
}

int main() {
    // Original district update SQL (from DialectStrategy::getNoDistrictUpdate)
    std::string original = "update tpcch.district set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?";

    // Case 1: No mapping -> plan empty
    {
        std::map<std::string, std::vector<std::string>> old2new;
        std::map<std::string, std::vector<std::string>> newCols;
        auto plan = RewriteDML::planDistrictUpdateRewrite(original, old2new, newCols);
        std::cout << "-- case1: no mapping --\n";
        printPlan(plan);
    }

    // Case 2: Merge/rename -> single target, keep both predicates
    {
        std::map<std::string, std::vector<std::string>> old2new{
            {"district", {"tpcch.district_v2"}}
        };
        std::map<std::string, std::vector<std::string>> newCols{
            {"tpcch.district_v2", {"d_w_id","d_id","d_next_o_id"}}
        };
        auto plan = RewriteDML::planDistrictUpdateRewrite(original, old2new, newCols);
        std::cout << "-- case2: merge/rename --\n";
        printPlan(plan);
    }

    // Case 3: Split into core/aux, both have keys, only core has d_next_o_id
    {
        std::map<std::string, std::vector<std::string>> old2new{
            {"district", {"tpcch.district_core", "tpcch.district_aux"}}
        };
        std::map<std::string, std::vector<std::string>> newCols{
            {"tpcch.district_core", {"d_w_id","d_id","d_next_o_id"}},
            {"tpcch.district_aux",  {"d_w_id","d_id","d_ytd","d_tax"}}
        };
        auto plan = RewriteDML::planDistrictUpdateRewrite(original, old2new, newCols);
        std::cout << "-- case3: split, only core updated --\n";
        printPlan(plan);
    }

    // Case 4: Split with partial predicates on some table
    // example: shard by d_w_id; one table only carries d_w_id key, not d_id
    {
        std::map<std::string, std::vector<std::string>> old2new{
            {"district", {"tpcch.district_w", "tpcch.district_id", "tpcch.district_full"}}
        };
        std::map<std::string, std::vector<std::string>> newCols{
            {"tpcch.district_w",    {"d_w_id","d_next_o_id"}},
            {"tpcch.district_id",   {"d_id","d_next_o_id"}},
            {"tpcch.district_full", {"d_w_id","d_id","d_next_o_id"}}
        };
        auto plan = RewriteDML::planDistrictUpdateRewrite(original, old2new, newCols);
        std::cout << "-- case4: split, partial predicates --\n";
        printPlan(plan);
    }

    return 0;
}

