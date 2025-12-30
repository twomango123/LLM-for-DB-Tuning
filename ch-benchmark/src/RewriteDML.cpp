// Minimal implementation to support district UPDATE rewrite with safe fallback.

#include "RewriteDML.h"
#ifndef REWRITE_NO_ODBC
#include "DbcTools.h"
#include "dialect/DialectStrategy.h"
#endif
#include "Config.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <set>
#include <cstdio>

// ---- Lightweight SQL UPDATE parser (set/where, and '?' positions) ----
namespace {
struct SetFrag { std::string columnLower; std::string raw; size_t absStart=0, absEnd=0; std::vector<int> paramIdx; };
struct WhereFrag { std::string columnLower; std::string raw; size_t absStart=0, absEnd=0; std::vector<int> paramIdx; };
struct ParsedUpdate { std::vector<SetFrag> sets; std::vector<WhereFrag> wheres; };

static std::string toLowerCopy(const std::string& s){
    std::string r=s; std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){return std::tolower(c);}); return r;
}
static std::string trim(const std::string& s){
    size_t i=0,j=s.size();
    while(i<j && std::isspace((unsigned char)s[i])) ++i;
    while(j>i && std::isspace((unsigned char)s[j-1])) --j;
    return s.substr(i,j-i);
}

static void collectQPositions(const std::string& sql, std::vector<size_t>& qpos){
    for(size_t i=0;i<sql.size();++i) if(sql[i]=='?') qpos.push_back(i);
}

static bool findKeyword(const std::string& lower, const std::string& kw, size_t start, size_t& pos){
    size_t p = lower.find(kw, start);
    if(p==std::string::npos) return false;
    pos = p; return true;
}

static std::vector<std::pair<size_t,size_t>> splitByCommaWithPos(const std::string& section){
    std::vector<std::pair<size_t,size_t>> ranges; // [start,end)
    size_t start=0; int depth=0; bool inQuote=false; char qchar=0;
    for(size_t i=0;i<section.size();++i){
        char c=section[i];
        if((c=='\''||c=='\"')){ if(!inQuote){ inQuote=true; qchar=c; } else if(qchar==c){ inQuote=false; } }
        else if(!inQuote){ if(c=='(') depth++; else if(c==')') depth--; }
        if(!inQuote && depth==0 && c==','){
            ranges.emplace_back(start,i);
            start=i+1;
        }
    }
    if(start<section.size()) ranges.emplace_back(start, section.size());
    return ranges;
}

static std::vector<std::pair<size_t,size_t>> splitWhereAndPos(const std::string& lowerWhere){
    std::vector<std::pair<size_t,size_t>> ranges; // [start,end)
    size_t start=0; size_t pos=0; const std::string AND=" and ";
    while(true){
        pos = lowerWhere.find(AND, start);
        if(pos==std::string::npos){ ranges.emplace_back(start, lowerWhere.size()); break; }
        ranges.emplace_back(start, pos);
        start = pos + AND.size();
    }
    return ranges;
}

static std::string extractColumnLower(const std::string& expr){
    auto eq = expr.find('=');
    if(eq==std::string::npos) return "";
    std::string lhs = trim(expr.substr(0, eq));
    return toLowerCopy(lhs);
}

static void assignParamIdxByRange(size_t absStart, size_t absEnd, const std::vector<size_t>& qpos, std::vector<int>& out){
    for(size_t i=0;i<qpos.size();++i){ size_t q=qpos[i]; if(q>=absStart && q<absEnd) out.push_back(static_cast<int>(i+1)); }
}

static bool parseUpdateSQL(const std::string& sql, ParsedUpdate& out){
    std::string lower = toLowerCopy(sql);
    size_t posSet=std::string::npos, posWhere=std::string::npos;
    if(!findKeyword(lower, " set ", 0, posSet)) return false;
    findKeyword(lower, " where ", posSet+5, posWhere); // where optional
    size_t setStart = posSet + 5;
    size_t setEnd = (posWhere==std::string::npos)? sql.size() : posWhere;
    std::string setSection = sql.substr(setStart, setEnd-setStart);
    std::string whereSection;
    if(posWhere!=std::string::npos) whereSection = sql.substr(posWhere+7);

    std::vector<size_t> qpos; collectQPositions(sql, qpos);

    // parse set fragments
    auto setRanges = splitByCommaWithPos(setSection);
    for(const auto& r : setRanges){
        size_t a = setStart + r.first; size_t b = setStart + r.second;
        std::string raw = trim(sql.substr(a, b-a));
        if(raw.empty()) continue;
        SetFrag sf; sf.raw = raw; sf.absStart=a; sf.absEnd=b; sf.columnLower = extractColumnLower(raw);
        assignParamIdxByRange(a, b, qpos, sf.paramIdx);
        out.sets.push_back(std::move(sf));
    }

    // parse where fragments
    if(!whereSection.empty()){
        std::string lowerWhere = toLowerCopy(whereSection);
        auto whereRanges = splitWhereAndPos(lowerWhere);
        for(const auto& r : whereRanges){
            size_t a = (posWhere+7) + r.first; size_t b = (posWhere+7) + r.second;
            std::string raw = trim(sql.substr(a, b-a));
            if(raw.empty()) continue;
            WhereFrag wf; wf.raw = raw; wf.absStart=a; wf.absEnd=b; wf.columnLower = extractColumnLower(raw);
            assignParamIdxByRange(a, b, qpos, wf.paramIdx);
            out.wheres.push_back(std::move(wf));
        }
    }
    return true;
}
}

// Heuristic: recognize the specific district update pattern and rewrite
// according to mapping. If mapping doesn't indicate change, return false.

// Forward declaration so callers before the definition can compile
static bool buildRewrittenGenericUpdate(
    const std::string& oldTable,
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    std::vector<RewrittenStmt>& out);

#ifndef REWRITE_NO_ODBC
bool RewriteDML::maybeRewriteAndExecUpdate(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    auto it = old2new.find(oldTable);
    if (it == old2new.end()) {
        return false; // no rewrite mapping
    }

    std::vector<RewrittenStmt> plan;
    if (!buildRewrittenGenericUpdate(oldTable, originalSql, old2new, newTableColumns, plan)) return false;

    for (const auto& rs : plan) {
        if (!prepareBindExecute(hDBC, rs, params)) {
            return false; // let caller fallback
        }
    }
    return true;
}
#endif

// For district case we support two patterns:
// 1) Split into multiple new tables each containing subsets of columns.
//    We replicate WHERE(D_W_ID=?, D_ID=?) on each table that contains both keys.
// 2) Merge into a single table name (size()==1), replace table name, keep SQL as-is.
// NOTE: We do not implement full SQL parsing here; only simple string ops for demo.
static bool buildRewrittenGenericUpdate(
    const std::string& oldTable,
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    std::vector<RewrittenStmt>& out
){
    // Resolve targets: prefer horizontal split/merge over vertical mapping
    std::vector<std::string> targets;
    {
        auto hs = Config::getHorizontalSplit();
        auto hm = Config::getHorizontalMerge();
        auto itHS = hs.find(oldTable);
        auto itHM = hm.find(oldTable);
        if (itHS != hs.end() && !itHS->second.empty()) {
            targets = itHS->second;
        } else if (itHM != hm.end() && !itHM->second.empty()) {
            targets = { itHM->second };
        } else {
            auto it = old2new.find(oldTable);
            if (it == old2new.end() || it->second.empty()) return false;
            targets = it->second;
        }
    }

    ParsedUpdate pu; if(!parseUpdateSQL(originalSql, pu)) return false;

    // Rewrite helpers
    auto renames    = Config::getColumnRenames();
    auto rmColsConf = Config::getRemoveColumns();
    auto extraSets  = Config::getExtraUpdateSets();
    auto splitCols  = Config::getColumnSplitColumns();
    auto splitDelim = Config::getColumnSplitDelims();

    auto renameCol = [&](const std::string& colLower)->std::string{
        auto itT = renames.find(oldTable); if (itT==renames.end()) return colLower;
        auto itC = itT->second.find(colLower); if (itC==itT->second.end()) return colLower;
        return toLowerCopy(itC->second);
    };
    auto removed = [&](const std::string& colLower)->bool{
        auto it = rmColsConf.find(oldTable); if (it==rmColsConf.end()) return false;
        const auto& v = it->second; return std::find_if(v.begin(), v.end(), [&](const std::string& x){ return toLowerCopy(x)==colLower; })!=v.end();
    };
    auto getSplitSpec = [&](const std::string& srcCol, std::vector<std::string>& outCols, std::string& delim)->bool{
        auto itC = splitCols.find(oldTable); if (itC==splitCols.end()) return false;
        auto itV = itC->second.find(srcCol); if (itV==itC->second.end() || itV->second.empty()) return false;
        outCols = itV->second;
        delim = ",";
        auto itD = splitDelim.find(oldTable); if (itD!=splitDelim.end()){
            auto itD2 = itD->second.find(srcCol); if (itD2!=itD->second.end()) delim = itD2->second;
        }
        // normalize lower
        for (auto& c : outCols) c = toLowerCopy(c);
        return true;
    };

    bool produced=false;
    for(const auto& t : targets){
        // available columns for this target (strict when known)
        bool strict = (newTableColumns.find(t) != newTableColumns.end());
        const auto& cols = strict ? newTableColumns.at(t) : std::vector<std::string>{};
        auto has = [&](const std::string& c){ if(!strict) return true; return std::find_if(cols.begin(), cols.end(), [&](const std::string& x){ return toLowerCopy(x)==c; })!=cols.end(); };

        std::string setClause; std::vector<int> map; std::vector<RewrittenStmt::SplitSpec> specs;
        // SET fragments
        for(const auto& sf : pu.sets){
            if (sf.columnLower.empty()) continue;
            std::string destCol = renameCol(sf.columnLower);
            if (removed(destCol)) continue; // drop redundant

            // attribute split if requested and simple param form present
            std::vector<std::string> splitColsList; std::string delim;
            bool doSplit = getSplitSpec(sf.columnLower, splitColsList, delim) && (sf.paramIdx.size()==1);
            if (doSplit){
                int oldParam = sf.paramIdx[0];
                for (size_t k=0;k<splitColsList.size();++k){
                    const std::string& newCol = splitColsList[k];
                    if (!has(newCol) || removed(newCol)) continue;
                    if (!setClause.empty()) setClause += ", ";
                    setClause += newCol + "=?";
                    map.push_back(oldParam);
                    RewrittenStmt::SplitSpec sp; sp.isSplit=true; sp.tokenIndex=static_cast<int>(k); sp.delim=delim; specs.push_back(sp);
                }
                continue;
            }

            // rename-only
            if (!has(destCol)) continue;
            // rebuild raw as: <dest>=<rhs>
            auto eq = sf.raw.find('=');
            std::string rhs = (eq==std::string::npos) ? std::string("") : trim(sf.raw.substr(eq+1));
            if (!setClause.empty()) setClause += ", ";
            setClause += destCol + "=" + rhs;
            for (int idx : sf.paramIdx) { map.push_back(idx); RewrittenStmt::SplitSpec sp; sp.isSplit=false; sp.tokenIndex=0; specs.push_back(sp);}            
        }
        // extra update sets (literals/expressions)
        auto itEx = extraSets.find(t);
        if (itEx != extraSets.end()){
            for (const auto& kv : itEx->second){
                std::string col = toLowerCopy(kv.first);
                if (!has(col) || removed(col)) continue;
                if (!setClause.empty()) setClause += ", ";
                setClause += col + "=" + kv.second;
            }
        }
        if (setClause.empty()) continue;

        // WHERE fragments
        std::string whereClause; bool anyWhere=false;
        for(const auto& wf : pu.wheres){
            if (wf.columnLower.empty()) continue;
            std::string destCol = renameCol(wf.columnLower);
            if (removed(destCol)) continue; // drop redundant in WHERE
            if (!has(destCol)) continue;
            // rebuild raw as <dest>=<rhs>
            auto eq = wf.raw.find('=');
            std::string rhs = (eq==std::string::npos) ? std::string("") : trim(wf.raw.substr(eq+1));
            if (!whereClause.empty()) whereClause += " and ";
            whereClause += destCol + "=" + rhs; anyWhere=true;
            for (int idx : wf.paramIdx) { map.push_back(idx); RewrittenStmt::SplitSpec sp; sp.isSplit=false; sp.tokenIndex=0; specs.push_back(sp);}            
        }
        if(!pu.wheres.empty() && !anyWhere) continue; // avoid full table update when original had where

        RewrittenStmt rs; rs.sql = std::string("update ") + t + " set " + setClause;
        if(!whereClause.empty()) rs.sql += " where " + whereClause;
        rs.newToOldIndex = std::move(map);
        rs.splitSpecs = std::move(specs);
        out.push_back(std::move(rs));
        produced = true;
    }
    return produced;
}

#ifndef REWRITE_NO_ODBC
// Build plan for stock update (split/merge), similar to district logic.
static bool buildRewrittenForStock(
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    std::vector<RewrittenStmt>& out
) {
    auto it = old2new.find("stock");
    if (it == old2new.end() || it->second.empty()) return false;

    std::string lowered = originalSql;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c){return std::tolower(c);} );
    if (lowered.find("update") == std::string::npos || lowered.find("stock") == std::string::npos) {
        return false;
    }

    // Detect presence of set parts in original SQL
    bool has_ytd = lowered.find("s_ytd=s_ytd+?") != std::string::npos;
    bool has_qty = lowered.find("s_quantity=?") != std::string::npos;
    bool has_order_cnt = lowered.find("s_order_cnt=s_order_cnt+1") != std::string::npos;
    bool has_remote_cnt = lowered.find("s_remote_cnt=s_remote_cnt+1") != std::string::npos;

    struct Cond { const char* col; int oldParamIdx; const char* expr; };
    Cond conds[] = {
        {"s_i_id", 3, "s_i_id=?"},
        {"s_w_id", 4, "s_w_id=?"}
    };

    for (const auto& t : it->second) {
        auto nIt = newTableColumns.find(t);
        if (nIt == newTableColumns.end()) continue;
        const auto& cols = nIt->second;
        auto has = [&](const std::string& c){ return std::find_if(cols.begin(), cols.end(), [&](const std::string& x){
            std::string lx = x; std::transform(lx.begin(), lx.end(), lx.begin(), ::tolower);
            std::string lc = c; std::transform(lc.begin(), lc.end(), lc.begin(), ::tolower);
            return lx == lc;
        }) != cols.end(); };

        std::string set;
        std::vector<int> map;
        if (has_ytd && has("s_ytd")) { set += (set.empty()?"":" , "); set += "s_ytd=s_ytd+?"; map.push_back(1); }
        if (has_qty && has("s_quantity")) { set += (set.empty()?"":" , "); set += "s_quantity=?"; map.push_back(2); }
        if (has_order_cnt && has("s_order_cnt")) { set += (set.empty()?"":" , "); set += "s_order_cnt=s_order_cnt+1"; }
        if (has_remote_cnt && has("s_remote_cnt")) { set += (set.empty()?"":" , "); set += "s_remote_cnt=s_remote_cnt+1"; }

        if (set.empty()) continue;

        std::string where;
        for (const auto& c : conds) {
            if (has(c.col)) { if (!where.empty()) where += " and "; where += c.expr; map.push_back(c.oldParamIdx); }
        }
        if (where.empty()) continue;

        RewrittenStmt rs;
        rs.sql = std::string("update ") + t + " set " + set + " where " + where;
        rs.newToOldIndex = std::move(map);
        out.push_back(std::move(rs));
    }
    return !out.empty();
}

bool RewriteDML::prepareBindExecute(
    SQLHDBC hDBC,
    const RewrittenStmt& stmt,
    const ParamPack& params
) {
    SQLHSTMT hStmt = 0;
    if (!DbcTools::allocAndPrepareStmt(hDBC, hStmt, stmt.sql.c_str())) {
        return false;
    }
    // Bind supported kinds; support optional string splitting (attribute split)
    for (size_t i = 0; i < stmt.newToOldIndex.size(); ++i) {
        int oldPos = stmt.newToOldIndex[i];
        if (oldPos < 1 || static_cast<size_t>(oldPos) > params.params.size()) return false;
        const ParamValue& pv = params.params[static_cast<size_t>(oldPos - 1)];
        bool useSplit = (i < stmt.splitSpecs.size() && stmt.splitSpecs[i].isSplit);
        if (useSplit) {
            if (pv.kind != ParamKind::String) return false;
            std::string s = pv.s;
            const auto& sp = stmt.splitSpecs[i];
            std::vector<std::string> tokens; size_t start=0; while(true){ size_t pos = s.find(sp.delim, start); if(pos==std::string::npos){ tokens.push_back(s.substr(start)); break; } tokens.push_back(s.substr(start,pos-start)); start = pos + sp.delim.size(); }
            std::string picked = (sp.tokenIndex >=0 && sp.tokenIndex < static_cast<int>(tokens.size())) ? tokens[sp.tokenIndex] : std::string();
            if (!DbcTools::bind(hStmt, static_cast<int>(i+1), static_cast<int>(picked.size()), const_cast<char*>(picked.data()))) return false;
        } else {
            switch(pv.kind){
                case ParamKind::Int: {
                    int v = const_cast<ParamValue&>(pv).i; if(!DbcTools::bind(hStmt, static_cast<int>(i+1), v)) return false; break; }
                case ParamKind::Double: {
                    double v = const_cast<ParamValue&>(pv).d; if(!DbcTools::bind(hStmt, static_cast<int>(i+1), v)) return false; break; }
                case ParamKind::String: {
                    if(!DbcTools::bind(hStmt, static_cast<int>(i+1), pv.slen, const_cast<char*>(pv.s.data()))) return false; break; }
                case ParamKind::Timestamp: {
                    if(!DbcTools::bind(hStmt, static_cast<int>(i+1), const_cast<SQL_TIMESTAMP_STRUCT&>(pv.ts))) return false; break; }
            }
        }
    }
    if (!DbcTools::executePreparedStatement(hStmt)) {
        return false;
    }
    return true;
}
#endif

// Expose plan building for test without executing against DB.
std::vector<RewrittenStmt> RewriteDML::planDistrictUpdateRewrite(
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    std::vector<RewrittenStmt> plan;
    (void)buildRewrittenGenericUpdate("district", originalSql, old2new, newTableColumns, plan);
    return plan;
}

// Simplified INSERT rewrite for split/merge. We only handle direct column subsets and
// keep VALUES positional mapping. For now this supports the known NewOrder inserts:
// - orders(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local)
// - neworder(no_o_id,no_d_id,no_w_id)
// - orderline(ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_dist_info)
// If a target table contains a subset of these columns (by exact name match), we emit one
// INSERT with those columns and map parameters accordingly.
// ---- Lightweight INSERT parser ----
namespace {
struct ParsedInsertItem { std::string raw; bool isParam=false; int paramIdx=0; };
struct ParsedInsert { std::string tableLower; std::vector<std::string> colsLower; std::vector<ParsedInsertItem> items; };

static bool parseInsertSQL(const std::string& sql, const std::string& oldTableLower, ParsedInsert& out){
    std::string lower = toLowerCopy(sql);
    size_t posIns = lower.find("insert into");
    if(posIns==std::string::npos) return false;
    size_t p = posIns + std::string("insert into").size();
    // skip spaces
    while(p<lower.size() && std::isspace((unsigned char)lower[p])) ++p;
    // read table identifier
    size_t tStart=p; while(p<lower.size() && (std::isalnum((unsigned char)lower[p])||lower[p]=='_'||lower[p]=='.')) ++p;
    std::string tableName = lower.substr(tStart, p-tStart);
    out.tableLower = tableName;
    // optional column list
    while(p<lower.size() && std::isspace((unsigned char)lower[p])) ++p;
    size_t colEndPos = p;
    if(p<lower.size() && lower[p]=='('){
        int depth=1; size_t q = p+1; for(; q<lower.size(); ++q){ if(lower[q]=='(') depth++; else if(lower[q]==')'){ if(--depth==0) break; } }
        if(q>=lower.size()) return false;
        std::string colSec = sql.substr(p+1, q-(p+1));
        auto ranges = splitByCommaWithPos(colSec);
        for(const auto& r : ranges){ std::string c = trim(sql.substr(p+1 + r.first, r.second-r.first)); out.colsLower.push_back(toLowerCopy(c)); }
        colEndPos = q+1; // position after columns )
        p = colEndPos;
    }
    // find values
    size_t posValues = lower.find("values", colEndPos);
    if(posValues==std::string::npos) return false;
    size_t vp = posValues + std::string("values").size();
    while(vp<lower.size() && std::isspace((unsigned char)lower[vp])) ++vp;
    if(vp>=lower.size() || lower[vp] != '(') return false;
    int depth=1; size_t vq = vp+1; for(; vq<lower.size(); ++vq){ if(lower[vq]=='(') depth++; else if(lower[vq]==')'){ if(--depth==0) break; } }
    if(vq>=lower.size()) return false;
    std::string valSec = sql.substr(vp+1, vq-(vp+1));
    auto ranges = splitByCommaWithPos(valSec);
    int paramCounter=0;
    for(const auto& r : ranges){
        std::string raw = trim(sql.substr(vp+1 + r.first, r.second-r.first));
        ParsedInsertItem item; item.raw = raw;
        if(raw == "?") { item.isParam=true; item.paramIdx = ++paramCounter; }
        out.items.push_back(std::move(item));
    }

    // If columns absent, fallback known default order per table
    if(out.colsLower.empty()){
        if(oldTableLower=="orders"){
            out.colsLower = {"o_id","o_d_id","o_w_id","o_c_id","o_entry_d","o_carrier_id","o_ol_cnt","o_all_local"};
        } else if(oldTableLower=="neworder"){
            out.colsLower = {"no_o_id","no_d_id","no_w_id"};
        } else if(oldTableLower=="orderline"){
            out.colsLower = {"ol_o_id","ol_d_id","ol_w_id","ol_number","ol_i_id","ol_supply_w_id","ol_delivery_d","ol_quantity","ol_amount","ol_dist_info"};
        } else {
            return false;
        }
    }
    return true;
}
}

static bool buildInsertPlan(
    const std::string& oldTable,
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    std::vector<RewrittenStmt>& out)
{
    // Effective targets may be overridden by horizontal split/merge config
    std::vector<std::string> targets;
    {
        auto hs = Config::getHorizontalSplit();
        auto hm = Config::getHorizontalMerge();
        auto itHS = hs.find(oldTable);
        auto itHM = hm.find(oldTable);
        if (itHS != hs.end() && !itHS->second.empty()) {
            targets = itHS->second;
        } else if (itHM != hm.end() && !itHM->second.empty()) {
            targets = { itHM->second };
        } else {
            auto it = old2new.find(oldTable);
            if (it == old2new.end() || it->second.empty()) return false;
            targets = it->second;
        }
    }

    ParsedInsert pin;
    std::string oldLower = oldTable; std::transform(oldLower.begin(), oldLower.end(), oldLower.begin(), ::tolower);
    if(!parseInsertSQL(originalSql, oldLower, pin)) return false;

    // Map column index -> old param index (0 means constant)
    std::vector<int> colToParam(pin.colsLower.size(), 0);
    size_t count = std::min(pin.colsLower.size(), pin.items.size());
    for(size_t i=0;i<count;++i){ if(pin.items[i].isParam) colToParam[i] = pin.items[i].paramIdx; }

    auto renames = Config::getColumnRenames();
    auto extra   = Config::getExtraInsertCols();
    auto rmCols  = Config::getRemoveColumns();
    auto splitCols = Config::getColumnSplitColumns();
    auto splitDelims = Config::getColumnSplitDelims();

    for(const auto& t : targets){
        // If newTableColumns doesn't know the target (pure horizontal split/merge), we allow all columns
        bool strictCols = (newTableColumns.find(t) != newTableColumns.end());
        const auto& ncols = strictCols ? newTableColumns.at(t) : std::vector<std::string>{};
        auto has = [&](const std::string& c){
            if (!strictCols) return true;
            return std::find_if(ncols.begin(), ncols.end(), [&](const std::string& x){ return toLowerCopy(x)==c; }) != ncols.end();
        };
        auto isRemoved = [&](const std::string& colLower){
            auto itrm = rmCols.find(oldTable); if (itrm==rmCols.end()) return false; const auto& v = itrm->second;
            return std::find_if(v.begin(), v.end(), [&](const std::string& x){ return toLowerCopy(x)==colLower; }) != v.end();
        };
        auto renameCol = [&](const std::string& colLower)->std::string{
            auto itT = renames.find(oldTable); if (itT==renames.end()) return colLower;
            auto itC = itT->second.find(colLower); if (itC==itT->second.end()) return colLower;
            return toLowerCopy(itC->second);
        };

        std::string colList; std::string valList; std::vector<int> map; std::vector<std::string> paramCols; std::vector<std::string> pickedCols; std::vector<RewrittenStmt::SplitSpec> splitSpecs;
        for(size_t i=0;i<count;++i){
            std::string srcCol = pin.colsLower[i];
            std::string destCol = renameCol(srcCol);
            if (isRemoved(destCol)) continue;

            // Attribute split?
            bool didSplit=false;
            auto itSplitCols = splitCols.find(oldTable);
            if (itSplitCols != splitCols.end()){
                auto itSC = itSplitCols->second.find(srcCol);
                if (itSC != itSplitCols->second.end() && !itSC->second.empty()){
                    // Multi-column expansion
                    auto itDel = splitDelims.find(oldTable);
                    std::string delim = ",";
                    if (itDel != splitDelims.end()){
                        auto itD2 = itDel->second.find(srcCol); if (itD2!=itDel->second.end()) delim = itD2->second;
                    }
                    int oldParam = colToParam[i];
                    for(size_t k=0;k<itSC->second.size();++k){
                        std::string newCol = toLowerCopy(itSC->second[k]);
                        if (!has(newCol) || isRemoved(newCol)) continue;
                        if(!colList.empty()){ colList += ","; valList += ","; }
                        colList += newCol; pickedCols.push_back(newCol);
                        if (oldParam != 0 && pin.items[i].isParam){
                            valList += "?"; map.push_back(oldParam); paramCols.push_back(newCol);
                            RewrittenStmt::SplitSpec sp; sp.isSplit=true; sp.tokenIndex=static_cast<int>(k); sp.delim=delim; splitSpecs.push_back(sp);
                        } else {
                            // If literal, we cannot split reliably at plan time; keep original raw
                            valList += pin.items[i].raw;
                        }
                    }
                    didSplit = true;
                }
            }
            if (didSplit) continue;

            // Normal one-to-one mapping
            if(!has(destCol)) continue;
            if(!colList.empty()){ colList += ","; valList += ","; }
            colList += destCol;
            pickedCols.push_back(destCol);
            if(colToParam[i] != 0){ valList += "?"; map.push_back(colToParam[i]); paramCols.push_back(destCol); RewrittenStmt::SplitSpec sp; sp.isSplit=false; splitSpecs.push_back(sp); }
            else { valList += pin.items[i].raw; }
        }
        // Extra insert columns (e.g., redundant values from FK); raw SQL snippets
        auto itEx = extra.find(t);
        if (itEx != extra.end()){
            for (const auto& kv : itEx->second){
                std::string col = toLowerCopy(kv.first);
                if (isRemoved(col)) continue;
                // avoid duplicate
                bool exists=false; for(const auto& pc : pickedCols){ if (pc==col){ exists=true; break; } }
                if (exists) continue;
                if(!colList.empty()){ colList += ","; valList += ","; }
                colList += col; pickedCols.push_back(col);
                valList += kv.second; // expression literal
            }
        }
        if(colList.empty()) continue; // nothing for this table

        RewrittenStmt rs; rs.sql = std::string("insert into ") + t + "(" + colList + ") values (" + valList + ")";
        rs.newToOldIndex = std::move(map);
        rs.paramColsLower = std::move(paramCols);
        rs.colsLower = std::move(pickedCols);
        rs.targetLower = toLowerCopy(t);
        rs.splitSpecs = std::move(splitSpecs);
        out.push_back(std::move(rs));
    }
    return !out.empty();
}

#ifndef REWRITE_NO_ODBC
static bool prepareBindExecuteInsert(SQLHDBC hDBC, const RewrittenStmt& stmt, const ParamPack& params) {
    SQLHSTMT hStmt = 0;
    if (!DbcTools::allocAndPrepareStmt(hDBC, hStmt, stmt.sql.c_str())) return false;
    for (size_t i = 0; i < stmt.newToOldIndex.size(); ++i) {
        int oldPos = stmt.newToOldIndex[i];
        if (oldPos < 1 || static_cast<size_t>(oldPos) > params.params.size()) return false;
        const ParamValue& pv = params.params[static_cast<size_t>(oldPos - 1)];
        // Optional attribute split
        bool useSplit = (i < stmt.splitSpecs.size() && stmt.splitSpecs[i].isSplit);
        if (useSplit) {
            // Split only supported for string params
            if (pv.kind != ParamKind::String) return false;
            std::string s = pv.s;
            const std::string& delim = stmt.splitSpecs[i].delim;
            int tokenIdx = stmt.splitSpecs[i].tokenIndex; // 0-based
            // simple split
            std::vector<std::string> tokens; size_t start=0; while(true){ size_t pos = s.find(delim, start); if(pos==std::string::npos){ tokens.push_back(s.substr(start)); break; } tokens.push_back(s.substr(start, pos-start)); start = pos + delim.size(); }
            std::string picked = (tokenIdx >=0 && tokenIdx < static_cast<int>(tokens.size())) ? tokens[tokenIdx] : std::string();
            // bind as string
            if (!DbcTools::bind(hStmt, static_cast<int>(i+1), static_cast<int>(picked.size()), const_cast<char*>(picked.data()))) return false;
        } else {
            switch (pv.kind) {
                case ParamKind::Int:
                    if (!DbcTools::bind(hStmt, static_cast<int>(i+1), const_cast<int&>(pv.i))) return false;
                    break;
                case ParamKind::Double:
                    if (!DbcTools::bind(hStmt, static_cast<int>(i+1), const_cast<double&>(pv.d))) return false;
                    break;
                case ParamKind::String: {
                    if (!DbcTools::bind(hStmt, static_cast<int>(i+1), pv.slen, const_cast<char*>(pv.s.data()))) return false;
                    break;
                }
                case ParamKind::Timestamp:
                    if (!DbcTools::bind(hStmt, static_cast<int>(i+1), const_cast<SQL_TIMESTAMP_STRUCT&>(pv.ts))) return false;
                    break;
            }
        }
    }
    if (!DbcTools::executePreparedStatement(hStmt)) return false;
    return true;
}

bool RewriteDML::maybeRewriteAndExecInsert(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    std::vector<RewrittenStmt> plan;
    if (!buildInsertPlan(oldTable, originalSql, old2new, newTableColumns, plan)) {
        return false;
    }
    for (const auto& rs : plan) {
        if (!prepareBindExecuteInsert(hDBC, rs, params)) return false;
    }
    return true;
}
#endif // REWRITE_NO_ODBC

#ifndef REWRITE_NO_ODBC
RewriteResult RewriteDML::tryRewriteAndExecUpdate(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    auto it = old2new.find(oldTable);
    if (it == old2new.end()) return RewriteResult::NotApplicable;
    std::vector<RewrittenStmt> plan;
    if (!buildRewrittenGenericUpdate(oldTable, originalSql, old2new, newTableColumns, plan))
        return RewriteResult::NotApplicable;
    for (const auto& rs : plan) {
        if (!prepareBindExecute(hDBC, rs, params)) return RewriteResult::Failed;
    }
    return RewriteResult::AppliedOk;
}

RewriteResult RewriteDML::tryRewriteAndExecInsert(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    auto it = old2new.find(oldTable);
    if (it == old2new.end()) return RewriteResult::NotApplicable;
    std::vector<RewrittenStmt> plan;
    if (!buildInsertPlan(oldTable, originalSql, old2new, newTableColumns, plan))
        return RewriteResult::NotApplicable;
    for (const auto& rs : plan) {
        if (!prepareBindExecuteInsert(hDBC, rs, params)) return RewriteResult::Failed;
    }
    return RewriteResult::AppliedOk;
}
#endif

#ifndef REWRITE_NO_ODBC
// ---------- Advanced, transaction-aware helpers ----------
namespace {
static bool isKeyLike(const std::string& c) {
    std::string s = c; std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    static const std::set<std::string> keys = {
        "o_id","o_d_id","o_w_id",
        "no_o_id","no_d_id","no_w_id",
        "ol_o_id","ol_d_id","ol_w_id","ol_number","ol_i_id","ol_supply_w_id",
        "s_i_id","s_w_id"
    };
    if (keys.count(s)) return true;
    if (s.size()>=3 && (s.find("_id")!=std::string::npos || (s.size()>=2 && s.substr(s.size()-2)=="id"))) return true;
    return false;
}

static std::string pvToLiteral(const ParamValue& pv) {
    std::ostringstream oss;
    switch(pv.kind){
        case ParamKind::Int: oss << pv.i; break;
        case ParamKind::Double: {
            oss.setf(std::ios::fixed); oss.precision(6); oss << pv.d; break;
        }
        case ParamKind::String: {
            oss << "'";
            for(char ch : pv.s){ if(ch=='\'') oss << "''"; else oss << ch; }
            oss << "'"; break;
        }
        case ParamKind::Timestamp: {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "'%04d-%02d-%02d %02d:%02d:%02d'",
                pv.ts.year, pv.ts.month, pv.ts.day, pv.ts.hour, pv.ts.minute, pv.ts.second);
            oss << buf; break;
        }
    }
    return oss.str();
}

static std::string paramToString(const ParamValue& pv) {
    std::ostringstream oss;
    switch(pv.kind){
        case ParamKind::Int: oss << pv.i; break;
        case ParamKind::Double: { oss.setf(std::ios::fixed); oss.precision(6); oss << pv.d; break; }
        case ParamKind::String: oss << pv.s; break;
        case ParamKind::Timestamp: {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                pv.ts.year, pv.ts.month, pv.ts.day, pv.ts.hour, pv.ts.minute, pv.ts.second);
            oss << buf; break;
        }
    }
    return oss.str();
}

static std::string computeSignature(const RewrittenStmt& rs, const ParamPack& params) {
    // Prefer key-like columns; fallback to all params
    std::vector<std::pair<std::string,std::string>> items; // col->value
    for (size_t i=0;i<rs.newToOldIndex.size() && i<rs.paramColsLower.size();++i){
        int oldPos = rs.newToOldIndex[i];
        if (oldPos < 1 || static_cast<size_t>(oldPos) > params.params.size()) continue;
        const ParamValue& pv = params.params[static_cast<size_t>(oldPos-1)];
        items.emplace_back(rs.paramColsLower[i], paramToString(pv));
    }
    std::ostringstream oss;
    oss << rs.targetLower << "|";
    // 1) 按配置的表键优先
    auto tableKeys = Config::getTableKeys();
    auto kit = tableKeys.find(rs.targetLower);
    if (kit != tableKeys.end() && !kit->second.empty()) {
        bool any=false;
        for (const auto& k : kit->second) {
            std::string kl = toLowerCopy(k);
            for (const auto& kv : items) {
                if (kv.first == kl) {
                    if (any) oss << ";"; any=true; oss << kv.first << "=" << kv.second; break;
                }
            }
        }
        if (any) return oss.str();
    }
    // 2) 没有配置键时，按“像 key 的列”
    {
        bool any=false;
        for (const auto& kv : items) {
            if (!isKeyLike(kv.first)) continue;
            if (any) oss << ";"; any=true;
            oss << kv.first << "=" << kv.second;
        }
        if (any) return oss.str();
    }
    // 3) 最后退化所有参数
    {
        bool any=false;
        for (const auto& kv : items) { if (any) oss << ";"; any=true; oss << kv.first << "=" << kv.second; }
        return oss.str();
    }
}

static bool prepareBindExecuteInsertCustom(SQLHDBC hDBC, const std::string& sql,
                                           const std::vector<int>& newToOldIndex,
                                           const ParamPack& params) {
    SQLHSTMT hStmt = 0;
    if (!DbcTools::allocAndPrepareStmt(hDBC, hStmt, sql.c_str())) return false;
    for (size_t i = 0; i < newToOldIndex.size(); ++i) {
        int oldPos = newToOldIndex[i];
        if (oldPos < 1 || static_cast<size_t>(oldPos) > params.params.size()) return false;
        const ParamValue& pv = params.params[static_cast<size_t>(oldPos - 1)];
        switch (pv.kind) {
            case ParamKind::Int: {
                int v = const_cast<ParamValue&>(pv).i;
                if (!DbcTools::bind(hStmt, static_cast<int>(i+1), v)) return false; break;
            }
            case ParamKind::Double: {
                double v = const_cast<ParamValue&>(pv).d;
                if (!DbcTools::bind(hStmt, static_cast<int>(i+1), v)) return false; break;
            }
            case ParamKind::String: {
                if (!DbcTools::bind(hStmt, static_cast<int>(i+1), pv.slen, const_cast<char*>(pv.s.data()))) return false; break;
            }
            case ParamKind::Timestamp: {
                if (!DbcTools::bind(hStmt, static_cast<int>(i+1), const_cast<SQL_TIMESTAMP_STRUCT&>(pv.ts))) return false; break;
            }
        }
    }
    if (!DbcTools::executePreparedStatement(hStmt)) return false;
    return true;
}
}

RewriteResult RewriteDML::tryRewriteAndExecInsertWithCtx(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    const std::map<std::string, std::vector<std::string>>& /*oldTableColumns*/,
    TxnRewriteCtx& ctx
) {
    std::vector<RewrittenStmt> plan;
    if (!buildInsertPlan(oldTable, originalSql, old2new, newTableColumns, plan))
        return RewriteResult::NotApplicable;
    bool executedAny=false;
    for (const auto& rs : plan) {
        std::string sig = computeSignature(rs, params);
        auto& setRef = ctx.seenInsertSig[rs.targetLower];
        if (setRef.find(sig) != setRef.end()) {
            continue; // dedup
        }
        if (!prepareBindExecuteInsert(hDBC, rs, params)) return RewriteResult::Failed;
        setRef.insert(sig);
        executedAny = true;
    }
    return executedAny ? RewriteResult::AppliedOk : RewriteResult::NotApplicable;
}

namespace {
static bool buildColValueMapForInsert(const std::string& oldTable,
                                      const std::string& originalSql,
                                      const ParamPack& params,
                                      std::unordered_map<std::string, StagedColValue>& out) {
    ParsedInsert pin; std::string oldLower = oldTable; std::transform(oldLower.begin(), oldLower.end(), oldLower.begin(), ::tolower);
    if(!parseInsertSQL(originalSql, oldLower, pin)) return false;
    // Build map col-> value (literal or param)
    size_t count = std::min(pin.colsLower.size(), pin.items.size());
    for(size_t i=0;i<count;++i){
        const auto& col = pin.colsLower[i];
        const auto& item = pin.items[i];
        StagedColValue scv;
        if (item.isParam) {
            int idx = item.paramIdx;
            if (idx < 1 || static_cast<size_t>(idx) > params.params.size()) continue;
            scv.isLiteral = false;
            scv.pv = params.params[static_cast<size_t>(idx-1)];
        } else {
            scv.isLiteral = true;
            scv.literal = item.raw;
        }
        out[toLowerCopy(col)] = scv;
    }
    return true;
}

static bool computeOrderKeyFromMap(const std::unordered_map<std::string, StagedColValue>& m, std::string& key) {
    auto get = [&](const char* c, std::string& out)->bool{
        auto it = m.find(c); if(it==m.end()) return false; const auto& v = it->second; if(v.isLiteral) { out = v.literal; return true; } out = paramToString(v.pv); return true; };
    std::string w,d,o;
    if (get("o_w_id", w) && get("o_d_id", d) && get("o_id", o)) { key = w+"|"+d+"|"+o; return true; }
    if (get("no_w_id", w) && get("no_d_id", d) && get("no_o_id", o)) { key = w+"|"+d+"|"+o; return true; }
    if (get("ol_w_id", w) && get("ol_d_id", d) && get("ol_o_id", o)) { key = w+"|"+d+"|"+o; return true; }
    return false;
}
}

RewriteResult RewriteDML::deferMergeInsert(
    SQLHDBC /*hDBC*/,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    const std::map<std::string, std::vector<std::string>>& /*oldTableColumns*/,
    TxnRewriteCtx& ctx
) {
    // Heuristic: only stage for known outside-loop tables to support merge with orderline
    std::string low = toLowerCopy(oldTable);
    if (!(low=="orders" || low=="neworder")) return RewriteResult::NotApplicable;

    auto it = old2new.find(oldTable);
    if (it == old2new.end() || it->second.empty()) return RewriteResult::NotApplicable;

    std::unordered_map<std::string, StagedColValue> colmap;
    if (!buildColValueMapForInsert(oldTable, originalSql, params, colmap)) return RewriteResult::NotApplicable;
    std::string key; if (!computeOrderKeyFromMap(colmap, key)) return RewriteResult::NotApplicable;

    bool stagedAny=false;
    for (const auto& t : it->second) {
        if (newTableColumns.find(t) == newTableColumns.end()) continue;
        std::string k = toLowerCopy(t) + "|" + key;
        auto& rec = ctx.pendingByKey[k];
        rec.target = t;
        // merge columns; prefer existing
        for (const auto& kv : colmap) {
            if (rec.colValues.find(kv.first) == rec.colValues.end()) rec.colValues[kv.first] = kv.second;
        }
        stagedAny=true;
    }
    return stagedAny ? RewriteResult::AppliedOk : RewriteResult::NotApplicable;
}

RewriteResult RewriteDML::flushMergeInsert(
    SQLHDBC hDBC,
    const std::string& oldTable,
    const std::string& originalSql,
    const ParamPack& params,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns,
    const std::map<std::string, std::vector<std::string>>& /*oldTableColumns*/,
    TxnRewriteCtx& ctx
) {
    auto it = old2new.find(oldTable);
    if (it == old2new.end() || it->second.empty()) return RewriteResult::NotApplicable;

    // Build col->value (orderline row)
    std::unordered_map<std::string, StagedColValue> lineMap;
    if (!buildColValueMapForInsert(oldTable, originalSql, params, lineMap)) return RewriteResult::NotApplicable;
    std::string key; if (!computeOrderKeyFromMap(lineMap, key)) return RewriteResult::NotApplicable;

    bool didAny=false;
    for (const auto& t : it->second) {
        std::string tk = toLowerCopy(t) + "|" + key;
        auto pit = ctx.pendingByKey.find(tk);
        if (pit == ctx.pendingByKey.end()) continue; // no staged header for this target
        // union columns that exist in target definition
        auto ntIt = newTableColumns.find(t);
        if (ntIt == newTableColumns.end()) continue;
        const auto& ncols = ntIt->second;
        auto has = [&](const std::string& c){ return std::find_if(ncols.begin(), ncols.end(), [&](const std::string& x){ return toLowerCopy(x)==c; }) != ncols.end(); };

        // 按新表列顺序构造最终列序列：依次挑选在 staged 或 lineMap 中存在的列
        std::vector<std::string> cols;
        for (const auto& nc : ncols) {
            std::string c = toLowerCopy(nc);
            if (pit->second.colValues.find(c) != pit->second.colValues.end() || lineMap.find(c) != lineMap.end()) {
                cols.push_back(c);
            }
        }
        if (cols.empty()) continue;

        // We need accurate newToOldIdx mapping for placeholders. Build it by re-parsing original for param indices per column.
        ParsedInsert pin;
        std::string oldLower = oldTable; std::transform(oldLower.begin(), oldLower.end(), oldLower.begin(), ::tolower);
        if(!parseInsertSQL(originalSql, oldLower, pin)) continue;
        std::unordered_map<std::string,int> colParamIdx;
        size_t cnt = std::min(pin.colsLower.size(), pin.items.size());
        for(size_t i=0;i<cnt;++i){ if(pin.items[i].isParam) colParamIdx[toLowerCopy(pin.colsLower[i])] = pin.items[i].paramIdx; }

        std::string colList; std::string valList; std::vector<int> newToOldIdx;
        for (const auto& c : cols) {
            if (!colList.empty()) { colList += ","; valList += ","; }
            colList += c;
            auto itParam = colParamIdx.find(c);
            auto lineIt2 = lineMap.find(c);
            if (itParam != colParamIdx.end() && lineIt2 != lineMap.end() && !lineIt2->second.isLiteral) {
                // 列来自循环内参数，占位符顺序按 newToOldIdx 推入
                valList += "?"; newToOldIdx.push_back(itParam->second);
            } else if (lineIt2 != lineMap.end()) {
                // 列来自循环内字面量
                valList += (lineIt2->second.isLiteral ? lineIt2->second.literal : pvToLiteral(lineIt2->second.pv));
            } else {
                // 列来自循环外阶段化（orders/neworder）
                const auto& scv = pit->second.colValues.at(c);
                valList += (scv.isLiteral ? scv.literal : pvToLiteral(scv.pv));
            }
        }

        // 按新表列顺序生成最终 SQL，并保持参数映射顺序与 newToOldIdx 一致
        std::string sql = std::string("insert into ") + t + "(" + colList + ") values (" + valList + ")";

        // Dedup by key and param signature
        RewrittenStmt rs; rs.targetLower = toLowerCopy(t); rs.paramColsLower.clear(); rs.newToOldIndex = newToOldIdx;
        for (const auto& c : cols) { if (colParamIdx.find(c) != colParamIdx.end()) rs.paramColsLower.push_back(c); }
        std::string sig;
        if (!rs.newToOldIndex.empty()) sig = computeSignature(rs, params); else sig = rs.targetLower + "|" + key;
        auto& setRef = ctx.seenInsertSig[rs.targetLower];
        if (setRef.find(sig) != setRef.end()) continue;
        if (!prepareBindExecuteInsertCustom(hDBC, sql, newToOldIdx, params)) return RewriteResult::Failed;
        setRef.insert(sig);
        didAny = true;
    }

    return didAny ? RewriteResult::AppliedOk : RewriteResult::NotApplicable;
}
#endif // REWRITE_NO_ODBC
// Plan-only wrappers
std::vector<RewrittenStmt> RewriteDML::planGenericUpdateRewrite(
    const std::string& oldTable,
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    std::vector<RewrittenStmt> plan;
    (void)buildRewrittenGenericUpdate(oldTable, originalSql, old2new, newTableColumns, plan);
    return plan;
}

std::vector<RewrittenStmt> RewriteDML::planInsertRewrite(
    const std::string& oldTable,
    const std::string& originalSql,
    const std::map<std::string, std::vector<std::string>>& old2new,
    const std::map<std::string, std::vector<std::string>>& newTableColumns
) {
    std::vector<RewrittenStmt> plan;
    (void)buildInsertPlan(oldTable, originalSql, old2new, newTableColumns, plan);
    return plan;
}

#ifndef REWRITE_NO_ODBC
void RewriteDML::stageExtraForOrderKey(
    TxnRewriteCtx& ctx,
    const std::string& targetTable,
    int wId,
    int dId,
    int oId,
    const std::string& colLower,
    const ParamValue& value
) {
    std::string key = toLowerCopy(targetTable) + "|" + std::to_string(wId) + "|" + std::to_string(dId) + "|" + std::to_string(oId);
    auto& rec = ctx.pendingByKey[key];
    rec.target = targetTable;
    StagedColValue scv; scv.isLiteral=false; scv.pv = value;
    rec.colValues[toLowerCopy(colLower)] = scv;
}
#endif
