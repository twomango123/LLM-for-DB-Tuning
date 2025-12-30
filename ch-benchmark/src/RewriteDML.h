/*
 Minimal DML rewrite interface for UPDATE-only district case.
 This is a pluggable, non-intrusive helper. Internals can be filled later.
*/

#ifndef REWRITE_DML_H
#define REWRITE_DML_H

// For offline unit tests, allow building without ODBC headers by defining
// REWRITE_NO_ODBC. Production build should NOT define it.
#ifndef REWRITE_NO_ODBC
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#endif

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

enum class ParamKind { Int, Double, String, Timestamp };

struct ParamValue {
    ParamKind kind;
    int i = 0;
    double d = 0.0;
    std::string s; // store content for fixed strings
    int slen = 0;  // explicit length for fixed strings
#ifndef REWRITE_NO_ODBC
    SQL_TIMESTAMP_STRUCT ts;
#endif
};

struct ParamPack {
    // Capture positional parameters in their original order (1-based mapping).
    std::vector<ParamValue> params;
    void addInt(int v) {
        ParamValue p; p.kind = ParamKind::Int; p.i = v; params.push_back(p);
    }
    void addDouble(double v) {
        ParamValue p; p.kind = ParamKind::Double; p.d = v; params.push_back(p);
    }
    void addFixedString(const char* data, int len) {
        ParamValue p; p.kind = ParamKind::String; p.s.assign(data, data + len); p.slen = len; params.push_back(p);
    }
#ifndef REWRITE_NO_ODBC
    void addTimestamp(const SQL_TIMESTAMP_STRUCT& v) {
        ParamValue p; p.kind = ParamKind::Timestamp; p.ts = v; params.push_back(p);
    }
#endif
};

enum class RewriteResult { NotApplicable = 0, AppliedOk = 1, Failed = 2 };

struct RewrittenStmt {
    std::string sql;                 // new SQL text
    std::vector<int> newToOldIndex;  // new positional param -> old positional index (1-based)
    std::vector<std::string> colsLower; // optional: columns in target insert/update (lowercase)
    std::string targetLower;         // optional: target table name in lower case
    std::vector<std::string> paramColsLower; // columns corresponding to bound params in order
    // Optional per-parameter split info (for attribute split, INSERT only for now)
    struct SplitSpec { bool isSplit=false; int tokenIndex=0; std::string delim; };
    std::vector<SplitSpec> splitSpecs; // size == newToOldIndex.size() when used
};

struct StagedColValue {
    bool isLiteral = false;      // true: use literal in SQL; false: bind param value
    std::string literal;         // e.g., NULL or numeric/text literal as appears in SQL
    ParamValue pv;               // captured parameter value when isLiteral == false
};

// Per-transaction rewrite context for advanced behaviors (dedupe, deferred merge)
struct TxnRewriteCtx {
    // For split tables: dedupe repeated identical inserts within a txn
    std::unordered_map<std::string, std::unordered_set<std::string>> seenInsertSig; // target -> set(signature)

    // For merges across in-loop/out-of-loop: stage outside-loop inserts to be combined inside
    struct PendingHalfRow {
        std::string target;
        // Map lowercased column -> staged value (literal or param)
        std::unordered_map<std::string, StagedColValue> colValues;
    };
    // key: target + '|' + logical order key (w|d|o)
    std::unordered_map<std::string, PendingHalfRow> pendingByKey;
};

class RewriteDML {
public:
    // Try to rewrite and execute an UPDATE on oldTable. Returns true if handled.
    // If returns false, caller should fallback to original prepared statement.
#ifndef REWRITE_NO_ODBC
    static bool maybeRewriteAndExecUpdate(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );
#endif

    // Test helper: build the district UPDATE rewrite plan without executing.
    // Returns the list of rewritten statements with their parameter mappings.
    static std::vector<RewrittenStmt> planDistrictUpdateRewrite(
        const std::string& originalSql,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

    // Plan-only helpers (no DB needed)
    static std::vector<RewrittenStmt> planGenericUpdateRewrite(
        const std::string& oldTable,
        const std::string& originalSql,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

    static std::vector<RewrittenStmt> planInsertRewrite(
        const std::string& oldTable,
        const std::string& originalSql,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

#ifndef REWRITE_NO_ODBC
    // INSERT support for executeNewOrder (orders/neworder/orderline)
    static bool maybeRewriteAndExecInsert(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

    // Tri-state versions: NotApplicable, AppliedOk, Failed (no fallback on Failed)
    static RewriteResult tryRewriteAndExecUpdate(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

    static RewriteResult tryRewriteAndExecInsert(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns
    );

    // Advanced, transaction-aware insert rewrites
    static RewriteResult tryRewriteAndExecInsertWithCtx(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns,
        const std::map<std::string, std::vector<std::string>>& oldTableColumns,
        TxnRewriteCtx& ctx
    );

    // Defer inserts to merged targets when coming from outside the loop (e.g., orders/neworder)
    static RewriteResult deferMergeInsert(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns,
        const std::map<std::string, std::vector<std::string>>& oldTableColumns,
        TxnRewriteCtx& ctx
    );

    // Inside the loop, flush deferred merges by combining staged values with current row (e.g., orderline)
    static RewriteResult flushMergeInsert(
        SQLHDBC hDBC,
        const std::string& oldTable,
        const std::string& originalSql,
        const ParamPack& params,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns,
        const std::map<std::string, std::vector<std::string>>& oldTableColumns,
        TxnRewriteCtx& ctx
    );

    // Stage extra column value for a given (w,d,o) logical key on a target merged table.
    // This supports runtime-fetched redundant values from FK tables.
    static void stageExtraForOrderKey(
        TxnRewriteCtx& ctx,
        const std::string& targetTable,
        int wId,
        int dId,
        int oId,
        const std::string& colLower,
        const ParamValue& value
    );
#endif

private:
    static bool buildRewrittenForDistrict(
        const std::string& originalSql,
        const std::map<std::string, std::vector<std::string>>& old2new,
        const std::map<std::string, std::vector<std::string>>& newTableColumns,
        std::vector<RewrittenStmt>& out
    );

#ifndef REWRITE_NO_ODBC
    static bool prepareBindExecute(
        SQLHDBC hDBC,
        const RewrittenStmt& stmt,
        const ParamPack& params
    );
#endif
};

#endif // REWRITE_DML_H
