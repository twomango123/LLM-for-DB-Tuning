/*
Copyright 2014 Florian Wolf, SAP AG

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

#ifndef CONFIG_H
#define CONFIG_H

// For offline rewrite-plan tests, allow building without ODBC headers by
// defining REWRITE_NO_ODBC. Production build should NOT define it.
#ifndef REWRITE_NO_ODBC
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#endif
#include <string>
#include <map>
#include <vector>

class Config{

	private:
		static int TYPE;

		static std::string DATA_SOURCE_NAME;
		static std::string DBS_USER;
		static std::string DBS_PASSWORD;

		static int ANALYTICAL_CLIENTS;
		static int TRANSACTIONAL_CLIENTS;

		static int WARMUP_DURATION_IN_S;
		static int TEST_DURATION_IN_S;
		static int WAREHOUSE_COUNT;
		static std::string INITIAL_DB_CREATION_PATH;
		static std::string OUTPUT_PATH;

		// Schema tuning mappings (optional). When empty, rewriting is skipped.
		static std::map<std::string, std::vector<std::string>> OLD2NEW;
		static std::map<std::string, std::vector<std::string>> NEW_TABLE_COLUMNS;
		// Optional: original (old) table columns metadata to assist txn-level rewrites
		static std::map<std::string, std::vector<std::string>> OLD_TABLE_COLUMNS;
		// Optional: table key columns (for both old/new), used for dedupe/signature, by table name
		static std::map<std::string, std::vector<std::string>> TABLE_KEYS;

		// Advanced rewrite configuration (optional)
		// 1) Column renames: oldTable -> (oldColumn -> newColumn)
		static std::map<std::string, std::map<std::string, std::string>> COLUMN_RENAMES;
		// 2) Extra insert columns: targetTable -> (column -> SQL literal/expression)
		static std::map<std::string, std::map<std::string, std::string>> EXTRA_INSERT_COLS;
		// 2b) Extra update SET expressions: targetTable -> (column -> SQL literal/expression)
		static std::map<std::string, std::map<std::string, std::string>> EXTRA_UPDATE_SETS;

		// 3) Horizontal split: oldTable -> [child tables]
		static std::map<std::string, std::vector<std::string>> HORIZONTAL_SPLIT;
		// 4) Horizontal merge: oldTable -> mergedTable
		static std::map<std::string, std::string> HORIZONTAL_MERGE;

		// 5) Attribute split configuration
		//    - Columns to split: table -> (oldColumn -> [newColumns...])
		static std::map<std::string, std::map<std::string, std::vector<std::string>>> COLUMN_SPLIT_COLUMNS;
		//    - Delimiter for each split column: table -> (oldColumn -> delimiter)
		static std::map<std::string, std::map<std::string, std::string>> COLUMN_SPLIT_DELIMS;

		// 6) Remove columns (redundant) from DML: table -> [columns...]
		static std::map<std::string, std::vector<std::string>> REMOVE_COLUMNS;
		
		static const char CSV_DELIMITER = '|';

		static int is(const char* value, int argc, char* argv[]);
		static bool is(const char* value, int argc, char* argv[], int* dest);
		static bool is(const char* value, int argc, char* argv[], std::string* dest);

	public:				
		static bool initialize(int argc, char* argv[]);
    // Only available when ODBC is present
#ifndef REWRITE_NO_ODBC
    static bool warehouseDetection(SQLHSTMT& hStmt);
#endif

		static int getType();

		static std::string getDataSourceName();
		static std::string getDbsUser();
		static std::string getDbsPassword();

		static int getAnalyticalClients();
		static int getTransactionalClients();

		static int getWarmupDurationInS();
		static int getTestDurationInS();
		static int getWarehouseCount();

		static std::string getOutputPath();
		static std::string getInitialDbCreationPath();
		static char getCsvDelim();

		// Schema tuning mapping accessors
		// Note: these return copies intentionally to keep thread-safety simple.
		static std::map<std::string, std::vector<std::string>> getOld2New();
		static std::map<std::string, std::vector<std::string>> getnewTableColumns();
		static std::map<std::string, std::vector<std::string>> getOldTableColumns();
		static std::map<std::string, std::vector<std::string>> getTableKeys();
		static void setOld2New(const std::map<std::string, std::vector<std::string>>& m);
		static void setNewTableColumns(const std::map<std::string, std::vector<std::string>>& m);
		static void setOldTableColumns(const std::map<std::string, std::vector<std::string>>& m);
		static void setTableKeys(const std::map<std::string, std::vector<std::string>>& m);

		// Advanced rewrite config accessors
		static std::map<std::string, std::map<std::string, std::string>> getColumnRenames();
		static void setColumnRenames(const std::map<std::string, std::map<std::string, std::string>>& m);
		static std::map<std::string, std::map<std::string, std::string>> getExtraInsertCols();
		static void setExtraInsertCols(const std::map<std::string, std::map<std::string, std::string>>& m);
		static std::map<std::string, std::map<std::string, std::string>> getExtraUpdateSets();
		static void setExtraUpdateSets(const std::map<std::string, std::map<std::string, std::string>>& m);

		static std::map<std::string, std::vector<std::string>> getHorizontalSplit();
		static void setHorizontalSplit(const std::map<std::string, std::vector<std::string>>& m);
		static std::map<std::string, std::string> getHorizontalMerge();
		static void setHorizontalMerge(const std::map<std::string, std::string>& m);

		static std::map<std::string, std::map<std::string, std::vector<std::string>>> getColumnSplitColumns();
		static void setColumnSplitColumns(const std::map<std::string, std::map<std::string, std::vector<std::string>>>& m);
		static std::map<std::string, std::map<std::string, std::string>> getColumnSplitDelims();
		static void setColumnSplitDelims(const std::map<std::string, std::map<std::string, std::string>>& m);

		static std::map<std::string, std::vector<std::string>> getRemoveColumns();
		static void setRemoveColumns(const std::map<std::string, std::vector<std::string>>& m);

};

#endif
