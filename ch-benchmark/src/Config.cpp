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

#include "Config.h"
#include "DbcTools.h"
#include "Log.h"
#include "dialect/DialectStrategy.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <map>
#include <vector>

using namespace std;

int Config::TYPE;

string Config::DATA_SOURCE_NAME;
string Config::DBS_USER;
string Config::DBS_PASSWORD;

int Config::ANALYTICAL_CLIENTS=-1;
int Config::TRANSACTIONAL_CLIENTS=-1;

int Config::WAREHOUSE_COUNT=-1;
int Config::WARMUP_DURATION_IN_S=-1;
int Config::TEST_DURATION_IN_S=-1;
string Config::INITIAL_DB_CREATION_PATH;
string Config::OUTPUT_PATH;

// Schema tuning mappings (optional). When empty, rewriting is skipped.
std::map<std::string, std::vector<std::string>> Config::OLD2NEW = {};
std::map<std::string, std::vector<std::string>> Config::NEW_TABLE_COLUMNS = {};
std::map<std::string, std::vector<std::string>> Config::OLD_TABLE_COLUMNS = {};
std::map<std::string, std::vector<std::string>> Config::TABLE_KEYS = {};
std::map<std::string, std::map<std::string, std::string>> Config::COLUMN_RENAMES = {};
std::map<std::string, std::map<std::string, std::string>> Config::EXTRA_INSERT_COLS = {};
std::map<std::string, std::map<std::string, std::string>> Config::EXTRA_UPDATE_SETS = {};
std::map<std::string, std::vector<std::string>> Config::HORIZONTAL_SPLIT = {};
std::map<std::string, std::string> Config::HORIZONTAL_MERGE = {};
std::map<std::string, std::map<std::string, std::vector<std::string>>> Config::COLUMN_SPLIT_COLUMNS = {};
std::map<std::string, std::map<std::string, std::string>> Config::COLUMN_SPLIT_DELIMS = {};
std::map<std::string, std::vector<std::string>> Config::REMOVE_COLUMNS = {};


int Config::is(const char* value, int argc, char* argv[]){
	for(int i=1; i<argc; i++){
		if(strcoll(argv[i],value)==0)
			return i;
	}
	return 0;
}

bool Config::is(const char* value, int argc, char* argv[], int* dest){
	int temp = is(value,argc,argv);
	if(temp){
		*dest = strtol(argv[temp+1],NULL,0);
		if(*dest<0){
			cout << "Unkown command\ntype: chBenchmark --help" << endl;
			return 0;
		}
	}
	else{
		cout << "Unkown command\ntype: chBenchmark --help" << endl;
		return 0;
	}
	return 1;
}

bool Config::is(const char* value, int argc, char* argv[], string* dest){
	int temp = is(value,argc,argv);
	if(temp){
		*dest = argv[temp+1];
		if(*dest==""){
			cout << "Unkown command\ntype: chBenchmark --help" << endl;
			return 0;
		}
	}
	else{
		cout << "Unkown command\ntype: chBenchmark --help" << endl;
		return 0;
	}
	return 1;
}

bool Config::initialize(int argc, char* argv[]){

	if(is("-csv",argc,argv)){

		TYPE = 1;
		
		//Warehouse count
		if(!is("-wh",argc,argv,&WAREHOUSE_COUNT))
			return 0;

		//Path to csv output
		if(!is("-pa",argc,argv,&INITIAL_DB_CREATION_PATH))
			return 0;

	}else if(is("-run",argc,argv)){

		TYPE = 2;

		//IP Adress and port of target DBS
		if(!is("-dsn",argc,argv,&DATA_SOURCE_NAME))
			return 0;

		//User on target DBS
		if(!is("-usr",argc,argv,&DBS_USER))
			return 0;

		//Password of user on target DBS
		if(!is("-pwd",argc,argv,&DBS_PASSWORD))
			return 0;

		//Number of transctional clients
		if(!is("-a",argc,argv,&ANALYTICAL_CLIENTS))
			return 0;

		//Number of analytical clients
		if(!is("-t",argc,argv,&TRANSACTIONAL_CLIENTS))
			return 0;

		//Test duration
		if(!is("-td",argc,argv,&TEST_DURATION_IN_S))
			return 0;
		if(TEST_DURATION_IN_S<1){
			cout << "Error: TEST_DURATION_IN_S has to be larger than zero!" << endl;
			return 0;
		}

		//Warmup duration
		if(!is("-wd",argc,argv,&WARMUP_DURATION_IN_S))
			return 0;

		//Path to input binaries
		if(!is("-pa",argc,argv,&INITIAL_DB_CREATION_PATH))
			return 0;

		//Results and log output path
		if(!is("-op",argc,argv,&OUTPUT_PATH))
			return 0;

        

		Log::l1() << Log::tm() << "Config:\n";
		Log::l1() << Log::tm() << "-data source name: " << DATA_SOURCE_NAME << "\n";
		Log::l1() << Log::tm() << "-dbs user: " << DBS_USER << "\n";
		Log::l1() << Log::tm() << "-password: " << DBS_PASSWORD << "\n";
		Log::l1() << Log::tm() << "-analytical clients: " << ANALYTICAL_CLIENTS << "\n";
		Log::l1() << Log::tm() << "-transactional clients: " << TRANSACTIONAL_CLIENTS << "\n";
		Log::l1() << Log::tm() << "-test duration [s]: " << TEST_DURATION_IN_S << "\n";
		Log::l1() << Log::tm() << "-warmup duration [s]: " << WARMUP_DURATION_IN_S << "\n";
		Log::l1() << Log::tm() << "-initial db creation path: " << INITIAL_DB_CREATION_PATH << "\n";
		Log::l1() << Log::tm() << "-output path: " << OUTPUT_PATH << "\n";

	}else if(is("-h",argc,argv)){
		TYPE = 3;
	}else if(is("--help",argc,argv)){
		TYPE = 3;
	}else if(is("-init",argc,argv)){

		TYPE = 4;

		//IP Adress and port of target DBS
		if(!is("-dsn",argc,argv,&DATA_SOURCE_NAME))
			return 0;

		//User on target DBS
		if(!is("-usr",argc,argv,&DBS_USER))
			return 0;

		//Password of user on target DBS
		if(!is("-pwd",argc,argv,&DBS_PASSWORD))
			return 0;

		//Number of transctional clients
		if(!is("-a",argc,argv,&ANALYTICAL_CLIENTS))
			return 0;

		//Number of analytical clients
		if(!is("-t",argc,argv,&TRANSACTIONAL_CLIENTS))
			return 0;

		//Test duration
		if(!is("-td",argc,argv,&TEST_DURATION_IN_S))
			return 0;
		if(TEST_DURATION_IN_S<1){
			cout << "Error: TEST_DURATION_IN_S has to be larger than zero!" << endl;
			return 0;
		}

		//Warmup duration
		if(!is("-wd",argc,argv,&WARMUP_DURATION_IN_S))
			return 0;

		//Path to input binaries
		if(!is("-pa",argc,argv,&INITIAL_DB_CREATION_PATH))
			return 0;

		//Results and log output path
		if(!is("-op",argc,argv,&OUTPUT_PATH))
			return 0;

        

		Log::l1() << Log::tm() << "Config:\n";
		Log::l1() << Log::tm() << "-data source name: " << DATA_SOURCE_NAME << "\n";
		Log::l1() << Log::tm() << "-dbs user: " << DBS_USER << "\n";
		Log::l1() << Log::tm() << "-password: " << DBS_PASSWORD << "\n";
		

	}else{
		cout << "Unkown command\ntype: chBenchmark --help" << endl;
		return 0;
	}
	return 1;
}

#ifndef REWRITE_NO_ODBC
bool Config::warehouseDetection(SQLHSTMT& hStmt){

	WAREHOUSE_COUNT = 0;

	// Primary: query the database
	DbcTools::executeServiceStatement(hStmt, DialectStrategy::getInstance()->getSelectCountWarehouse());

	int temp = 0;
	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};
	if (DbcTools::fetch(hStmt, buf, &nIdicator, 1, temp)) {
		WAREHOUSE_COUNT = temp;
	}

	// Fallback: if query failed or returned 0, infer from CSV
	if (WAREHOUSE_COUNT == 0) {
		const std::string whFile = INITIAL_DB_CREATION_PATH + "/warehouse.tbl";
		std::ifstream ifs(whFile);
		if (ifs.is_open()) {
			std::string line;
			int lines = 0;
			while (std::getline(ifs, line)) {
				if (!line.empty()) ++lines;
			}
			ifs.close();
			if (lines > 0) {
				WAREHOUSE_COUNT = lines;
				Log::l1() << Log::tm() << "-detected warehouses from CSV: " << WAREHOUSE_COUNT << "\n";
				return 1;
			}
		}
		Log::l2() << Log::tm() << "-detecting warehouses failed\n";
		return 0;
	}

	Log::l1() << Log::tm() << "-detected warehouses: " << WAREHOUSE_COUNT << "\n";

	return 1;
}
#endif

int Config::getType(){
	return TYPE;
}

string Config::getDataSourceName(){
	return DATA_SOURCE_NAME;
}

string Config::getDbsUser(){
	return DBS_USER;
}

string Config::getDbsPassword(){
	return DBS_PASSWORD;
}

int Config::getAnalyticalClients(){
	return ANALYTICAL_CLIENTS;
}

int Config::getTransactionalClients(){
	return TRANSACTIONAL_CLIENTS;
}

int Config::getWarmupDurationInS(){
	return WARMUP_DURATION_IN_S;
}

int Config::getTestDurationInS(){
    return TEST_DURATION_IN_S;
}



int Config::getWarehouseCount(){
	return WAREHOUSE_COUNT;
}

string Config::getInitialDbCreationPath(){
	return INITIAL_DB_CREATION_PATH;
}

string Config::getOutputPath(){
	return OUTPUT_PATH;
}

char Config::getCsvDelim(){
	return CSV_DELIMITER;
}

// --- Schema tuning mapping accessors ---
std::map<std::string, std::vector<std::string>> Config::getOld2New(){
    return OLD2NEW;
}

std::map<std::string, std::vector<std::string>> Config::getnewTableColumns(){
    return NEW_TABLE_COLUMNS;
}

std::map<std::string, std::vector<std::string>> Config::getOldTableColumns(){
    return OLD_TABLE_COLUMNS;
}

std::map<std::string, std::vector<std::string>> Config::getTableKeys(){
    return TABLE_KEYS;
}

void Config::setOld2New(const std::map<std::string, std::vector<std::string>>& m){
    OLD2NEW = m;
}

void Config::setNewTableColumns(const std::map<std::string, std::vector<std::string>>& m){
    NEW_TABLE_COLUMNS = m;
}

void Config::setOldTableColumns(const std::map<std::string, std::vector<std::string>>& m){
    OLD_TABLE_COLUMNS = m;
}

void Config::setTableKeys(const std::map<std::string, std::vector<std::string>>& m){
    TABLE_KEYS = m;
}

std::map<std::string, std::map<std::string, std::string>> Config::getColumnRenames(){
    return COLUMN_RENAMES;
}

void Config::setColumnRenames(const std::map<std::string, std::map<std::string, std::string>>& m){
    COLUMN_RENAMES = m;
}

std::map<std::string, std::map<std::string, std::string>> Config::getExtraInsertCols(){
    return EXTRA_INSERT_COLS;
}

void Config::setExtraInsertCols(const std::map<std::string, std::map<std::string, std::string>>& m){
    EXTRA_INSERT_COLS = m;
}
std::map<std::string, std::map<std::string, std::string>> Config::getExtraUpdateSets(){
    return EXTRA_UPDATE_SETS;
}
void Config::setExtraUpdateSets(const std::map<std::string, std::map<std::string, std::string>>& m){
    EXTRA_UPDATE_SETS = m;
}

std::map<std::string, std::vector<std::string>> Config::getHorizontalSplit(){
    return HORIZONTAL_SPLIT;
}

void Config::setHorizontalSplit(const std::map<std::string, std::vector<std::string>>& m){
    HORIZONTAL_SPLIT = m;
}

std::map<std::string, std::string> Config::getHorizontalMerge(){
    return HORIZONTAL_MERGE;
}

void Config::setHorizontalMerge(const std::map<std::string, std::string>& m){
    HORIZONTAL_MERGE = m;
}

std::map<std::string, std::map<std::string, std::vector<std::string>>> Config::getColumnSplitColumns(){
    return COLUMN_SPLIT_COLUMNS;
}

void Config::setColumnSplitColumns(const std::map<std::string, std::map<std::string, std::vector<std::string>>>& m){
    COLUMN_SPLIT_COLUMNS = m;
}

std::map<std::string, std::map<std::string, std::string>> Config::getColumnSplitDelims(){
    return COLUMN_SPLIT_DELIMS;
}

void Config::setColumnSplitDelims(const std::map<std::string, std::map<std::string, std::string>>& m){
    COLUMN_SPLIT_DELIMS = m;
}

std::map<std::string, std::vector<std::string>> Config::getRemoveColumns(){
    return REMOVE_COLUMNS;
}

void Config::setRemoveColumns(const std::map<std::string, std::vector<std::string>>& m){
    REMOVE_COLUMNS = m;
}
