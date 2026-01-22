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

#include "src/AnalyticalStatistic.h"
#include "src/Config.h"
#include "src/DataSource.h"
#include "src/DbcTools.h"
#include "src/Log.h"
#include "src/Queries.h"
#include "src/Schema.h"
#include "src/TransactionalStatistic.h"
#include "src/Transactions.h"
#include "src/TupleGen.h"

#include <fstream>
#include <iostream>
#include <pthread.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <unistd.h>
#include <string>
#include <chrono>
#include <functional>
#include <array>

using namespace std;

bool dbgen(){
    DataSource::initialize();
    TupleGen::openOutputFiles();

    for(int iId=1; iId<=100000; iId++){
        //Item
        TupleGen::genItem(iId);
    }

    int oId;
    int olCount;
    string customerTime = "";
    string orderTime;
    for(int wId=1; wId<=Config::getWarehouseCount(); wId++){
        //Warehouse
        TupleGen::genWarehouse(wId);

        for(int iId=1; iId<=100000; iId++){
            //Stock
            TupleGen::genStock(iId, wId);
        }

        for(int dId=1; dId<=10; dId++){
            //District
            TupleGen::genDistrict(dId, wId);
            for(int cId=1; cId<=3000; cId++){
                //Customer
                if(customerTime == "")
                    customerTime = DataSource::getCurrentTimeString();
                TupleGen::genCustomer(cId, dId, wId,customerTime);

                //History
                TupleGen::genHistory(cId, dId, wId);

                //Order
                oId = DataSource::permute(cId,1,3000);
                olCount = DataSource::nextOderlineCount();
                orderTime = DataSource::getCurrentTimeString();
                TupleGen::genOrder(oId, dId, wId, cId, olCount, orderTime);

                for(int olNumber=1; olNumber<=olCount; olNumber++){
                    //Orderline
                    TupleGen::genOrderline(oId, dId, wId, olNumber, orderTime);
                }

                //Neworder
                if(oId>2100){
                    TupleGen::genNeworder(oId, dId, wId);
                }
            }
        }
    }

    //Region
    for(int rId=0; rId<5; rId++){
        TupleGen::genRegion(rId,DataSource::getRegion(rId));
    }

    //Nation
    for(int i=0; i<62; i++){
        TupleGen::genNation(DataSource::getNation(i));
    }

    //Supplier
    for(int suId=0; suId<10000; suId++){
        TupleGen::genSupplier(suId);
    }

    TupleGen::closeOutputFiles();

    return 1;
}

bool import_data_only() {
    // Connect to database
    SQLHENV hEnv = 0;
    DbcTools::setEnv(hEnv);
    SQLHDBC hDBC = 0;
    if(!DbcTools::connect(hEnv, hDBC)){
        cerr << "Failed to connect to database" << endl;
        return false;
    }

    // Create statement handle
    SQLHSTMT hStmt = 0;
    SQLAllocHandle(SQL_HANDLE_STMT, hDBC, &hStmt);

    // Create database schema
    cout << "Creating schema..." << endl;
    if(!Schema::createSchema(hStmt)){
        cerr << "Failed to create schema" << endl;
        return false;
    }

    // Import CSV files
    cout << "Importing CSV data..." << endl;
    if(!Schema::importCSV(hStmt)){
        cerr << "Failed to import CSV data" << endl;
        return false;
    }

    // Detect warehouse count
    if(!Config::warehouseDetection(hStmt)){
        cerr << "Failed to detect warehouse count" << endl;
        return false;
    }

    // Check if data was imported correctly
    cout << "Checking data integrity..." << endl;
    if(!Schema::check(hStmt)){
        cerr << "Data integrity check failed" << endl;
        return false;
    }

    // Additional preparation
    cout << "Running additional preparation..." << endl;
    if(!Schema::additionalPreparation(hStmt)){
        cerr << "Additional preparation failed" << endl;
        return false;
    }

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDBC);
    SQLFreeHandle(SQL_HANDLE_DBC, hDBC);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    cout << "Data import completed successfully!" << endl;
    return true;
}

typedef struct _threadParameters{
    pthread_barrier_t* barStart;
    int* runState;
    int threadId;
    SQLHENV* hEnv;
    void* stat;
} threadParameters;

void* analyticalThread(void* args){
    threadParameters* prm = (threadParameters*) args;
    AnalyticalStatistic* aStat = (AnalyticalStatistic*) prm->stat;

    bool b;
    long query=0;
    int q=0;

    SQLHDBC hDBC = 0;
    if(!DbcTools::connect(*(prm->hEnv), hDBC)){
        exit(1);
    }

    Queries* queries = new Queries();
    if(!queries->prepareStatements(hDBC)){
        exit(1);
    }

    pthread_barrier_wait(prm->barStart);

    if(*(prm->runState)==1){
        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ":  start warmup\n";
        while(*(prm->runState)==1){
            q=(query%22)+1;
            Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H " << q << "\n";
            queries->executeTPCH(hDBC,q);
            query++;
        }
    }
    if(*(prm->runState)==2){
        bool collectAPLatency = (Config::getAnalyticalClients() == 1 && Config::getTransactionalClients() == 0);
        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": start test\n";
        while(*(prm->runState)==2){
            if (collectAPLatency) {
                const std::string apOut = Config::getOutputPath()+"/latency_AP.txt";
                std::ofstream ofs(apOut, std::ios::app);
                if (!ofs.is_open()) {
                    Log::l1() << "Error: cannot write AP latency file " << apOut << "\n";
                    break;
                }
                ofs << "AP Solo Latency Collection:\n";
                for (int qid = 1; qid <= 22; qid++) {
                    using namespace std::chrono;
                    auto start = high_resolution_clock::now();
                    bool ok = queries->executeTPCH(hDBC, qid);
                    auto end = high_resolution_clock::now();
                    long long ms = duration_cast<milliseconds>(end - start).count();
                    aStat->executeTPCHSuccess(qid, ok);
                    ofs << "Q" << qid << ", "
                        << (ok ? "success" : "rollback") << ", "
                        << ms << " ms\n";
                    Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPCH " << qid
                              << " (" << (ok ? "success" : "rollback") << ") latency=" << ms << "ms\n";
                    if (*(prm->runState) != 2) break;
                }
                ofs << "--------------------------\n";
                ofs.close();
                break; // exit thread after solo collection
            } else {
                q=(query%22)+1;
                Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H " << q << "\n";
                b = queries->executeTPCH(hDBC,q);
                aStat->executeTPCHSuccess(q,b);
                query++;
            }
        }
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": exit\n";
    return NULL;
}

void* transactionalThread(void* args){
    threadParameters* prm = (threadParameters*) args;
    TransactionalStatistic* tStat = (TransactionalStatistic*) prm->stat;

    SQLHDBC hDBC = 0;
    if(!DbcTools::connect(*(prm->hEnv), hDBC)){
        exit(1);
    }

    Transactions* transactions = new Transactions();
    if(!transactions->prepareStatements(hDBC)){
        exit(1);
    }

    bool b;
    int decision=0;

    if(DbcTools::autoCommitOff(hDBC)){
        pthread_barrier_wait(prm->barStart);

        if(*(prm->runState)==1){
            Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": start warmup\n";
            while(*(prm->runState)==1){
                DataSource::randomUniformInt(1,100,decision);
                if(decision<=44 && (*(prm->runState)==1)){
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": NewOrder\n";
                    transactions->executeNewOrder(hDBC);
                }
                DataSource::randomUniformInt(1,100,decision);
                if(decision<=44 && (*(prm->runState)==1)){
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Payment\n";
                    transactions->executePayment(hDBC);
                }
                DataSource::randomUniformInt(1,100,decision);
                if(decision<=4 && (*(prm->runState)==1)){
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": OrderStatus\n";
                    transactions->executeOrderStatus(hDBC);
                }
                DataSource::randomUniformInt(1,100,decision);
                if(decision<=4 && (*(prm->runState)==1)){
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Delivery\n";
                    transactions->executeDelivery(hDBC);
                }
                DataSource::randomUniformInt(1,100,decision);
                if(decision<=4 && (*(prm->runState)==1)){
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": StockLevel\n";
                    transactions->executeStockLevel(hDBC);
                }
            }
        }

        if(*(prm->runState)==2){
            bool soloTransactional = (Config::getAnalyticalClients() == 0 && Config::getTransactionalClients() == 1);
            Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": start test\n";
            while(*(prm->runState)==2){
                if (soloTransactional) {
                    using namespace std::chrono;
                    std::array<long long, 5> txnLatency = {0,0,0,0,0};
                    std::array<bool, 5> txnResult = {false, false, false, false, false};
                    std::function<bool()> txnFuncs[5] = {
                        [&](){ return transactions->executeNewOrder(hDBC); },
                        [&](){ return transactions->executePayment(hDBC); },
                        [&](){ return transactions->executeOrderStatus(hDBC); },
                        [&](){ return transactions->executeDelivery(hDBC); },
                        [&](){ return transactions->executeStockLevel(hDBC); }
                    };
                    const char* txnNames[5] = { "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel" };
                    for (int i=0; i<5; i++) {
                        auto start = high_resolution_clock::now();
                        bool ok = txnFuncs[i]();
                        auto end = high_resolution_clock::now();
                        long long ms = duration_cast<milliseconds>(end - start).count();
                        txnLatency[i] = ms;
                        txnResult[i] = ok;
                        tStat->executeTPCCSuccess(i+1, ok);
                    }
                    const std::string outPath = Config::getOutputPath()+"/latency_TP.txt";
                    std::ofstream ofs(outPath, std::ios::app);
                    if (ofs.is_open()) {
                        ofs << "Solo Transaction Latency (ms):\n";
                        for (int i = 0; i < 5; i++) {
                            ofs << txnNames[i] << " (" << (txnResult[i] ? "success" : "rollback") << "): "
                                << txnLatency[i] << " ms\n";
                        }
                        ofs << "-----------------------------\n";
                        ofs.close();
                    } else {
                        Log::l1() << "Error: cannot write latency file " << outPath << "\n";
                    }
                    break; // exit thread after solo collection
                } else {
                    DataSource::randomUniformInt(1,100,decision);
                    if(decision<=44 && (*(prm->runState)==2)){
                        Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": NewOrder\n";
                        b = transactions->executeNewOrder(hDBC);
                        tStat->executeTPCCSuccess(1,b);
                    }
                    DataSource::randomUniformInt(1,100,decision);
                    if(decision<=44 && (*(prm->runState)==2)){
                        Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Payment\n";
                        b = transactions->executePayment(hDBC);
                        tStat->executeTPCCSuccess(2,b);
                    }
                    DataSource::randomUniformInt(1,100,decision);
                    if(decision<=4 && (*(prm->runState)==2)){
                        Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": OrderStatus\n";
                        b = transactions->executeOrderStatus(hDBC);
                        tStat->executeTPCCSuccess(3,b);
                    }
                    DataSource::randomUniformInt(1,100,decision);
                    if(decision<=4 && (*(prm->runState)==2)){
                        Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Delivery\n";
                        b = transactions->executeDelivery(hDBC);
                        tStat->executeTPCCSuccess(4,b);
                    }
                    DataSource::randomUniformInt(1,100,decision);
                    if(decision<=4 && (*(prm->runState)==2)){
                        Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": StockLevel\n";
                        b = transactions->executeStockLevel(hDBC);
                        tStat->executeTPCCSuccess(5,b);
                    }
                }
            }
        }
    }

    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": exit\n";
    return NULL;
}

bool run_benchmark_only() {
    // Initialization
    cout << "Initializing benchmark..." << endl;
    
    SQLHENV hEnv = 0;
    DbcTools::setEnv(hEnv);
    
    // Check if database is already populated (optional)
    SQLHDBC checkDBC = 0;
    if(!DbcTools::connect(hEnv, checkDBC)){
        cerr << "Failed to connect to database. Make sure data is already imported." << endl;
        return false;
    }
    SQLDisconnect(checkDBC);
    
    DataSource::initialize();

    int runState = 0; // 0=dont_run 1=warmup 2=run
    unsigned int count = Config::getAnalyticalClients() + Config::getTransactionalClients() + 1;
    pthread_barrier_t barStart;
    pthread_barrier_init(&barStart, NULL, count);

    // Start analytical threads
    AnalyticalStatistic* aStat[Config::getAnalyticalClients()];
    pthread_t apt[Config::getAnalyticalClients()];
    threadParameters aprm[Config::getAnalyticalClients()];
    for(int i = 0; i < Config::getAnalyticalClients(); i++){
        aStat[i] = new AnalyticalStatistic();
        aprm[i] = {&barStart, &runState, i+1, &hEnv, (void*)aStat[i]};
        pthread_create(&apt[i], NULL, analyticalThread, &aprm[i]);
    }

    // Start transactional threads
    TransactionalStatistic* tStat[Config::getTransactionalClients()];
    pthread_t tpt[Config::getTransactionalClients()];
    threadParameters tprm[Config::getTransactionalClients()];
    for(int i = 0; i < Config::getTransactionalClients(); i++){
        tStat[i] = new TransactionalStatistic();
        tprm[i] = {&barStart, &runState, i+1, &hEnv, (void*)tStat[i]};
        pthread_create(&tpt[i], NULL, transactionalThread, &tprm[i]);
    }

    runState = 1;
    cout << "Waiting for threads to initialize..." << endl;
    pthread_barrier_wait(&barStart);
    cout << "All threads initialized" << endl;

    // Warmup phase
    cout << "Starting warmup phase (" << Config::getWarmupDurationInS() << " seconds)..." << endl;
    sleep(Config::getWarmupDurationInS());

    // Test phase (support single-client latency mode)
    bool singleClientMode = (Config::getAnalyticalClients() == 1 && Config::getTransactionalClients() == 0)
                         || (Config::getAnalyticalClients() == 0 && Config::getTransactionalClients() == 1);
    if (singleClientMode) {
        runState = 2;
        cout << "Starting test phase (single client)" << endl;
        if (Config::getAnalyticalClients() == 1) {
            pthread_join(apt[0], nullptr);
        }
        if (Config::getTransactionalClients() == 1) {
            pthread_join(tpt[0], nullptr);
        }
        cout << "Single client finished" << endl;
        runState = 0;
    } else {
        runState = 2;
        cout << "Starting test phase (" << Config::getTestDurationInS() << " seconds)..." << endl;
        sleep(Config::getTestDurationInS());
        runState = 0;
        cout << "Stopping benchmark..." << endl;
    }

    // Write results to file
    unsigned long long analyticalResults = 0;
    unsigned long long transactionalResults = 0;
    for(int i = 0; i < Config::getAnalyticalClients(); i++){
        aStat[i]->addResult(analyticalResults);
    }
    for(int i = 0; i < Config::getTransactionalClients(); i++){
        tStat[i]->addResult(transactionalResults);
    }

    unsigned long long qphh = analyticalResults * 3600 / Config::getTestDurationInS();
    unsigned long long tpmc = transactionalResults * 60 / Config::getTestDurationInS();

    ofstream resultStream;
    resultStream.open((Config::getOutputPath() + "/Results.csv").c_str());
    resultStream << "System Under Test:" << Config::getDataSourceName() << endl;
    resultStream << "Analytical Clients:" << Config::getAnalyticalClients() << endl;
    resultStream << "Transactional Clients:" << Config::getTransactionalClients() << endl;
    resultStream << "Warmup Duration in [s]:" << Config::getWarmupDurationInS() << endl;
    resultStream << "Test Duration in [s]:" << Config::getTestDurationInS() << endl;
    resultStream << "Warehouses:" << Config::getWarehouseCount() << endl;
    resultStream << endl;
    resultStream << "OLAP Throughput in [QphH]:" << qphh << endl;
    resultStream << "OLTP Throughput in [tpmC]:" << tpmc << endl;
    resultStream.close();
    cout << "Results written to: " << Config::getOutputPath() << "/Results.csv" << endl;

    // Wait for threads to finish (avoid double-join in single-client mode)
    cout << "Waiting for clients to return..." << endl;
    if (!singleClientMode) {
        for(int i = 0; i < Config::getAnalyticalClients(); i++){
            pthread_join(apt[i], NULL);
            delete aStat[i];
        }
        for(int i = 0; i < Config::getTransactionalClients(); i++){
            pthread_join(tpt[i], NULL);
            delete tStat[i];
        }
    } else {
        if (Config::getAnalyticalClients() == 1) {
            delete aStat[0];
        }
        if (Config::getTransactionalClients() == 1) {
            delete tStat[0];
        }
    }

    pthread_barrier_destroy(&barStart);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);
    
    cout << "Benchmark completed!" << endl;
    return true;
}

int main(int argc, char* argv[]){
    // Parse command line arguments
    string mode = "full";
    
    // Simple argument parsing for mode
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-mode") {
            if (i + 1 < argc) {
                mode = argv[++i];
            }
        }
    }
    
    // Read config from command line parameters
    if(!Config::initialize(argc, argv))
        return 1;

    if (mode == "generate") {
        // 1. Generate CSV files only
        cout << "Generating initial database as CSV files:" << endl;
        if(!dbgen()){
            cout << "Failed to generate CSV files" << endl;
            return 1;
        }
        cout << "CSV generation succeeded" << endl;
        return 0;
    }
    else if (mode == "import") {
        // 2. Import data into database only
        cout << "Importing data into database..." << endl;
        if(!import_data_only()){
            cout << "Data import failed" << endl;
            return 1;
        }
        cout << "Data import succeeded" << endl;
        return 0;
    }
    else if (mode == "benchmark") {
        // 3. Run benchmark only (assumes data is already imported)
        cout << "Running benchmark..." << endl;
        if(!run_benchmark_only()){
            cout << "Benchmark failed" << endl;
            return 1;
        }
        cout << "Benchmark succeeded" << endl;
        return 0;
    }
    else if (mode == "full") {
        // 4. Original behavior: generate, import, and run benchmark
        // Generate CSV
        cout << "Generating initial database as CSV files:" << endl;
        if(!dbgen()){
            cout << "Failed to generate CSV files" << endl;
            return 1;
        }
        cout << "CSV generation succeeded" << endl;
        
        // Import data
        cout << "Importing data into database..." << endl;
        if(!import_data_only()){
            cout << "Data import failed" << endl;
            return 1;
        }
        cout << "Data import succeeded" << endl;
        
        // Run benchmark
        cout << "Running benchmark..." << endl;
        if(!run_benchmark_only()){
            cout << "Benchmark failed" << endl;
            return 1;
        }
        cout << "Benchmark succeeded" << endl;
        return 0;
    }
    else if (mode == "help") {
        cout << "Usage: chBenchmark [-mode <mode>] [other options]\n"
             << "\nModes:\n"
             << "  generate   - Generate CSV files only\n"
             << "  import     - Import data into database only (requires CSV files)\n"
             << "  benchmark  - Run benchmark only (requires data already imported)\n"
             << "  full       - Run all steps: generate, import, benchmark (default)\n"
             << "  help       - Show this help message\n"
             << "\nExample usage:\n"
             << "  chBenchmark -mode generate -wh 2 -pa ./data\n"
             << "  chBenchmark -mode import -dsn MyDB -usr user -pwd pass -pa ./data\n"
             << "  chBenchmark -mode benchmark -a 2 -t 4 -wd 60 -td 300\n"
             << "\nOriginal options still supported:\n"
             << "  -csv       - Generate CSV files (legacy)\n"
             << "  -run       - Run full test (legacy)\n";
        return 0;
    }
    else {
        cerr << "Unknown mode: " << mode << endl;
        cerr << "Use -mode help for usage information" << endl;
        return 1;
    }

    return 0;
}
