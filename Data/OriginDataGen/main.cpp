//g++ -o tpcc_data_gen main.cpp DataSource.cpp TupleGen.cpp -std=c++11 -lpthread
//./tpcc_data_gen -w 1 -o D:\LLM4DBTUNING\tpcc_data  
#include "DataSource.h"
#include "TupleGen.h"

#include <fstream>
#include <iostream>
#include <pthread.h>
#include <string>

// Windows特定的头文件
#ifdef _WIN32
    #include <direct.h>
    #include <windows.h>
#else
    #include <sys/stat.h>
#endif

#include <unistd.h>
#include <cstdlib>
#include <cstring>

using namespace std;

// 修改dbgen函数，接受参数
bool dbgen(int warehouseCount, const string& outputPath) {
    DataSource::initialize(outputPath, warehouseCount);
    TupleGen::openOutputFiles(outputPath);  // 假设可以传递输出路径
    // 检查文件是否真的生成
    cout << "initialize..." << endl;
    for(int iId=1; iId<=100000; iId++){
        //Item
        cout << "generateItem..." << endl;
        TupleGen::genItem(iId);
    }

    int oId;
    int olCount;
    string customerTime = "";
    string orderTime;
    for(int wId=1; wId<=warehouseCount; wId++){  // 使用传入的warehouseCount
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

    return true;
}

void printUsage(const char* programName) {
    cout << "Usage: " << programName << " -w <warehouse_count> -o <output_path>" << endl;
    cout << "Options:" << endl;
    cout << "  -w <count>     Number of warehouses (required)" << endl;
    cout << "  -o <path>      Output directory path (required)" << endl;
    cout << "  -h             Show this help message" << endl;
}

bool createDirectory(const string& path) {
#ifdef _WIN32
    // Windows版本
    DWORD attrib = GetFileAttributesA(path.c_str());
    if (attrib != INVALID_FILE_ATTRIBUTES) {
        if (attrib & FILE_ATTRIBUTE_DIRECTORY) {
            cout << "Directory already exists: " << path << endl;
            return true;
        } else {
            cerr << "Error: Path exists but is not a directory: " << path << endl;
            return false;
        }
    } else {
        // 目录不存在，尝试创建
        cout << "Creating directory: " << path << endl;
        if (_mkdir(path.c_str()) == 0) {
            cout << "Directory created successfully" << endl;
            return true;
        } else {
            cerr << "Error: Failed to create directory: " << path << endl;
            return false;
        }
    }
#else
    // Linux/macOS版本
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        cout << "Creating directory: " << path << endl;
        if (mkdir(path.c_str(), 0755) == 0) {
            cout << "Directory created successfully" << endl;
            return true;
        } else {
            cerr << "Error: Failed to create directory: " << path << endl;
            return false;
        }
    } else if (info.st_mode & S_IFDIR) {
        cout << "Directory already exists: " << path << endl;
        return true;
    } else {
        cerr << "Error: Path exists but is not a directory: " << path << endl;
        return false;
    }
#endif
}

int main(int argc, char* argv[]) {
    int warehouseCount = 0;
    string outputPath;
    
    // 解析命令行参数
    int opt;
    while ((opt = getopt(argc, argv, "w:o:h")) != -1) {
        switch (opt) {
            case 'w':
                warehouseCount = atoi(optarg);
                if (warehouseCount <= 0) {
                    cerr << "Error: Warehouse count must be a positive integer" << endl;
                    return 1;
                }
                break;
            case 'o':
                outputPath = optarg;
                break;
            case 'h':
                printUsage(argv[0]);
                return 0;
            case '?':
                cerr << "Error: Unknown option or missing argument" << endl;
                printUsage(argv[0]);
                return 1;
            default:
                printUsage(argv[0]);
                return 1;
        }
    }
    
    // 验证必需参数
    if (warehouseCount == 0 || outputPath.empty()) {
        cerr << "Error: Both warehouse count and output path are required" << endl;
        printUsage(argv[0]);
        return 1;
    }
    
    cout << "Starting TPC-C data generation..." << endl;
    cout << "Warehouse count: " << warehouseCount << endl;
    cout << "Output path: " << outputPath << endl;

    // 创建输出目录
    if (!createDirectory(outputPath)) {
        cerr << "Error: Cannot create output directory. Exiting." << endl;
        return 1;
    }
    
    // 执行数据生成，直接传递参数
    try {
        bool success = dbgen(warehouseCount, outputPath);
        if (success) {
            cout << "Data generation completed successfully!" << endl;
            return 0;
        } else {
            cerr << "Data generation failed!" << endl;
            return 1;
        }
    } catch (const exception& e) {
        cerr << "Error during data generation: " << e.what() << endl;
        return 1;
    }
}