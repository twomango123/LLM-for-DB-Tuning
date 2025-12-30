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
#include "DataSource.h"
#include "Log.h"
#include "Transactions.h"
#include "dialect/DialectStrategy.h"
#include "RewriteDML.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <vector>
#include <unordered_set>
#include <algorithm>

using namespace std;

bool Transactions::prepare(SQLHDBC& hDBC){

	//NewOrder:
	if(!DbcTools::allocAndPrepareStmt(hDBC, noWarehouseSelect, DialectStrategy::getInstance()->getNoWarehouseSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noDistrictSelect, DialectStrategy::getInstance()->getNoDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noDistrictUpdate, DialectStrategy::getInstance()->getNoDistrictUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noCustomerSelect, DialectStrategy::getInstance()->getNoCustomerSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noItemSelect, DialectStrategy::getInstance()->getNoItemSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[0], DialectStrategy::getInstance()->getNoStockSelect01()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[1], DialectStrategy::getInstance()->getNoStockSelect02()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[2], DialectStrategy::getInstance()->getNoStockSelect03()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[3], DialectStrategy::getInstance()->getNoStockSelect04()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[4], DialectStrategy::getInstance()->getNoStockSelect05()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[5], DialectStrategy::getInstance()->getNoStockSelect06()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[6], DialectStrategy::getInstance()->getNoStockSelect07()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[7], DialectStrategy::getInstance()->getNoStockSelect08()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[8], DialectStrategy::getInstance()->getNoStockSelect09()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[9], DialectStrategy::getInstance()->getNoStockSelect10()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockUpdates[0], DialectStrategy::getInstance()->getNoStockUpdate01()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockUpdates[1], DialectStrategy::getInstance()->getNoStockUpdate02()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noOrderlineInsert, DialectStrategy::getInstance()->getNoOrderlineInsert()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noOrderInsert, DialectStrategy::getInstance()->getNoOrderInsert()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noNewOrderInsert, DialectStrategy::getInstance()->getNoNewOrderInsert()))
		return 0;

	//Payment:
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmWarehouseSelect, DialectStrategy::getInstance()->getPmWarehouseSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmWarehouseUpdate, DialectStrategy::getInstance()->getPmWarehouseUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmDistrictSelect, DialectStrategy::getInstance()->getPmDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmDistrictUpdate, DialectStrategy::getInstance()->getPmDistrictUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect1, DialectStrategy::getInstance()->getPmCustomerSelect1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect2, DialectStrategy::getInstance()->getPmCustomerSelect2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect3, DialectStrategy::getInstance()->getPmCustomerSelect3()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerUpdate1, DialectStrategy::getInstance()->getPmCustomerUpdate1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect4, DialectStrategy::getInstance()->getPmCustomerSelect4()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerUpdate2, DialectStrategy::getInstance()->getPmCustomerUpdate2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmHistoryInsert, DialectStrategy::getInstance()->getPmHistoryInsert()))
		return 0;

	//OrderStatus:
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect1, DialectStrategy::getInstance()->getOsCustomerSelect1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect2, DialectStrategy::getInstance()->getOsCustomerSelect2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect3, DialectStrategy::getInstance()->getOsCustomerSelect3()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osOrderSelect, DialectStrategy::getInstance()->getOsOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osOrderlineSelect, DialectStrategy::getInstance()->getOsOrderlineSelect()))
		return 0;

	//Delivery
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlNewOrderSelect, DialectStrategy::getInstance()->getDlNewOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlNewOrderDelete, DialectStrategy::getInstance()->getDlNewOrderDelete()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderSelect, DialectStrategy::getInstance()->getDlOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderUpdate, DialectStrategy::getInstance()->getDlOrderUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderlineUpdate, DialectStrategy::getInstance()->getDlOrderlineUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderlineSelect, DialectStrategy::getInstance()->getDlOrderlineSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlCustomerUpdate, DialectStrategy::getInstance()->getDlCustomerUpdate()))
		return 0;

	//StockLevel
	if(!DbcTools::allocAndPrepareStmt(hDBC, slDistrictSelect, DialectStrategy::getInstance()->getSlDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, slStockSelect, DialectStrategy::getInstance()->getSlStockSelect()))
		return 0;

	return 1;

}
bool Transactions::prepareNewOrder(SQLHDBC& hDBC){
	//NewOrder
	if(!DbcTools::allocAndPrepareStmt(hDBC, noWarehouseSelect, DialectStrategy::getInstance()->getNoWarehouseSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noDistrictSelect, DialectStrategy::getInstance()->getNoDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noDistrictUpdate, DialectStrategy::getInstance()->getNoDistrictUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noCustomerSelect, DialectStrategy::getInstance()->getNoCustomerSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noItemSelect, DialectStrategy::getInstance()->getNoItemSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[0], DialectStrategy::getInstance()->getNoStockSelect01()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[1], DialectStrategy::getInstance()->getNoStockSelect02()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[2], DialectStrategy::getInstance()->getNoStockSelect03()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[3], DialectStrategy::getInstance()->getNoStockSelect04()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[4], DialectStrategy::getInstance()->getNoStockSelect05()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[5], DialectStrategy::getInstance()->getNoStockSelect06()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[6], DialectStrategy::getInstance()->getNoStockSelect07()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[7], DialectStrategy::getInstance()->getNoStockSelect08()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[8], DialectStrategy::getInstance()->getNoStockSelect09()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[9], DialectStrategy::getInstance()->getNoStockSelect10()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockUpdates[0], DialectStrategy::getInstance()->getNoStockUpdate01()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noStockUpdates[1], DialectStrategy::getInstance()->getNoStockUpdate02()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noOrderlineInsert, DialectStrategy::getInstance()->getNoOrderlineInsert()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noOrderInsert, DialectStrategy::getInstance()->getNoOrderInsert()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, noNewOrderInsert, DialectStrategy::getInstance()->getNoNewOrderInsert()))
		return 0;
}
bool Transactions::preparePayment(SQLHDBC& hDBC){
	//Payment
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmWarehouseSelect, DialectStrategy::getInstance()->getPmWarehouseSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmWarehouseUpdate, DialectStrategy::getInstance()->getPmWarehouseUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmDistrictSelect, DialectStrategy::getInstance()->getPmDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmDistrictUpdate, DialectStrategy::getInstance()->getPmDistrictUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect1, DialectStrategy::getInstance()->getPmCustomerSelect1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect2, DialectStrategy::getInstance()->getPmCustomerSelect2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect3, DialectStrategy::getInstance()->getPmCustomerSelect3()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerUpdate1, DialectStrategy::getInstance()->getPmCustomerUpdate1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerSelect4, DialectStrategy::getInstance()->getPmCustomerSelect4()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmCustomerUpdate2, DialectStrategy::getInstance()->getPmCustomerUpdate2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, pmHistoryInsert, DialectStrategy::getInstance()->getPmHistoryInsert()))
		return 0;
}
bool Transactions::prepareOrderStatus(SQLHDBC& hDBC){
	//OrderStatus:
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect1, DialectStrategy::getInstance()->getOsCustomerSelect1()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect2, DialectStrategy::getInstance()->getOsCustomerSelect2()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osCustomerSelect3, DialectStrategy::getInstance()->getOsCustomerSelect3()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osOrderSelect, DialectStrategy::getInstance()->getOsOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, osOrderlineSelect, DialectStrategy::getInstance()->getOsOrderlineSelect()))
		return 0;
}
bool Transactions::prepareDelivery(SQLHDBC& hDBC){
	//Delivery
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlNewOrderSelect, DialectStrategy::getInstance()->getDlNewOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlNewOrderDelete, DialectStrategy::getInstance()->getDlNewOrderDelete()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderSelect, DialectStrategy::getInstance()->getDlOrderSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderUpdate, DialectStrategy::getInstance()->getDlOrderUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderlineUpdate, DialectStrategy::getInstance()->getDlOrderlineUpdate()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlOrderlineSelect, DialectStrategy::getInstance()->getDlOrderlineSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, dlCustomerUpdate, DialectStrategy::getInstance()->getDlCustomerUpdate()))
		return 0;
}
bool Transactions::prepareStockLevel(SQLHDBC& hDBC){
	//StockLevel
	if(!DbcTools::allocAndPrepareStmt(hDBC, slDistrictSelect, DialectStrategy::getInstance()->getSlDistrictSelect()))
		return 0;
	if(!DbcTools::allocAndPrepareStmt(hDBC, slStockSelect, DialectStrategy::getInstance()->getSlStockSelect()))
		return 0;

}



bool Transactions::prepareStatements(SQLHDBC& hDBC){
	if(!prepare(hDBC)){
		Log::l2() << Log::tm() << "-prepare statements failed\n";
		return 0;
	}
	Log::l1() << Log::tm() << "-prepare statements succeeded\n";
	return 1;
}

bool Transactions::executeNewOrder(SQLHDBC& hDBC){
    prepareNewOrder(hDBC);
    // 事务级上下文：用于延迟合并、去重、以及阶段化冗余列
    TxnRewriteCtx rwctx;
    // 在事务开始时枚举判断：是否存在“合并目标表”覆盖了本事务 for 循环内插入的表（orderline）
    // 若不存在合并目标，则无需延迟 orders/neworder 的插入与后续 flush。
    auto old2new_all = Config::getOld2New();
    auto newCols_all = Config::getnewTableColumns();
    auto hasNewTable = [&](const std::string& t){ return newCols_all.find(t) != newCols_all.end(); };
    auto getTargets = [&](const char* tab){
        std::vector<std::string> v; auto it = old2new_all.find(tab); if(it!=old2new_all.end()) v = it->second; return v; };
    std::vector<std::string> orderlineTargets = getTargets("orderline");
    std::vector<std::string> ordersTargets    = getTargets("orders");
    std::vector<std::string> neworderTargets  = getTargets("neworder");
    // 判断交集：orderline 与 (orders ∪ neworder) 在相同的新表上是否有交集，并且这些新表真实存在于映射列清单中
    std::unordered_set<std::string> hdrTargets;
    for(const auto& t : ordersTargets) hdrTargets.insert(t);
    for(const auto& t : neworderTargets) hdrTargets.insert(t);
    bool mergeRequired = false;
    std::vector<std::string> mergeTargets;
    for(const auto& t : orderlineTargets){
        if(hdrTargets.count(t) && hasNewTable(t)) { mergeRequired = true; mergeTargets.push_back(t); }
    }

    // 若需要延迟合并，则在循环期间累积行参数，待最后一条行插入时统一 flush
    struct StagedLineArgs { int o_id; int d_id; int w_id; int number; int i_id; int supply_w_id; int quantity; double amount; char dist[24]; };
    std::vector<StagedLineArgs> stagedLines;
	struct OrderLine{
		int olIId;
		int olSupplyWId;
		bool olIsRemote;
		int olQuantity;
	};

	//2.4.1.1
	int wId = 0;
	DataSource::randomUniformInt(1,Config::getWarehouseCount(), wId);
	//2.4.1.2
	int dId = 0;
	DataSource::randomUniformInt(1,10,dId);
	int cId = 0;
	DataSource::randomNonUniformInt(1023,1,3000,867,cId);
	//2.4.1.3
	int olCount = 0;
	DataSource::randomUniformInt(5,15,olCount);
	//2.4.1.4
	int randomRollback = 0;
	DataSource::randomUniformInt(1,100,randomRollback);
	//2.4.1.5
	int allLocal=1;
	OrderLine oLines[olCount];
	for(int i=0; i<olCount; i++){
		//1.
		if(i==olCount-1 && randomRollback==1)
			oLines[i].olIId = 100001;
		else
			DataSource::randomNonUniformInt(8191,1,100000,5867,oLines[i].olIId);
		//2.
		if(DataSource::randomUniformInt(1,100)==1){
			DataSource::getRemoteWId(wId,oLines[i].olSupplyWId);
			oLines[i].olIsRemote = 1;
			allLocal = 0;
		}
		else{
			oLines[i].olSupplyWId = wId;
			oLines[i].olIsRemote = 0;
		}
		//3.
		DataSource::randomUniformInt(1,10,oLines[i].olQuantity);
	}
	//2.4.1.6
	SQL_TIMESTAMP_STRUCT oEntryD;
	DataSource::getCurrentTimestamp(oEntryD);

	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};

	//BEGIN TRANSACTION
	// 查找对应warehouse（已经改写替换好的select）
	DbcTools::resetStatement(noWarehouseSelect);
	DbcTools::bind(noWarehouseSelect,1,wId);
	if(!DbcTools::executePreparedStatement(noWarehouseSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	// 查找对应district（已经改写替换好的select）
	DbcTools::resetStatement(noDistrictSelect);
	DbcTools::bind(noDistrictSelect,1,wId);
	DbcTools::bind(noDistrictSelect,2,dId);
	if(!DbcTools::executePreparedStatement(noDistrictSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	int dNextOId = 0;
	if(!DbcTools::fetch(noDistrictSelect, buf, &nIdicator, 2, dNextOId)){
		DbcTools::rollback(hDBC);
		return 0;
	}
    // 更新district：update tpcch.district set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?

    // 清除之前绑定的参数
    DbcTools::resetStatement(noDistrictUpdate);

    // 基于映射的最小重写流程（仅处理 UPDATE district）
    auto old2new = Config::getOld2New();
    auto newTableColumns = Config::getnewTableColumns();

    // 捕获原参数（顺序一致）：1 -> wId, 2 -> dId
    ParamPack pack; // from RewriteDML.h
    pack.addInt(wId);
    pack.addInt(dId);

    const std::string originalSql = DialectStrategy::getInstance()->getNoDistrictUpdate();

    {
        auto rr = RewriteDML::tryRewriteAndExecUpdate(hDBC,
                                                      std::string("district"),
                                                      originalSql,
                                                      pack,
                                                      old2new,
                                                      newTableColumns);
        if (rr == RewriteResult::Failed) {
            DbcTools::rollback(hDBC);
            return 0;
        }
        if (rr == RewriteResult::NotApplicable) {
        // 不重写或重写失败，走原逻辑
        DbcTools::bind(noDistrictUpdate,1,wId);
        DbcTools::bind(noDistrictUpdate,2,dId);
        if(!DbcTools::executePreparedStatement(noDistrictUpdate)){
            DbcTools::rollback(hDBC);
            return 0;
        }
        }
    }
	

	DbcTools::resetStatement(noCustomerSelect);
	DbcTools::bind(noCustomerSelect,1,wId);
	DbcTools::bind(noCustomerSelect,2,dId);
	DbcTools::bind(noCustomerSelect,3,cId);
    if(!DbcTools::executePreparedStatement(noCustomerSelect)){
        DbcTools::rollback(hDBC);
        return 0;
    }
    // 若需要合并到包含冗余列的新表，则从外键 customer 实时取值并阶段化
    if (mergeRequired) {
        double cDiscount = 0.0; std::string cLast = ""; std::string cCredit = "";
        // 期望列顺序: C_DISCOUNT (1), C_LAST (2), C_CREDIT (3)
        if (SQL_SUCCESS==SQLFetch(noCustomerSelect)){
            // C_DISCOUNT
            if(SQL_SUCCESS==SQLGetData(noCustomerSelect,1,SQL_C_CHAR,buf,1024,&nIdicator)){
                cDiscount = atof((char*)buf);
            } else { DbcTools::rollback(hDBC); return 0; }
            // C_LAST
            if(SQL_SUCCESS==SQLGetData(noCustomerSelect,2,SQL_C_CHAR,buf,1024,&nIdicator)){
                cLast = std::string((char*)buf);
            } else { DbcTools::rollback(hDBC); return 0; }
            // C_CREDIT
            if(SQL_SUCCESS==SQLGetData(noCustomerSelect,3,SQL_C_CHAR,buf,1024,&nIdicator)){
                cCredit = std::string((char*)buf);
            } else { DbcTools::rollback(hDBC); return 0; }

            // 将冗余列阶段化到所有合并目标（只有存在对应列的新表才会在 flush 中被使用）
            for (const auto& tgt : mergeTargets){
                ParamValue pv;
                // c_discount
                pv.kind = ParamKind::Double; pv.d = cDiscount;
                RewriteDML::stageExtraForOrderKey(rwctx, tgt, wId, dId, dNextOId, "c_discount", pv);
                // c_last
                pv = ParamValue{}; pv.kind = ParamKind::String; pv.s = cLast; pv.slen = (int)cLast.size();
                RewriteDML::stageExtraForOrderKey(rwctx, tgt, wId, dId, dNextOId, "c_last", pv);
                // c_credit
                pv = ParamValue{}; pv.kind = ParamKind::String; pv.s = cCredit; pv.slen = (int)cCredit.size();
                RewriteDML::stageExtraForOrderKey(rwctx, tgt, wId, dId, dNextOId, "c_credit", pv);
            }
        } else {
            DbcTools::rollback(hDBC);
            return 0;
        }
    }

    DbcTools::resetStatement(noOrderInsert);
    // 使用事务级上下文：若确定存在合并场景，则延迟 orders；否则走常规路径
    {
        ParamPack pack;
        pack.addInt(dNextOId);
        pack.addInt(dId);
        pack.addInt(wId);
        pack.addInt(cId);
        pack.addTimestamp(oEntryD);
        pack.addInt(olCount);
        pack.addInt(allLocal);
        std::string sql = DialectStrategy::getInstance()->getNoOrderInsert();
        auto old2new = old2new_all;
        auto newCols  = newCols_all;
        auto oldCols  = Config::getOldTableColumns();
        if (mergeRequired) {
            auto rr = RewriteDML::deferMergeInsert(hDBC, "orders", sql, pack, old2new, newCols, oldCols, rwctx);
            if (rr == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
            if (rr == RewriteResult::NotApplicable) {
                // 未识别到可延迟目标，退回常规路径
                auto rr2 = RewriteDML::tryRewriteAndExecInsert(hDBC, "orders", sql, pack, old2new, newCols);
                if (rr2 == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
                if (rr2 == RewriteResult::NotApplicable) {
                    DbcTools::bind(noOrderInsert,1,dNextOId);
                    DbcTools::bind(noOrderInsert,2,dId);
                    DbcTools::bind(noOrderInsert,3,wId);
                    DbcTools::bind(noOrderInsert,4,cId);
                    DbcTools::bind(noOrderInsert,5,oEntryD);
                    DbcTools::bind(noOrderInsert,6,olCount);
                    DbcTools::bind(noOrderInsert,7,allLocal);
                    if(!DbcTools::executePreparedStatement(noOrderInsert)){
                        DbcTools::rollback(hDBC);
                        return 0;
                    }
                }
            }
        } else {
            // 无合并需求，直接常规改写或原语句
            auto rr2 = RewriteDML::tryRewriteAndExecInsert(hDBC, "orders", sql, pack, old2new, newCols);
            if (rr2 == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
            if (rr2 == RewriteResult::NotApplicable) {
                DbcTools::bind(noOrderInsert,1,dNextOId);
                DbcTools::bind(noOrderInsert,2,dId);
                DbcTools::bind(noOrderInsert,3,wId);
                DbcTools::bind(noOrderInsert,4,cId);
                DbcTools::bind(noOrderInsert,5,oEntryD);
                DbcTools::bind(noOrderInsert,6,olCount);
                DbcTools::bind(noOrderInsert,7,allLocal);
                if(!DbcTools::executePreparedStatement(noOrderInsert)){
                    DbcTools::rollback(hDBC);
                    return 0;
                }
            }
        }
    }

    DbcTools::resetStatement(noNewOrderInsert);
    {
        ParamPack pack;
        pack.addInt(dNextOId);
        pack.addInt(dId);
        pack.addInt(wId);
        std::string sql = DialectStrategy::getInstance()->getNoNewOrderInsert();
        auto old2new = old2new_all;
        auto newCols  = newCols_all;
        auto oldCols  = Config::getOldTableColumns();
        if (mergeRequired) {
            auto rr = RewriteDML::deferMergeInsert(hDBC, "neworder", sql, pack, old2new, newCols, oldCols, rwctx);
            if (rr == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
            if (rr == RewriteResult::NotApplicable) {
                auto rr2 = RewriteDML::tryRewriteAndExecInsert(hDBC, "neworder", sql, pack, old2new, newCols);
                if (rr2 == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
                if (rr2 == RewriteResult::NotApplicable) {
                    DbcTools::bind(noNewOrderInsert,1,dNextOId);
                    DbcTools::bind(noNewOrderInsert,2,dId);
                    DbcTools::bind(noNewOrderInsert,3,wId);
                    if(!DbcTools::executePreparedStatement(noNewOrderInsert)){
                        DbcTools::rollback(hDBC);
                        return 0;
                    }
                }
            }
        } else {
            auto rr2 = RewriteDML::tryRewriteAndExecInsert(hDBC, "neworder", sql, pack, old2new, newCols);
            if (rr2 == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
            if (rr2 == RewriteResult::NotApplicable) {
                DbcTools::bind(noNewOrderInsert,1,dNextOId);
                DbcTools::bind(noNewOrderInsert,2,dId);
                DbcTools::bind(noNewOrderInsert,3,wId);
                if(!DbcTools::executePreparedStatement(noNewOrderInsert)){
                    DbcTools::rollback(hDBC);
                    return 0;
                }
            }
        }
    }

	double iPrice;
	int sQuantity;
	string sDist;
	double tmp2;

	// 进入for 先创建一个id_set

	for(int i=0; i<olCount; i++){

		DbcTools::resetStatement(noItemSelect);
		DbcTools::bind(noItemSelect,1,oLines[i].olIId);
		if(!DbcTools::executePreparedStatement(noItemSelect)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		iPrice = 0;
		if(SQL_SUCCESS==SQLFetch(noItemSelect)){
			if(SQL_SUCCESS==SQLGetData(noItemSelect,1,SQL_C_CHAR,buf,1024,&nIdicator))
				iPrice = atof ((char*)buf);
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}
		}
		else{	//Expected Rollback
			if(DbcTools::rollback(hDBC))
				return 1;
			return 0;
		}
		//日志4
		for(int j=0;j<10;j++)
			if(!DbcTools::allocAndPrepareStmt(hDBC, noStockSelects[j], DialectStrategy::getInstance()->getNoStockSelect05()))
				return 0;
		DbcTools::resetStatement(noStockSelects[dId-1]);
		DbcTools::bind(noStockSelects[dId-1],1,oLines[i].olIId);
		DbcTools::bind(noStockSelects[dId-1],2,oLines[i].olSupplyWId);
		if(!DbcTools::executePreparedStatement(noStockSelects[dId-1])){
			DbcTools::rollback(hDBC);
			return 0;
		}
		sQuantity = 0;
		sDist = "";
		if(SQL_SUCCESS==SQLFetch(noStockSelects[dId-1])){
			if(SQL_SUCCESS==SQLGetData(noStockSelects[dId-1],1,SQL_C_CHAR,buf,1024,&nIdicator)){
				sQuantity = strtol ((char*)buf,NULL,0);
			}
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}

			if(SQL_SUCCESS==SQLGetData(noStockSelects[dId-1],2,SQL_C_CHAR,buf,1024,&nIdicator)){
				sDist = string((char*)buf);
			}
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}
		}
		else{
			DbcTools::rollback(hDBC);
			return 0;
		}
		
		DbcTools::resetStatement(noStockUpdates[(oLines[i].olIsRemote?1:0)]);
		DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote?1:0)],1, oLines[i].olQuantity);
		int tmp1 = 0;
		if(oLines[i].olQuantity<=sQuantity-10)
			tmp1 = sQuantity-oLines[i].olQuantity;
		else
			tmp1 = sQuantity-oLines[i].olQuantity+91;
		DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote?1:0)],2, tmp1);
		DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote?1:0)],3, oLines[i].olIId);
		DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote?1:0)],4, oLines[i].olSupplyWId);
        {
            ParamPack pack;
            pack.addInt(oLines[i].olQuantity);
            pack.addInt(tmp1);
            pack.addInt(oLines[i].olIId);
            pack.addInt(oLines[i].olSupplyWId);
            std::string sql = (oLines[i].olIsRemote ?
                std::string(DialectStrategy::getInstance()->getNoStockUpdate02()) :
                std::string(DialectStrategy::getInstance()->getNoStockUpdate01()));
            auto old2new = Config::getOld2New();
            auto newCols = Config::getnewTableColumns();
            auto rr = RewriteDML::tryRewriteAndExecUpdate(hDBC, "stock", sql, pack, old2new, newCols);
            if (rr == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
            if (rr == RewriteResult::NotApplicable) {
                if(!DbcTools::executePreparedStatement(noStockUpdates[(oLines[i].olIsRemote?1:0)])){
                    DbcTools::rollback(hDBC);
                    return 0;
                }
            }
        }

        // 插入 orderline
        // 若需要合并（mergeRequired==true），则先行累积参数，最后一条时统一 flush；
        // 否则按原策略逐条改写/插入。
        tmp1 = i+1;
        tmp2 = iPrice*oLines[i].olQuantity;
        if (mergeRequired) {
            StagedLineArgs args;
            args.o_id = dNextOId; args.d_id = dId; args.w_id = wId; args.number = tmp1;
            args.i_id = oLines[i].olIId; args.supply_w_id = oLines[i].olSupplyWId;
            args.quantity = oLines[i].olQuantity; args.amount = tmp2;
            memset(args.dist, 0, sizeof(args.dist));
            // sDist 应为 24 字节固定宽度
            memcpy(args.dist, sDist.c_str(), std::min<size_t>(sizeof(args.dist), sDist.size()));
            stagedLines.push_back(args);

            // 若是最后一条，则统一 flush 所有累积的行
            if (i == olCount-1) {
                std::string sql = DialectStrategy::getInstance()->getNoOrderlineInsert();
                auto old2new = old2new_all;
                auto newCols  = newCols_all;
                auto oldCols  = Config::getOldTableColumns();
                for (const auto& ln : stagedLines) {
                    ParamPack p;
                    p.addInt(ln.o_id); p.addInt(ln.d_id); p.addInt(ln.w_id);
                    p.addInt(ln.number); p.addInt(ln.i_id); p.addInt(ln.supply_w_id);
                    p.addInt(ln.quantity); p.addDouble(ln.amount); p.addFixedString(ln.dist, 24);
                    // 先尝试合并延迟插入（若有）
                    auto rr = RewriteDML::flushMergeInsert(hDBC, "orderline", sql, p, old2new, newCols, oldCols, rwctx);
                    if (rr == RewriteResult::NotApplicable) {
                        // 常规改写执行（带去重）
                        rr = RewriteDML::tryRewriteAndExecInsertWithCtx(hDBC, "orderline", sql, p, old2new, newCols, oldCols, rwctx);
                    }
                    if (rr == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
                    if (rr == RewriteResult::NotApplicable) {
                        // 兜底：执行原始预处理语句
                        DbcTools::resetStatement(noOrderlineInsert);
                        int v_o = ln.o_id, v_d = ln.d_id, v_w = ln.w_id;
                        int v_num = ln.number, v_i = ln.i_id, v_sw = ln.supply_w_id, v_q = ln.quantity;
                        double v_amt = ln.amount;
                        char v_dist[24]; memcpy(v_dist, ln.dist, sizeof(v_dist));
                        DbcTools::bind(noOrderlineInsert,1,v_o);
                        DbcTools::bind(noOrderlineInsert,2,v_d);
                        DbcTools::bind(noOrderlineInsert,3,v_w);
                        DbcTools::bind(noOrderlineInsert,4,v_num);
                        DbcTools::bind(noOrderlineInsert,5,v_i);
                        DbcTools::bind(noOrderlineInsert,6,v_sw);
                        DbcTools::bind(noOrderlineInsert,7,v_q);
                        DbcTools::bind(noOrderlineInsert,8,v_amt);
                        DbcTools::bind(noOrderlineInsert,9,24,v_dist);
                        if(!DbcTools::executePreparedStatement(noOrderlineInsert)){
                            DbcTools::rollback(hDBC);
                            return 0;
                        }
                    }
                }
            }
        } else {
            DbcTools::resetStatement(noOrderlineInsert);
            {
                ParamPack pack;
                pack.addInt(dNextOId);
                pack.addInt(dId);
                pack.addInt(wId);
                pack.addInt(tmp1);
                pack.addInt(oLines[i].olIId);
                pack.addInt(oLines[i].olSupplyWId);
                pack.addInt(oLines[i].olQuantity);
                pack.addDouble(tmp2);
                char buffer[24];
                memcpy(buffer,sDist.c_str(),sizeof(buffer));
                pack.addFixedString(buffer, 24);
                std::string sql = DialectStrategy::getInstance()->getNoOrderlineInsert();
                auto old2new = old2new_all;
                auto newCols  = newCols_all;
                auto oldCols  = Config::getOldTableColumns();
                // 先尝试合并延迟插入（若有）
                auto rr = RewriteDML::flushMergeInsert(hDBC, "orderline", sql, pack, old2new, newCols, oldCols, rwctx);
                if (rr == RewriteResult::NotApplicable) {
                    // 常规改写执行（带去重）
                    rr = RewriteDML::tryRewriteAndExecInsertWithCtx(hDBC, "orderline", sql, pack, old2new, newCols, oldCols, rwctx);
                }
                if (rr == RewriteResult::Failed) { DbcTools::rollback(hDBC); return 0; }
                if (rr == RewriteResult::NotApplicable) {
                    DbcTools::bind(noOrderlineInsert,1,dNextOId);
                    DbcTools::bind(noOrderlineInsert,2,dId);
                    DbcTools::bind(noOrderlineInsert,3,wId);
                    DbcTools::bind(noOrderlineInsert,4,tmp1);
                    DbcTools::bind(noOrderlineInsert,5,(oLines[i].olIId));
                    DbcTools::bind(noOrderlineInsert,6,(oLines[i].olSupplyWId));
                    DbcTools::bind(noOrderlineInsert,7,(oLines[i].olQuantity));
                    DbcTools::bind(noOrderlineInsert,8,tmp2);
                    DbcTools::bind(noOrderlineInsert,9,24,buffer);
                    if(!DbcTools::executePreparedStatement(noOrderlineInsert)){
                        DbcTools::rollback(hDBC);
                        return 0;
                    }
                }
            }
        }
	}

	//COMMIT
	if(DbcTools::commit(hDBC)){
		return 1;
	}
	DbcTools::rollback(hDBC);
	return 0;
}

bool Transactions::executePayment(SQLHDBC& hDBC){
	//这里提前放出来查询准备
	preparePayment(hDBC);
	//

	//2.5.1.1
	int wId = 0;
	DataSource::randomUniformInt(1,Config::getWarehouseCount(),wId);
	//2.5.1.2
	int dId = 0;
	DataSource::randomUniformInt(1,10,dId);

	int x = 0;
	DataSource::randomUniformInt(1,100,x);
	int cDId = 0;
	int cWId = 0;
	if(x<=85){
		cDId = dId;
		cWId = wId;
	}
	else{
		DataSource::randomUniformInt(1,10,cDId);
		DataSource::getRemoteWId(wId,cWId);
	}

	int y = 0;
	DataSource::randomUniformInt(1,100,y);
	int cId = 0;
	string cLast = "";
	if(y<=60){
		DataSource::randomCLast(cLast);
	}
	else{
		DataSource::randomNonUniformInt(1023,1,3000,867,cId);
	}

	//2.5.1.3
	double hAmount = 0;
	DataSource::randomDouble(1.00,5000.00,2,hAmount);

	//2.5.1.4
	SQL_TIMESTAMP_STRUCT hDate;
	DataSource::getCurrentTimestamp(hDate);

	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};

	//BEGIN TRANSACTION
	DbcTools::resetStatement(pmWarehouseSelect);
	DbcTools::bind(pmWarehouseSelect,1,wId);
	if(!DbcTools::executePreparedStatement(pmWarehouseSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	string wName="";
	if(!DbcTools::fetch(pmWarehouseSelect,buf,&nIdicator,1,wName)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	DbcTools::resetStatement(pmWarehouseUpdate);
	DbcTools::bind(pmWarehouseUpdate,1,hAmount);
	DbcTools::bind(pmWarehouseUpdate,2,wId);
	if(!DbcTools::executePreparedStatement(pmWarehouseUpdate)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	DbcTools::resetStatement(pmDistrictSelect);
	DbcTools::bind(pmDistrictSelect,1,wId);
	DbcTools::bind(pmDistrictSelect,2,dId);
	if(!DbcTools::executePreparedStatement(pmDistrictSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	string dName="";
	if(!DbcTools::fetch(pmDistrictSelect,buf,&nIdicator,1,dName)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	DbcTools::resetStatement(pmDistrictUpdate);
	DbcTools::bind(pmDistrictUpdate,1,hAmount);
	DbcTools::bind(pmDistrictUpdate,2,wId);
	DbcTools::bind(pmDistrictUpdate,3,dId);
	if(!DbcTools::executePreparedStatement(pmDistrictUpdate)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	string cCredit;
	if(y<=60){ //Case 2
		DbcTools::resetStatement(pmCustomerSelect1);
		char buffer1[16];
		memcpy(buffer1,cLast.c_str(),sizeof(buffer1));
		//strcpy(buffer1,cLast.c_str());
		DbcTools::bind(pmCustomerSelect1,1,16,buffer1);
		DbcTools::bind(pmCustomerSelect1,2,cDId);
		DbcTools::bind(pmCustomerSelect1,3,cWId);
		if(!DbcTools::executePreparedStatement(pmCustomerSelect1)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		int count = 0;
		if(!DbcTools::fetch(pmCustomerSelect1,buf,&nIdicator,1,count)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(pmCustomerSelect2);
		char buffer2[16];
		memcpy(buffer2,cLast.c_str(),sizeof(buffer2));
		// strcpy(buffer2,cLast.c_str());
		DbcTools::bind(pmCustomerSelect2,1,16,buffer2);
		DbcTools::bind(pmCustomerSelect2,2,cDId);
		DbcTools::bind(pmCustomerSelect2,3,cWId);
		if(!DbcTools::executePreparedStatement(pmCustomerSelect2)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		cId	= 0;
		cCredit = "";
		for(int i=0; i < ((count+1)/2)-1; i++){ //move cursor
			SQLFetch(pmCustomerSelect2);
		}
		if(SQL_SUCCESS==SQLFetch(pmCustomerSelect2)){
			if(SQL_SUCCESS==SQLGetData(pmCustomerSelect2,1,SQL_C_CHAR,buf,1024,&nIdicator))
				cId = strtol ((char*)buf,NULL,0);
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}
			if(SQL_SUCCESS==SQLGetData(pmCustomerSelect2,11,SQL_C_CHAR,buf,1024,&nIdicator))
				cCredit = string((char*)buf);
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}
		}
		else{
			DbcTools::rollback(hDBC);
			return 0;
		}
	}
	else{ //Case 1
		DbcTools::resetStatement(pmCustomerSelect3);
		DbcTools::bind(pmCustomerSelect3,1,cId);
		DbcTools::bind(pmCustomerSelect3,2,cDId);
		DbcTools::bind(pmCustomerSelect3,3,cWId);
		if(!DbcTools::executePreparedStatement(pmCustomerSelect3)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		cCredit = "";
		if(!DbcTools::fetch(pmCustomerSelect3,buf,&nIdicator,11,cCredit)){
			DbcTools::rollback(hDBC);
			return 0;
		}
	}

	DbcTools::resetStatement(pmCustomerUpdate1);
	DbcTools::bind(pmCustomerUpdate1,1,hAmount);
	DbcTools::bind(pmCustomerUpdate1,2,hAmount);
	DbcTools::bind(pmCustomerUpdate1,3,cId);
	DbcTools::bind(pmCustomerUpdate1,4,cDId);
	DbcTools::bind(pmCustomerUpdate1,5,cWId);
	if(!DbcTools::executePreparedStatement(pmCustomerUpdate1)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	if(cCredit=="BC"){
		DbcTools::resetStatement(pmCustomerSelect4);
		DbcTools::bind(pmCustomerSelect4,1,cId);
		DbcTools::bind(pmCustomerSelect4,2,cDId);
		DbcTools::bind(pmCustomerSelect4,3,cWId);
		if(!DbcTools::executePreparedStatement(pmCustomerSelect4)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		string cData = "";
		if(!DbcTools::fetch(pmCustomerSelect4,buf,&nIdicator,1,cData)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		cData = to_string(cId)+","+to_string(cDId)+","+to_string(cWId)+","+to_string(dId)+","+to_string(wId)+","+to_string(hAmount)+","+cData;
		if(cData.length()>500)
			cData = cData.substr(0,500);

		DbcTools::resetStatement(pmCustomerUpdate2);
		char buffer3[500];
		//strcpy(buffer3,cData.c_str());
		memcpy(buffer3,cData.c_str(),sizeof(buffer3));

		DbcTools::bind(pmCustomerUpdate2,1,500,buffer3);
		DbcTools::bind(pmCustomerUpdate2,2,cId);
		DbcTools::bind(pmCustomerUpdate2,3,cDId);
		DbcTools::bind(pmCustomerUpdate2,4,cWId);
		if(!DbcTools::executePreparedStatement(pmCustomerUpdate2)){
			DbcTools::rollback(hDBC);
			return 0;
		}
	}

	string hData = wName + "    " + dName;

	DbcTools::resetStatement(pmHistoryInsert);
	DbcTools::bind(pmHistoryInsert,1,cId);
	DbcTools::bind(pmHistoryInsert,2,cDId);
	DbcTools::bind(pmHistoryInsert,3,cWId);
	DbcTools::bind(pmHistoryInsert,4,dId);
	DbcTools::bind(pmHistoryInsert,5,wId);
	DbcTools::bind(pmHistoryInsert,6,hDate);
	DbcTools::bind(pmHistoryInsert,7,hAmount);
	char buffer4[24];
	//strcpy(buffer4,hData.c_str());
	memcpy(buffer4,hData.c_str(),sizeof(buffer4));
	DbcTools::bind(pmHistoryInsert,8,24,buffer4);
	if(!DbcTools::executePreparedStatement(pmHistoryInsert)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	//COMMIT
	if(DbcTools::commit(hDBC)){
		return 1;
	}
	DbcTools::rollback(hDBC);
	return 0;
}

bool Transactions::executeOrderStatus(SQLHDBC& hDBC){
	prepareOrderStatus(hDBC);
	//2.6.1.1
	int wId = 0;
	DataSource::randomUniformInt(1,Config::getWarehouseCount(), wId);
	//2.6.1.2
	int dId = 0;
	DataSource::randomUniformInt(1,10,dId);
	int y = 0;
	DataSource::randomUniformInt(1,100,y);
	int cId = 0;
	string cLast = "";
	if(y<=60){
		DataSource::randomCLast(cLast);
	}
	else{
		DataSource::randomNonUniformInt(1023,1,3000,867,cId);
	}

	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};

	//BEGIN TRANSACTION
	if(y<=60){ //Case 2
		DbcTools::resetStatement(osCustomerSelect1);
		char buffer1[16];
		//strcpy(buffer1,cLast.c_str());
		memcpy(buffer1,cLast.c_str(),sizeof(buffer1));
		DbcTools::bind(osCustomerSelect1,1,16,buffer1);
		DbcTools::bind(osCustomerSelect1,2,dId);
		DbcTools::bind(osCustomerSelect1,3,wId);
		if(!DbcTools::executePreparedStatement(osCustomerSelect1)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		int count = 0;
		if(!DbcTools::fetch(osCustomerSelect1, buf, &nIdicator, 1, count)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(osCustomerSelect2);
		char buffer2[16];
		//strcpy(buffer2,cLast.c_str());
		memcpy(buffer2,cLast.c_str(),sizeof(buffer2));
		DbcTools::bind(osCustomerSelect2,1,16,buffer2);
		DbcTools::bind(osCustomerSelect2,2,dId);
		DbcTools::bind(osCustomerSelect2,3,wId);
		if(!DbcTools::executePreparedStatement(osCustomerSelect2)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		for(int i=0; i < ((count+1)/2)-1;i++){ //move cursor
			SQLFetch(osCustomerSelect2);
		}
		if(!DbcTools::fetch(osCustomerSelect2, buf, &nIdicator, 1, cId)){
			DbcTools::rollback(hDBC);
			return 0;
		}
	}
	else{ //Case 1
		DbcTools::resetStatement(osCustomerSelect3);
		DbcTools::bind(osCustomerSelect3,1,cId);
		DbcTools::bind(osCustomerSelect3,2,dId);
		DbcTools::bind(osCustomerSelect3,3,wId);
		if(!DbcTools::executePreparedStatement(osCustomerSelect3)){
			DbcTools::rollback(hDBC);
			return 0;
		}
	}

	DbcTools::resetStatement(osOrderSelect);
	DbcTools::bind(osOrderSelect,1,wId);
	DbcTools::bind(osOrderSelect,2,dId);
	DbcTools::bind(osOrderSelect,3,cId);
	DbcTools::bind(osOrderSelect,4,wId);
	DbcTools::bind(osOrderSelect,5,dId);
	DbcTools::bind(osOrderSelect,6,cId);
	if(!DbcTools::executePreparedStatement(osOrderSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	int oId = 0;
	if(!DbcTools::fetch(osOrderSelect, buf, &nIdicator, 1, oId)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	DbcTools::resetStatement(osOrderlineSelect);
	DbcTools::bind(osOrderlineSelect,1,wId);
	DbcTools::bind(osOrderlineSelect,2,dId);
	DbcTools::bind(osOrderlineSelect,3,oId);
	if(!DbcTools::executePreparedStatement(osOrderlineSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	//COMMIT
	if(DbcTools::commit(hDBC)){
		return 1;
	}
	DbcTools::rollback(hDBC);
	return 0;
}

bool Transactions::executeDelivery(SQLHDBC& hDBC){
	
	//2.7.1.1
	int wId = 0;
	DataSource::randomUniformInt(1,Config::getWarehouseCount(), wId);
	//2.7.1.2
	int oCarrierId = 0;
	DataSource::randomUniformInt(1,10,oCarrierId);
	//2.7.1.3
	SQL_TIMESTAMP_STRUCT olDeliveryD;
	DataSource::getCurrentTimestamp(olDeliveryD);

	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};

	//BEGIN TRANSACTION
	int noOId;
	int oCId;
	double olAmount;
	for(int dId=1; dId <=10; dId++){
		prepareDelivery(hDBC);
		DbcTools::resetStatement(dlNewOrderSelect);
		DbcTools::bind(dlNewOrderSelect,1,wId);
		DbcTools::bind(dlNewOrderSelect,2,dId);
		DbcTools::bind(dlNewOrderSelect,3,wId);
		DbcTools::bind(dlNewOrderSelect,4,dId);
		if(!DbcTools::executePreparedStatement(dlNewOrderSelect)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		noOId = 0;
		if(SQL_SUCCESS==SQLFetch(dlNewOrderSelect)){
			if(SQL_SUCCESS==SQLGetData(dlNewOrderSelect,1,SQL_C_CHAR,buf,1024,&nIdicator))
				noOId = strtol ((char*)buf,NULL,0);
			else{
				DbcTools::rollback(hDBC);
				return 0;
			}
		}
		else //If no matching row is found, then the delivery of an order for this district is skipped.
			continue;

		DbcTools::resetStatement(dlNewOrderDelete);
		DbcTools::bind(dlNewOrderDelete,1,wId);
		DbcTools::bind(dlNewOrderDelete,2,dId);
		DbcTools::bind(dlNewOrderDelete,3,noOId);
		if(!DbcTools::executePreparedStatement(dlNewOrderDelete)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(dlOrderSelect);
		DbcTools::bind(dlOrderSelect,1,wId);
		DbcTools::bind(dlOrderSelect,2,dId);
		DbcTools::bind(dlOrderSelect,3,noOId);
		if(!DbcTools::executePreparedStatement(dlOrderSelect)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		oCId = 0;
		if(!DbcTools::fetch(dlOrderSelect, buf, &nIdicator, 1, oCId)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(dlOrderUpdate);
		DbcTools::bind(dlOrderUpdate,1,oCarrierId);
		DbcTools::bind(dlOrderUpdate,2,wId);
		DbcTools::bind(dlOrderUpdate,3,dId);
		DbcTools::bind(dlOrderUpdate,4,noOId);
		if(!DbcTools::executePreparedStatement(dlOrderUpdate)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(dlOrderlineUpdate);
		DbcTools::bind(dlOrderlineUpdate,1,olDeliveryD);
		DbcTools::bind(dlOrderlineUpdate,2,wId);
		DbcTools::bind(dlOrderlineUpdate,3,dId);
		DbcTools::bind(dlOrderlineUpdate,4,noOId);
		if(!DbcTools::executePreparedStatement(dlOrderlineUpdate)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(dlOrderlineSelect);
		DbcTools::bind(dlOrderlineSelect,1,wId);
		DbcTools::bind(dlOrderlineSelect,2,dId);
		DbcTools::bind(dlOrderlineSelect,3,noOId);
		if(!DbcTools::executePreparedStatement(dlOrderlineSelect)){
			DbcTools::rollback(hDBC);
			return 0;
		}
		olAmount = 0;
		if(!DbcTools::fetch(dlOrderlineSelect, buf, &nIdicator, 1, olAmount)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		DbcTools::resetStatement(dlCustomerUpdate);
		DbcTools::bind(dlCustomerUpdate,1,olAmount);
		DbcTools::bind(dlCustomerUpdate,2,oCId);
		DbcTools::bind(dlCustomerUpdate,3,dId);
		DbcTools::bind(dlCustomerUpdate,4,wId);
		if(!DbcTools::executePreparedStatement(dlCustomerUpdate)){
			DbcTools::rollback(hDBC);
			return 0;
		}

		//COMMIT
		if(!DbcTools::commit(hDBC)){
			DbcTools::rollback(hDBC);
			return 0;
		}
	}
	return 1;
}

bool Transactions::executeStockLevel(SQLHDBC& hDBC){
	prepareStockLevel(hDBC);
	//2.8.1.1
	int wId = 0;
	DataSource::randomUniformInt(1,Config::getWarehouseCount(), wId);
	int dId = 0;
	DataSource::randomUniformInt(1,10, dId);
	//2.8.1.2
	int threshold = 0;
	DataSource::randomUniformInt(10,20,threshold);

	SQLLEN nIdicator = 0;
	SQLCHAR buf[1024] = {0};

	//BEGIN TRANSACTION
	DbcTools::resetStatement(slDistrictSelect);
	DbcTools::bind(slDistrictSelect,1,wId);
	DbcTools::bind(slDistrictSelect,2,dId);
	if(!DbcTools::executePreparedStatement(slDistrictSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}
	int dNextOId=0;
	if(!DbcTools::fetch(slDistrictSelect, buf, &nIdicator, 1, dNextOId)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	DbcTools::resetStatement(slStockSelect);
	DbcTools::bind(slStockSelect,1,wId);
	DbcTools::bind(slStockSelect,2,dId);
	DbcTools::bind(slStockSelect,3,dNextOId);
	int tmp = dNextOId-20;
	DbcTools::bind(slStockSelect,4,tmp);
	DbcTools::bind(slStockSelect,5,wId);
	DbcTools::bind(slStockSelect,6,threshold);
	//日志4
	if(!DbcTools::executePreparedStatement(slStockSelect)){
		DbcTools::rollback(hDBC);
		return 0;
	}

	//COMMIT
	if(DbcTools::commit(hDBC)){
		return 1;
	}
	DbcTools::rollback(hDBC);
	return 0;
}
