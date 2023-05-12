#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_range_lock.h"
#include "row_rdma_range_lock.h"
#include "ycsb_query.h"

#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
void RDMA_range_lock::unlock_read_data(yield_func_t &yield,TxnManager *txnMng, int *read_set,RC rc, uint64_t cor_id){
    Transaction *txn = txnMng->txn;

    for (int i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            uint64_t faa_num = (-1)<<1;
            uint64_t faa_result = 0;
            faa_result = txnMng->faa_remote_content(yield,g_node_id,(char *)(access->orig_row) - rdma_global_buffer,faa_num,cor_id);
        }else{
            Access * access = txn->accesses[ read_set[i] ];
            uint64_t faa_num = (-1)<<1;
            uint64_t faa_result = 0;
            faa_result = txnMng->faa_remote_content(yield,access->location,access->offset,faa_num,cor_id);
        }
    }

}

void RDMA_range_lock::unlock_write_data(yield_func_t &yield,TxnManager *txnMng,RC rc, uint64_t cor_id){
    Transaction *txn = txnMng->txn;
    vector<vector<uint64_t>> remote_access(g_node_cnt);
            //for write set element,write back and release lock
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        // printf("[rdma_range_lock.cpp:35]release x lock");
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
                write_and_unlock(yield, rc, access->orig_row, access->data, txnMng,access->leaf_offset,cor_id); 
        }else{
            //remote
                remote_access[txn->accesses[txnMng->write_set[i]]->location].push_back(txnMng->write_set[i]);
                Access * access = txn->accesses[ txnMng->write_set[i] ];
                remote_write_and_unlock(yield,rc, txnMng, txnMng->write_set[i],cor_id);
        }
    }

}

void RDMA_range_lock::unlock_read_range(yield_func_t &yield,TxnManager *txnMng,uint64_t cor_id){
    Transaction *txn = txnMng->txn;
    int range_count = txn->locked_range_num;
    for(int i = 0; i < range_count; i++){
        uint64_t faa_num = (-1)<<1;
        uint64_t faa_result = txnMng->faa_remote_content(yield,txn->server_set[i],txn->range_node_set[i],faa_num,cor_id);
        uint64_t ix,x,is,s;
        // ix = txnMng->decode_ix_lock(faa_result);
        x = txnMng->decode_x_lock(faa_result);
        // is = txnMng->decode_is_lock(faa_result);
        s = txnMng->decode_s_lock(faa_result);
        // printf("[rdma_range_lock.cpp:72]txn%ld release lock,offset=%ld,ix%ld,x%ld,is%ld,s%ld\n",txnMng->get_txn_id(),txn->range_node_set[i],ix,x,is,s);
    }
}

void RDMA_range_lock::write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t leaf_offset,uint64_t cor_id) {
	if (rc != Abort) row->copy(data);
#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK
    //release data lock(X)
    uint64_t faa_num = -1;
    uint64_t cas_result = 0;
    while(cas_result != 1 && !simulation->is_done()){
        cas_result = txnMng->cas_remote_content(yield,g_node_id,(char *)row - rdma_global_buffer,1,0,cor_id);
    }
#elif CC_ALG == RDMA_SINGLE_RANGE_LOCK
    //release range lock(X)
    uint64_t cas_result = 0;
    while(cas_result != 1 && !simulation->is_done()){
        cas_result = txnMng->cas_remote_content(yield,g_node_id,leaf_offset,1,0,cor_id);
    }
#endif
}

void RDMA_range_lock::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();

    row_t *data = access->data;
    // data->lock_info = 0; //write data and unlock

    uint64_t operate_size = 0;
       
    //TODO - ignore write operate
#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK    
    //release data lock(X)
    uint64_t cas_result = 0;
    while(cas_result != 1 && !simulation->is_done()){
        cas_result = txnMng->cas_remote_content(yield,loc,off,1,0,cor_id);
    }
#elif CC_ALG == RDMA_SINGLE_RANGE_LOCK
    uint64_t cas_result = 0;
    while(cas_result != 1 && !simulation->is_done()){
        cas_result = txnMng->cas_remote_content(yield,loc,access->leaf_offset,1,0,cor_id);
    }
#endif
}

void RDMA_range_lock::unlock_read(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, uint64_t leaf_offset, uint64_t cor_id){
}

void RDMA_range_lock::remote_unlock_read(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t num, uint64_t cor_id){
}

void RDMA_range_lock::unlock_range_read(yield_func_t &yield, uint64_t cor_id,TxnManager * txnMng ,uint64_t remote_server, uint64_t range_offset){
    //0x00S0
    uint64_t faa_num = (uint64_t)((-1)<<1);
    // faa_num = faa_num*(-1);
    uint64_t faa_result = txnMng->faa_remote_content(yield,remote_server,range_offset,faa_num,cor_id);

#if INDEX_STRUCT == IDX_RDMA_BTREE
    rdma_bt_node * remote_bt_node = txnMng->read_remote_bt_node(yield,remote_server,range_offset,cor_id);
#elif INDEX_STRUCT == IDX_LEARNED
    LeafIndexInfo * remote_bt_node = txnMng->read_remote_learn_node(yield,remote_server,range_offset,cor_id);
#endif
    uint64_t tmp_intent = remote_bt_node->intent_lock;
}

//write back and unlock
void RDMA_range_lock::finish(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t cor_id){
 
	Transaction *txn = txnMng->txn;
    uint64_t starttime = get_sys_clock();
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
	YCSBQuery* ycsb_query = (YCSBQuery*) (txnMng->query);
    if(ycsb_query->query_type == YCSB_CONTINUOUS){
#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK
        unlock_read_data(yield,txnMng,read_set,rc,cor_id);
        unlock_read_range(yield,txnMng,cor_id);
#elif CC_ALG == RDMA_SINGLE_RANGE_LOCK
        unlock_read_range(yield,txnMng,cor_id);
#endif
    }else if(ycsb_query->query_type == YCSB_DISCRETE){//common txn
        //for read set element, release lock
        unlock_read_data(yield,txnMng,read_set,rc,cor_id);      
        unlock_write_data(yield,txnMng,rc,cor_id);
    }else{
        
    }    

    uint64_t timespan = get_sys_clock() - starttime;
    txnMng->txn_stats.cc_time += timespan;
    txnMng->txn_stats.cc_time_short += timespan;
    INC_STATS(txnMng->get_thd_id(),twopl_release_time,timespan);
    INC_STATS(txnMng->get_thd_id(),twopl_release_cnt,1);
    
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
            //remote
            mem_allocator.free(txn->accesses[i]->data,0);
            // mem_allocator.free(txn->accesses[i]->orig_row,0);
            // mem_allocator.free(txn->accesses[i]->test_row,0);
            txn->accesses[i]->data = NULL;
            // txn->accesses[i]->orig_row = NULL;
            // txn->accesses[i]->orig_data = NULL;
            txn->accesses[i]->version = 0;
            txn->accesses[i]->leaf_offset = 0;
            txn->accesses[i]->location = 0;

            //txn->accesses[i]->test_row = NULL;
            txn->accesses[i]->offset = 0;
        } else {
            mem_allocator.free(txn->accesses[i]->data,0);
            // mem_allocator.free(txn->accesses[i]->orig_row,0);
            // mem_allocator.free(txn->accesses[i]->test_row,0);
            txn->accesses[i]->data = NULL;
            // txn->accesses[i]->orig_row = NULL;
            // txn->accesses[i]->orig_data = NULL;
            txn->accesses[i]->version = 0;
            txn->accesses[i]->leaf_offset = 0;
            txn->accesses[i]->location = 0;

            //txn->accesses[i]->test_row = NULL;
            txn->accesses[i]->offset = 0;           
        }
    }
	memset(txnMng->write_set, 0, 100);
}
#endif
