#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_opt_2pl.h"
#include "row_rdma_opt_2pl.h"

#if CC_ALG == RDMA_OPT_NO_WAIT ||  CC_ALG == RDMA_OPT_WAIT_DIE
void RDMA_opt_2pl::write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	if (rc != Abort) row->copy(data);
    uint64_t lock_info = row->lock_info;
    row->lock_info = 0;
#if DEBUG_PRINTF
		printf("---thread id: %lu, local unlock write suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), g_node_id, &row, txnMng->get_txn_id());
#endif
}

void RDMA_opt_2pl::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();

    uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(access->offset);
    row_t *data = access->data;
    data->lock_info = 0; //write data and unlock

    uint64_t operate_size = 0;
    uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)data);    
    if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size) - sizeof(data->conflict_num) - sizeof(data->is_hot) - sizeof(data->rcnt_neg) - sizeof(data->rcnt_pos);
    else operate_size = sizeof(uint64_t);

    assert(txnMng->write_remote_row(yield,loc,operate_size,remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id) == true);

#if DEBUG_PRINTF
		printf("---thread id: %lu, remote unlock write suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf+access->offset, txnMng->get_txn_id());
#endif
}

void RDMA_opt_2pl::unlock(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, lock_t lock_type, uint64_t cor_id){
    uint64_t try_lock = -1;
    uint64_t loc = g_node_id;
    uint64_t lock_info_pointer = row_t::get_lock_info_pointer((char*)row - rdma_global_buffer);
    uint64_t new_lock_info = txnMng->get_txn_id() + 1;
    uint64_t tts = txnMng->get_timestamp();
    uint64_t rcnt_neg_pointer = row_t::get_rcnt_neg_pointer((char*)row - rdma_global_buffer);    
#if CC_ALG == RDMA_OPT_NO_WAIT
    if (lock_type == DLOCK_EX) {
        row->lock_info = 0;
        // try_lock = txnMng->cas_remote_content(yield,loc,lock_info_pointer,new_lock_info,0,cor_id);
        // assert(try_lock == new_lock_info);
    } else if (lock_type == DLOCK_SH){
        txnMng->faa_remote_content(yield, loc, rcnt_neg_pointer, cor_id);
        // txnMng->loop_cas_remote(yield, loc, lock_info_pointer, 0, new_lock_info, cor_id);
        // row->read_cnt --;
        // row->lock_info = 0;
    }
#endif 
#if CC_ALG == RDMA_OPT_WAIT_DIE
    if (lock_type == DLOCK_EX) { //WRITE(lock_info=0, ts_array)
        int i;
        for(i=0;i<LOCK_LENGTH;i++){
            if(row->ts[i] == tts){
                row->ts[i] = 0;
                break;
            }
        }
        if(i == LOCK_LENGTH) assert(false); 
        row->lock_info = 0;
    } else if (lock_type == DLOCK_SH){ //Write(ts_array) FAA(rcnt_neg)
        int i;
        for(i=0;i<LOCK_LENGTH;i++){
            if(row->ts[i] == tts){
                row->ts[i] = 0;
                break;
            }
        }
        if(i == LOCK_LENGTH) assert(false);     
        txnMng->faa_remote_content(yield, loc, rcnt_neg_pointer, cor_id);
    }
#if DEBUG_PRINTF
		printf("---thread id: %lu, local unlock read suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), loc, &row, txnMng->get_txn_id());
#endif
#endif
}

void RDMA_opt_2pl::remote_unlock(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t num, uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    lock_t lock_type = access->lock_type;
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(access->offset);
    uint64_t remote_rcnt_neg_pointer = row_t::get_rcnt_neg_pointer(access->offset);
    uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)data);    
    uint64_t new_lock_info = txnMng->get_txn_id() + 1;
    uint64_t try_lock = -1;
    uint64_t tts = txnMng->get_timestamp();
#if CC_ALG == RDMA_OPT_NO_WAIT
    if (lock_type == DLOCK_EX) {
        uint64_t* new_lock = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
        *new_lock= 0;
        assert(txnMng->write_remote_row(yield,loc,sizeof(uint64_t),remote_lock_info_pointer,(char*)new_lock,cor_id)==true);
        mem_allocator.free(new_lock, sizeof(uint64_t));
    } else if (lock_type == DLOCK_SH){
        txnMng->faa_remote_content(yield, loc, remote_rcnt_neg_pointer,cor_id);    
    }
#endif
#if CC_ALG == RDMA_OPT_WAIT_DIE
    if (lock_type == DLOCK_EX) { //WRITE(lock_info=0, ts_array)
        int i;
        for(i=0;i<LOCK_LENGTH;i++){
            if(data->ts[i] == tts){
                data->ts[i] = 0;
                break;
            }
        }
        if(i == LOCK_LENGTH) assert(false); 
        data->lock_info = 0;
        uint64_t operate_size = sizeof(data->lock_info) + sizeof(data->ts[0])*LOCK_LENGTH;
        assert(txnMng->write_remote_row(yield,loc,operate_size,remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id)==true);
    } else if (lock_type == DLOCK_SH){ //Write(ts_array) FAA(rcnt_neg)
        int i;
        for(i=0;i<LOCK_LENGTH;i++){
            if(data->ts[i] == tts){
                data->ts[i] = 0;
                break;
            }
        }
        if(i == LOCK_LENGTH) assert(false); 
        uint64_t remote_ts_pointer = row_t::get_ts_pointer(access->offset,i);
        uint64_t local_ts_pointer = row_t::get_ts_pointer((uint64_t)data,i);
        assert(txnMng->write_remote_row(yield,loc,sizeof(data->ts[i]),remote_ts_pointer,(char*)local_ts_pointer,cor_id)==true);
        txnMng->faa_remote_content(yield, loc, remote_rcnt_neg_pointer,cor_id);    
    }
#if DEBUG_PRINTF
		printf("---thread id: %lu, remote unlock read suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf+access->offset, txnMng->get_txn_id());
#endif
#endif
}

//write back and unlock
void RDMA_opt_2pl::finish(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t cor_id){
	Transaction *txn = txnMng->txn;
    uint64_t starttime = get_sys_clock();
    // printf("txn %ld enter finish function with %ld access\n", txnMng->get_txn_id(), txn->row_cnt);
    //NO_WAIT has no problem of deadlock,so doesnot need to bubble sort the write_set in primary key order
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
    
    vector<vector<uint64_t>> remote_access(g_node_cnt);
    //for read set element, release lock
    for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            // printf("txn %ld access lock type = %ld", txnMng->get_txn_id(), access->lock_type);
            unlock(yield, rc, access->orig_row, txnMng, access->lock_type, cor_id);
        }else{
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(yield, rc, txnMng, read_set[i], cor_id);
        }
    }
    //for write set element,write back and release lock
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            write_and_unlock(yield, rc, access->orig_row, access->data, txnMng,cor_id); 
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(yield, rc, txnMng, txnMng->write_set[i], cor_id);
        }
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
            mem_allocator.free(txn->accesses[i]->orig_row,0);
            // mem_allocator.free(txn->accesses[i]->test_row,0);
            txn->accesses[i]->data = NULL;
            txn->accesses[i]->orig_row = NULL;
            txn->accesses[i]->orig_data = NULL;
            txn->accesses[i]->version = 0;

            //txn->accesses[i]->test_row = NULL;
            txn->accesses[i]->offset = 0;
        } else {
            mem_allocator.free(txn->accesses[i]->data,0);
            // mem_allocator.free(txn->accesses[i]->orig_row,0);
            // mem_allocator.free(txn->accesses[i]->test_row,0);
            txn->accesses[i]->data = NULL;
            txn->accesses[i]->orig_row = NULL;
            txn->accesses[i]->orig_data = NULL;
            txn->accesses[i]->version = 0;

            //txn->accesses[i]->test_row = NULL;
            txn->accesses[i]->offset = 0;           
        }
    }
	memset(txnMng->write_set, 0, 100);

}
#endif
