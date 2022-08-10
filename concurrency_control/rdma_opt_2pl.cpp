#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_opt_2pl.h"
#include "row_rdma_opt_2pl.h"

#if CC_ALG == RDMA_OPT_NO_WAIT ||  CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_OPT_NO_WAIT2
void RDMA_opt_2pl::write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	if (rc != Abort) row->copy(data);
    uint64_t lock_info = row->lock_info;
    row->lock_info = 0;
    // printf("[rdma_opt_2pl.cpp:16]unlock local write key = %ld\n",row->get_primary_key());

#if DEBUG_PRINTF
		printf("---thread id: %lu, local unlock write suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), g_node_id, &row, txnMng->get_txn_id());
#endif
}

void RDMA_opt_2pl::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();

    row_t *data = access->data;
    data->lock_info = 0; //write data and unlock

    uint64_t operate_size = 0;
       
#if CC_ALG != RDMA_OPT_NO_WAIT2
    if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size) - sizeof(data->conflict_num) - sizeof(data->is_hot) - sizeof(data->rcnt_neg) - sizeof(data->rcnt_pos);
    else operate_size = sizeof(uint64_t);
    uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(access->offset);
    uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)data); 
    assert(txnMng->write_remote_row(yield,loc,operate_size,remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id) == true);
#else
    operate_size = sizeof(uint64_t);
    uint64_t row_offset = access->offset;
    uint64_t* new_lock = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
    *new_lock= 0;
    // assert(txnMng->write_remote_row(yield,loc,operate_size,access->offset + sizeof(uint64_t)*2,(char*)((uint64_t)data+sizeof(uint64_t)*2),cor_id) == true);
    assert(txnMng->write_remote_row(yield,loc,sizeof(uint64_t),row_offset+sizeof(uint64_t)*2,(char*)new_lock,cor_id)==true);
    // printf("[rdma_opt_2pl.cpp:47]unlock remote write key = %ld\n",data->get_primary_key());
    mem_allocator.free(new_lock, sizeof(uint64_t));
#endif
#if DEBUG_PRINTF
		printf("---thread id: %lu, remote unlock write suc, lock location: %lu; %p, txn id: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf+access->offset, txnMng->get_txn_id());
#endif
}

void RDMA_opt_2pl::unlock(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, lock_t lock_type, uint64_t cor_id){
#if CC_ALG != RDMA_OPT_NO_WAIT2
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

#else
    uint64_t try_lock = -1;
    uint64_t loc = g_node_id;
    // uint64_t new_lock_info = txnMng->get_txn_id() + 1;
    uint64_t tts = txnMng->get_timestamp();
    if (lock_type == DLOCK_EX) {
        row->lock_info = 0;//TODO - CAS or RDMA_CAS?
        // try_lock = txnMng->cas_remote_content(yield,loc,lock_info_pointer,new_lock_info,0,cor_id);
    } else if (lock_type == DLOCK_SH){
        uint64_t row_offset = (char*)row - rdma_global_buffer; 
        uint64_t faa_num = (-1)<<2;
        uint64_t old_lock_info = txnMng->faa_remote_content(yield, loc, row_offset + sizeof(uint64_t)*2, faa_num,cor_id);
        
        // printf("[rdma_opt_2pl.cpp:115]");
        uint64_t read_lock_cnt = row_t::decode_lock_info_cnt(old_lock_info);
        if(read_lock_cnt == 1){
            old_lock_info = (read_lock_cnt - 1)<<2;
            old_lock_info = old_lock_info & 0x03;
            txnMng->cas_remote_content(loc,row_offset+sizeof(uint64_t)*2,old_lock_info,0);//TODO fail?
        }
    }
#endif
}

void RDMA_opt_2pl::remote_unlock(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t num, uint64_t cor_id){
#if CC_ALG != RDMA_OPT_NO_WAIT2
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
#else
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    lock_t lock_type = access->lock_type;
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    if (lock_type == DLOCK_EX) {
        uint64_t* new_lock = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
        *new_lock= 0;
        assert(txnMng->write_remote_row(yield,loc,sizeof(uint64_t),off+sizeof(uint64_t)*2,(char*)new_lock,cor_id)==true);
        mem_allocator.free(new_lock, sizeof(uint64_t));
    } else if (lock_type == DLOCK_SH){
        uint64_t faa_num = (-1)<<2;
        // faa_num = faa_num | 0x03;
        uint64_t old_lock_info = txnMng->faa_remote_content(yield, loc, off + sizeof(uint64_t)*2, faa_num,cor_id);
        
        uint64_t read_lock_cnt = row_t::decode_lock_info_cnt(old_lock_info);
        if(read_lock_cnt == 1){
            old_lock_info = (read_lock_cnt - 1)<<2;
            old_lock_info = old_lock_info & 0x03;
            txnMng->cas_remote_content(loc,off+sizeof(uint64_t)*2,old_lock_info,0);//TODO check
        }  
    }

    
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
            remote_access[txn->accesses[read_set[i]]->location].push_back(read_set[i]);
        #if USE_DBPAOR == false 
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(yield,rc , txnMng, read_set[i],cor_id);
        #endif
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
            remote_access[txn->accesses[txnMng->write_set[i]]->location].push_back(txnMng->write_set[i]);
        #if USE_DBPAOR == false 
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(yield,rc, txnMng, txnMng->write_set[i],cor_id);
        #endif
        }
    }


#if USE_DBPAOR == true && CC_ALG == RDMA_OPT_NO_WAIT
    starttime = get_sys_clock();
    uint64_t endtime;
    for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
        if(remote_access[i].size() > 0){
            txnMng->opt_batch_unlock_remote(yield, cor_id, i, rc, txnMng, remote_access);
        }
    }

    for(int i=0;i<g_node_cnt;i++){ //poll result
        if(remote_access[i].size() > 0){
        	//to do: add coroutine
            INC_STATS(txnMng->get_thd_id(), worker_oneside_cnt, 1);
        #if USE_COROUTINE
			uint64_t waitcomp_time;
			std::pair<int,ibv_wc> dbres1;
			INC_STATS(txnMng->get_thd_id(), worker_process_time, get_sys_clock() - txnMng->h_thd->cor_process_starttime[cor_id]);
			
			do {
				txnMng->h_thd->start_wait_time = get_sys_clock();
				txnMng->h_thd->last_yield_time = get_sys_clock();
				// printf("do\n");
				yield(txnMng->h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
				uint64_t yield_endtime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), worker_yield_cnt, 1);
				INC_STATS(txnMng->get_thd_id(), worker_yield_time, yield_endtime - txnMng->h_thd->last_yield_time);
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, yield_endtime - txnMng->h_thd->last_yield_time);
				dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_total_thread_cnt]->poll_send_comp();
				waitcomp_time = get_sys_clock();
				
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
				INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
			} while (dbres1.first == 0);
			txnMng->h_thd->cor_process_starttime[cor_id] = get_sys_clock();
			// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
        #else  
            
            auto dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_total_thread_cnt]->wait_one_comp();
            RDMA_ASSERT(dbres1 == IOCode::Ok);  
            endtime = get_sys_clock();
            INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
            INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
            DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
        #endif     
        }
    }
#endif

    #if CC_ALG == RDMA_OPT_NO_WAIT
    
    #endif 

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
