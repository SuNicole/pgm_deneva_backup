#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_opt_no_wait3.h"
#include "row_rdma_opt_no_wait3.h"
#include "ycsb_query.h"

#if CC_ALG == RDMA_OPT_NO_WAIT3
void RDMA_opt_no_wait3::write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t leaf_offset,uint64_t cor_id) {
	if (rc != Abort) row->copy(data);

    //release data lock(X)
    uint64_t faa_num = -1;
    uint64_t faa_result = 0;
    txnMng->faa_remote_content(yield,g_node_id,(char *)row - rdma_global_buffer,faa_num,cor_id);

    //release range lock(IX)
    faa_num = (-1)<<32;
    faa_result = txnMng->faa_remote_content(yield,g_node_id,leaf_offset,faa_num,cor_id);
    // printf("[rdma_opt_no_wait3.cpp:24]txn %ld release X lock on key = %ld,lock = %ld,%ld,%ld,%ld\n", txnMng->get_txn_id(),row->get_primary_key(),txnMng->decode_is_lock(row->_tid_word),txnMng->decode_ix_lock(row->_tid_word),txnMng->decode_s_lock(row->_tid_word),txnMng->decode_x_lock(row->_tid_word));

    // printf("[rdma_opt_no_wait3.cpp:26]txn %ld release IX lock on node = %ld,lock = %ld,%ld,%ld,%ld\n", txnMng->get_txn_id(),leaf_offset,txnMng->decode_is_lock(faa_result),txnMng->decode_ix_lock(faa_result),txnMng->decode_s_lock(faa_result),txnMng->decode_x_lock(faa_result));

}

void RDMA_opt_no_wait3::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();

    row_t *data = access->data;
    // data->lock_info = 0; //write data and unlock

    uint64_t operate_size = 0;
       
    //TODO - ignore write operate
    
    //release data lock(X)
    uint64_t faa_num = -1;
    uint64_t faa_result = txnMng->faa_remote_content(yield,loc,off,faa_num,cor_id);

    // printf("[rdma_opt_no_wait3.cpp:47]txn %ld release IX lock on node = %ld,lock = %ld,%ld,%ld,%ld\n", txnMng->get_txn_id(),off,txnMng->decode_is_lock(faa_result),txnMng->decode_ix_lock(faa_result),txnMng->decode_s_lock(faa_result),txnMng->decode_x_lock(faa_result));

    //release range lock(IX)
    uint64_t leaf_offset = access->leaf_offset;
    faa_num = (-1)<<32;
    faa_result = txnMng->faa_remote_content(yield,loc,leaf_offset,faa_num,cor_id);

    // printf("[rdma_opt_no_wait3.cpp:52]txn %ld release IX lock on node = %ld,lock = %ld,%ld,%ld,%ld\n", txnMng->get_txn_id(),leaf_offset,txnMng->decode_is_lock(faa_result),txnMng->decode_ix_lock(faa_result),txnMng->decode_s_lock(faa_result),txnMng->decode_x_lock(faa_result));
}

void RDMA_opt_no_wait3::unlock_read(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, uint64_t leaf_offset, uint64_t cor_id){
    // unlock data lock(S)
    uint64_t faa_num = (-1)<<16;
    uint64_t faa_result = txnMng->faa_remote_content(yield,g_node_id,(char *)row - rdma_global_buffer,faa_num,cor_id);

    // unlock range lock(IS)
    faa_num = (-1)<<48;
    faa_result = txnMng->faa_remote_content(yield,g_node_id,leaf_offset,faa_num,cor_id);

}

void RDMA_opt_no_wait3::remote_unlock_read(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t num, uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t leaf_offset = access->leaf_offset;

     // unlock data lock(S)
    uint64_t faa_num = (-1)<<16;
    uint64_t faa_result = txnMng->faa_remote_content(yield,loc,off,faa_num,cor_id);

    // unlock range lock(IS)
    faa_num = (-1)<<48;
    faa_result = txnMng->faa_remote_content(yield,loc,leaf_offset,faa_num,cor_id);
}

void RDMA_opt_no_wait3::unlock_range_read(yield_func_t &yield, uint64_t cor_id,TxnManager * txnMng ,uint64_t remote_server, uint64_t range_offset){
    //0x00S0
    uint64_t faa_num = (uint64_t)((-1)<<16);
    // faa_num = faa_num*(-1);
    uint64_t faa_result = txnMng->faa_remote_content(yield,remote_server,range_offset,faa_num,cor_id);
    // if(txnMng->decode_x_lock(faa_result)!=0){
    //     printf("[rdma_opt_no_wait3.cpp:84]faa_result=%ld,IS=%ld,IX=%ld,S=%ld,X=%ld\n",faa_result,txnMng->decode_is_lock(faa_result),txnMng->decode_ix_lock(faa_result),txnMng->decode_s_lock(faa_result),txnMng->decode_x_lock(faa_result));
    // }
#if INDEX_STRUCT == IDX_RDMA_BTREE
    rdma_bt_node * remote_bt_node = txnMng->read_remote_bt_node(yield,remote_server,range_offset,cor_id);
#elif INDEX_STRUCT == IDX_LEARNED
    LeafIndexInfo * remote_bt_node = txnMng->read_remote_learn_node(yield,remote_server,range_offset,cor_id);
#endif
    uint64_t tmp_intent = remote_bt_node->intent_lock;
    // printf("[rdma_opt_no_wait3:95] txn %ld release S on node %ld success, faa_result=%ld,intent=%ld,IS=%ld,IX=%ld,s=%ld,x=%ld\n",txnMng->get_txn_id(),range_offset,faa_result,tmp_intent,txnMng->decode_is_lock(tmp_intent),txnMng->decode_ix_lock(tmp_intent),txnMng->decode_s_lock(tmp_intent),txnMng->decode_x_lock(tmp_intent));
    if(txnMng->decode_x_lock(tmp_intent)!=0){
        // printf("[rdma_opt_no_wait3:90]faa_result=%ld,intent=%ld,IS=%ld,IX=%ld,s=%ld,x=%ld\n",faa_result,tmp_intent,txnMng->decode_is_lock(tmp_intent),txnMng->decode_ix_lock(tmp_intent),txnMng->decode_s_lock(tmp_intent),txnMng->decode_x_lock(tmp_intent));
        // printf("[rdma_opt_no_wait3:91]intent=%ld,IS=%ld,IX=%ld,s=%ld,x=%ld\n",tmp_intent,txnMng->decode_is_lock(tmp_intent),txnMng->decode_ix_lock(tmp_intent),txnMng->decode_s_lock(tmp_intent),txnMng->decode_x_lock(tmp_intent));

    }
}

//write back and unlock
void RDMA_opt_no_wait3::finish(yield_func_t &yield, RC rc, TxnManager * txnMng, uint64_t cor_id){
 
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
        int range_count = txn->locked_range_num;
        for(int i = 0; i < range_count; i++){
            // unlock_range_read(yield,cor_id,txnMng,txnMng->txn->server_set[i],txnMng->txn->range_node_set[i]);
            unlock_range_read(yield,cor_id,txnMng,txnMng->txn->server_set[i],txnMng->txn->range_node_set[i]);
        }
    }else{//common txn
        //for read set element, release lock
        for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
            //local
            if(txn->accesses[read_set[i]]->location == g_node_id){
                Access * access = txn->accesses[ read_set[i] ];
                unlock_read(yield, rc, access->orig_row, txnMng, access->leaf_offset, cor_id);
            }else{
                Access * access = txn->accesses[ read_set[i] ];
                remote_unlock_read(yield,rc , txnMng, read_set[i],cor_id);
            }
        }

        vector<vector<uint64_t>> remote_access(g_node_cnt);

            //for write set element,write back and release lock
        for (uint64_t i = 0; i < txn->write_cnt; i++) {
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
