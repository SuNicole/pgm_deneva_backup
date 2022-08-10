#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_opt_no_wait3.h"
#include "global.h"

#if CC_ALG == RDMA_OPT_NO_WAIT3

void Row_rdma_opt_no_wait3::init(row_t * row){
	_row = row;
}

RC Row_rdma_opt_no_wait3::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, uint64_t req_key,uint64_t leaf_node_offset) {  //本地加锁
    if(type == RD){
        //1.lock range(IS)
        rdma_bt_node *leaf_node = (rdma_bt_node*)(rdma_global_buffer + leaf_node_offset);
        uint64_t intent_lock = leaf_node->intent_lock;

        if(txn->is_lock_content(intent_lock)){
            // printf("[row_rdma_opt_no_wait3.cpp:24]range lock = %ld,%ld,%ld,%ld\n",txn->decode_is_lock(intent_lock),txn->decode_ix_lock(intent_lock),txn->decode_s_lock(intent_lock),txn->decode_x_lock(intent_lock));
			INC_STATS(txn->get_thd_id(), is_content_abort, 1);
            return Abort;
        }
            //acquire is lock
        uint64_t faa_num = 1<<48;
        uint64_t faa_result = 0;
        // txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);

        if(txn->is_lock_content(faa_result)){
			INC_STATS(txn->get_thd_id(), is_content_abort2, 1);
            faa_num = (-1)<<48;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            return Abort;
        }

        //2.lock data(S)
        uint64_t row_offset = (char *)row - rdma_global_buffer;
        intent_lock = row->_tid_word;
        if(txn->s_lock_content(intent_lock)){
			INC_STATS(txn->get_thd_id(), s_content_abort, 1);
            faa_num = (-1)<<48;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            return Abort;
        }
        faa_num = 1<<16;
        // faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);
        if(txn->s_lock_content(faa_result)){
			INC_STATS(txn->get_thd_id(), s_content_abort, 1);
            faa_num = (-1)<<48;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            faa_num = (-1)<<16;
            // faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);
            return Abort;
        }

        return RCOK;

    }else if(type == WR){
        // assert(false);
        //lock range(IX)
        rdma_bt_node *leaf_node = (rdma_bt_node*)(rdma_global_buffer + leaf_node_offset);
        uint64_t intent_lock = leaf_node->intent_lock;

        if(txn->ix_lock_content(intent_lock)){
			INC_STATS(txn->get_thd_id(), ix_content_abort, 1);
            return Abort;
        }

        uint64_t faa_num = 1<<32;
        uint64_t faa_result = 0;
        // txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);

        if(txn->ix_lock_content(faa_result)){
			INC_STATS(txn->get_thd_id(), ix_content_abort, 1);
            faa_num = (-1)<<32;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            return Abort;
        }
        //lock data(X)
        uint64_t row_offset = (char *)row - rdma_global_buffer;
        intent_lock = row->_tid_word;
        if(txn->x_lock_content(intent_lock)){
			INC_STATS(txn->get_thd_id(), x_content_abort, 1);
            faa_num = (-1)<<32;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            return Abort;
        }
        faa_num = 1;
        // faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);
        if(txn->x_lock_content(faa_result)){
			INC_STATS(txn->get_thd_id(), x_content_abort, 1);
            faa_num = (-1)<<32;
            // faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            faa_num = -1;
            // faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);
            return Abort;
        }
        // printf("[row_rdma_opt_no_wait3.cpp:98]lock on key = %ld,lock = %ld,%ld,%ld,%ld\n",row->get_primary_key(),txn->decode_is_lock(row->_tid_word),txn->decode_ix_lock(row->_tid_word),txn->decode_s_lock(row->_tid_word),txn->decode_x_lock(row->_tid_word));
        return RCOK;
    }

}



#endif