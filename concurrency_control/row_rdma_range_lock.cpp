#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_range_lock.h"
#include "global.h"

#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK

void Row_rdma_range_lock::init(row_t * row){
	_row = row;
}

RC Row_rdma_range_lock::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, uint64_t req_key,uint64_t leaf_node_offset) {  //本地加锁
    if(type == RD){
#if INDEX_STRUCT == IDX_RDMA_BTREE
        rdma_bt_node *leaf_node = (rdma_bt_node*)(rdma_global_buffer + leaf_node_offset);
#endif

#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK 
        uint64_t intent_lock;
        uint64_t faa_num;

        //1.lock data(S)
        uint64_t row_offset = (char *)row - rdma_global_buffer;
        intent_lock = row->_tid_word;

        if(intent_lock == 1){
            uint64_t s = txn->decode_s_lock(intent_lock);
            uint64_t x = txn->decode_x_lock(intent_lock);
            // printf("[row_rdma_range_lock.cpp:35]key=%ld,s=%ld,x=%ld\n",row->get_primary_key(),s,x);
            return Abort;
        }

        uint64_t olds = txn->decode_s_lock(row->_tid_word);
        uint64_t oldx = txn->decode_x_lock(row->_tid_word);
        uint64_t oldlock = row->_tid_word;

        faa_num = ((uint64_t)1<<1);
        uint64_t faa_result = -1;
        faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);

        uint64_t s = txn->decode_s_lock(row->_tid_word);
        uint64_t x = txn->decode_x_lock(row->_tid_word);

        // if(((oldx == 1) || (x == 1))&&row->get_primary_key() > 524288)
        // printf("key=%ld,faanum=%ld,oldlock=%ld,lock=%ld,old s=%ld,old x=%ld,s=%ld,x=%ld\n",row->get_primary_key(),faa_num,oldlock,row->_tid_word,olds,oldx,s,x);

        if(faa_result == 1){
            uint64_t s = txn->decode_s_lock(faa_result);
            uint64_t x = txn->decode_x_lock(faa_result);
            // printf("[row_rdma_range_lock.cpp:44]s=%ld,x=%ld\n",s,x);
            faa_num = ((-1)<<1);
            faa_result = txn->faa_remote_content(yield,g_node_id,row_offset,faa_num,cor_id);
            return Abort;
        }
#elif CC_ALG == RDMA_SINGLE_RANGE_LOCK
        uint64_t intent_lock;
        uint64_t faa_num;

        //1.lock range(S)
        intent_lock = leaf_node->intent_lock;

        if(intent_lock == 1){
            uint64_t s = txn->decode_s_lock(intent_lock);
            uint64_t x = txn->decode_x_lock(intent_lock);
            // printf("[row_rdma_range_lock.cpp:35]key=%ld,s=%ld,x=%ld\n",row->get_primary_key(),s,x);
            return Abort;
        }

        uint64_t olds = txn->decode_s_lock(row->_tid_word);
        uint64_t oldx = txn->decode_x_lock(row->_tid_word);
        uint64_t oldlock = row->_tid_word;

        faa_num = ((uint64_t)1<<1);
        uint64_t faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);

        uint64_t s = txn->decode_s_lock(row->_tid_word);
        uint64_t x = txn->decode_x_lock(row->_tid_word);

        // if(((oldx == 1) || (x == 1))&&row->get_primary_key() > 524288)
        // printf("key=%ld,faanum=%ld,oldlock=%ld,lock=%ld,old s=%ld,old x=%ld,s=%ld,x=%ld\n",row->get_primary_key(),faa_num,oldlock,row->_tid_word,olds,oldx,s,x);

        if(faa_result == 1){
            uint64_t s = txn->decode_s_lock(faa_result);
            uint64_t x = txn->decode_x_lock(faa_result);
            // printf("[row_rdma_range_lock.cpp:44]s=%ld,x=%ld\n",s,x);
            faa_num = ((-1)<<1);
            faa_result = txn->faa_remote_content(yield,g_node_id,leaf_node_offset,faa_num,cor_id);
            return Abort;
        }
#endif
        return RCOK;

    }else if(type == WR){
        // printf("[row_rdma_range_lock.cpp:60]write txn\n");
        // assert(false);
        //lock range(IX)
#if INDEX_STRUCT == IDX_RDMA_BTREE
        rdma_bt_node *leaf_node = (rdma_bt_node*)(rdma_global_buffer + leaf_node_offset);
#endif

#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK
        uint64_t faa_num = 1;

        //lock data(X)
        uint64_t row_offset = (char *)row - rdma_global_buffer;
        uint64_t intent_lock = row->_tid_word;
        if(txn->x_lock_content(intent_lock)){
            return Abort;
        }

        faa_num = 1;
        uint64_t cas_result = -1;
        cas_result = txn->cas_remote_content(yield,g_node_id,row_offset,0,1,cor_id);
        if(cas_result != 0){
            return Abort;
        }
#elif CC_ALG == RDMA_SINGLE_RANGE_LOCK
        uint64_t faa_num = 1;

        //lock range(X)
        uint64_t intent_lock = leaf_node->intent_lock;
        if(intent_lock != 0){
            return Abort;
        }

        uint64_t cas_result = -1;
        cas_result = txn->cas_remote_content(yield,g_node_id,leaf_node_offset,0,1,cor_id);
        if(cas_result != 0){
            return Abort;
        }
#endif
        return RCOK;
    }

}



#endif