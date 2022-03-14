#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_bamboo.h"
#include "row_rdma_bamboo.h"

#if CC_ALG == RDMA_BAMBOO_NO_WAIT
void RDMA_bamboo::dependcy(row_t * row, TxnManager * txnMng){
    // 记录依赖
    for (int i = 0; i < LOCK_LENGTH; i++){
        if (row->lock_retire[i] == 0) break;
        for (int j = 0; j < 100; j++) {
            if (txnMng->dependcy[j] == row->lock_retire[i]) break;
            if (txnMng->dependcy[j] == 0) {
                txnMng->dependcy[j] = row->lock_retire[i]; break;
            }
        }
    }
}
bool RDMA_bamboo::lock_retire(row_t *row, TxnManager * txnMng) {
    bool retire = false;
    for (int i = 0; i < LOCK_LENGTH; i++){
        if (row->lock_retire[i] == 0) {
            row->lock_retire[i] = txnMng->get_thd_id()+1;
            retire = true;
            break;
        }
    }
    return retire;
}

void RDMA_bamboo::lock_leave_retire(row_t *row, TxnManager * txnMng){
    int i = 0;
    for (; i < LOCK_LENGTH; i++){
        if (row->lock_retire[i] == txnMng->get_thd_id()+1) {
            break;
        }
    }
    for (; i < LOCK_LENGTH; i++) {
        if (i == LOCK_LENGTH - 1) row->lock_retire[i] = 0;
        row->lock_retire[i] = row->lock_retire[i + 1];
    }
}

bool RDMA_bamboo::write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
    // dependcy(test_row, txnMng);
    // bool retire = lock_retire(row);
    // if (!retire) return false;
    // row->_tid_word = 0;
        
    
#if DEBUG_PRINTF
    printf("---thd：%lu, local lock succ,lock location: %u; %p, txn: %lu, old lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info);
#endif
}

bool RDMA_bamboo::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    
}

void RDMA_bamboo::unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id){

}

void RDMA_bamboo::remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id){

}


//write back and unlock
void RDMA_bamboo::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
        if(rc == Abort){//undo modify and change status to abort
            for(int i = 0;i < txnMng->txn->write_cnt;i++){
                Access *access = txnMng->txn->accesses[i];
                uint64_t offset = access->offset;
                uint64_t loc = access->location;
                uint64_t try_lock = -1;
                while(try_lock != 0 && !simulation->is_done()){
                    try_lock = txnMng->cas_remote_content(yield,loc,offset,0,3,cor_id);
                }
            
                row_t * current_row = NULL;
                if(loc == g_node_id){//local
                    current_row = txnMng->txn->accesses[i]->orig_row;
                }else{//remote case 
                    current_row = txnMng->read_remote_row(yield,loc,offset,cor_id);
                }

                uint64_t txn_in_retire = -1;
                for(int i = 0; i < RETIRE_NUM;i++){
                    if(current_row->lock_retire[i] == txnMng->get_txn_id()){
                        txn_in_retire = i;
                        break;
                    }
                }
                if(txn_in_retire == -1)continue;
                else{
                    // current_row->data = old_value;
                    for(int i = txn_in_retire;i<RETIRE_NUM;i++){
                        current_row->lock_retire[i] = 0;
                    }
                }

                try_lock = -1;
                while(try_lock != 0 && !simulation->is_done()){
                    try_lock = txnMng->cas_remote_content(yield,loc,offset,3,0,cor_id);
                }
            }
            // txn_status[txnMng->get_txn_id()] = 2;
        }else if(rc == Commit){
            //check dependency 
            // txn_status[txnMng->get_txn_id()] = 1;
        }
}
#endif
