#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_opt_2pl.h"
#include "global.h"

#if CC_ALG == RDMA_OPT_NO_WAIT

void Row_rdma_opt_2pl::init(row_t * row){
	_row = row;
}

RC Row_rdma_opt_2pl::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, lock_t* lock_type) {  //本地加锁
    RC rc = RCOK;
    uint64_t loc = g_node_id;
    uint64_t lock_info_pointer = row_t::get_lock_info_pointer(_row);
    uint64_t conflict_pointer = (char*)_row - rdma_global_buffer;
    uint64_t new_lock_info = txn->get_txn_id() + 1;
    *lock_type = LOCK_NONE;
    if (type == RD || type == SCAN) {
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,new_lock_info,cor_id);
        if (try_lock != 0 ||_row->lock_info != new_lock_info) {
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            // printf("txn %ld lock failed try_lock = %ld now it is %ld\n", new_lock_info, try_lock, _row->lock_info);
            return Abort;
        }
        if (_row->is_hot) {
            _row->read_cnt ++;
            _row->lock_info = 0;
            *lock_type = DLOCK_SH;
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
        } else if(_row->read_cnt > 0) {
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            _row->lock_info = 0;
            return Abort;
        } else {
            *lock_type = DLOCK_EX;
        }
    } else if (type == WR) {
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,new_lock_info,cor_id);
        if (try_lock != 0 ||_row->lock_info != new_lock_info) {
            // txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            // printf("txn %ld lock failed try_lock = %ld now it is %ld\n", new_lock_info, try_lock, _row->lock_info);
            return Abort;
        }
        if(_row->read_cnt > 0) {
            // txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            _row->lock_info = 0;
            return Abort;
        }
        *lock_type = DLOCK_EX;
    }

	return rc;
}
#endif