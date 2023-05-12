#include "config.h"

#ifndef _RDMA_RANGE_LOCK_H_
#define _RDMA_RANGE_LOCK_H_

#include "row.h"
#include "semaphore.h"
#include "row_mvcc.h"

class RDMA_range_lock{
public:

   void finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:

    void unlock_read_data(yield_func_t &yield,TxnManager *txnMng,int *read_set,RC rc, uint64_t cor_id);
    void unlock_read_range(yield_func_t &yield,TxnManager *txnMng,uint64_t cor_id);
    void unlock_write_data(yield_func_t &yield,TxnManager *txnMng,RC rc, uint64_t cor_id);
    void unlock_range_read(yield_func_t &yield, uint64_t cor_id,TxnManager * txnMng ,uint64_t remote_server,uint64_t range_offset);
    void write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t leaf_offset, uint64_t cor_id);
    void remote_write_and_unlock(yield_func_t &yield, RC rc, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
    void unlock_read(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, uint64_t leaf_offset, uint64_t cor_id);
    void remote_unlock_read(yield_func_t &yield, RC rc, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
};

#endif