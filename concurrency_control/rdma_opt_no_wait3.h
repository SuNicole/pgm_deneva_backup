#include "config.h"

#ifndef _RDMA_OPT_NO_WAIT3_H_
#define _RDMA_OPT_NO_WAIT3_H_

#include "row.h"
#include "semaphore.h"
#include "row_mvcc.h"

class RDMA_opt_no_wait3{
public:

   void finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:
    // void write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng); 
	// void remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num);
	// void unlock(row_t * row, TxnManager * txnMng); 
	// void remote_unlock(TxnManager * txnMng , uint64_t num);
    void unlock_range_read(yield_func_t &yield, uint64_t cor_id,TxnManager * txnMng ,uint64_t remote_server,uint64_t range_offset);
    void write_and_unlock(yield_func_t &yield, RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t leaf_offset, uint64_t cor_id);
    void remote_write_and_unlock(yield_func_t &yield, RC rc, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
    void unlock_read(yield_func_t &yield, RC rc, row_t * row , TxnManager * txnMng, uint64_t leaf_offset, uint64_t cor_id);
    void remote_unlock_read(yield_func_t &yield, RC rc, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
};

#endif