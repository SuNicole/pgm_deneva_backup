#include "index_rdma_btree.h"
#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK|| CC_ALG == RDMA_SINGLE_RANGE_LOCK

class Row_rdma_range_lock{
public:

	void init(row_t * row);
	//RC lock_get(lock_t type, TxnManager * txn, row_t * row);
    RC lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, uint64_t req_key,uint64_t leaf_node_offset);

private:

	row_t * _row;

};

#endif