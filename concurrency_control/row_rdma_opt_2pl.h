
#if CC_ALG == RDMA_OPT_NO_WAIT3

class Row_rdma_opt_2pl{
public:

	void init(row_t * row);
	//RC lock_get(lock_t type, TxnManager * txn, row_t * row);
    RC lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, lock_t* lock_type, uint64_t req_key);

private:

	row_t * _row;

};

#endif