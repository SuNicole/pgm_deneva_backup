/*
	 Copyright 2016 Massachusetts Institute of Technology

	 Licensed under the Apache License, Version 2.0 (the "License");
	 you may not use this file except in compliance with the License.
	 You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

	 Unless required by applicable law or agreed to in writing, software
	 distributed under the License is distributed on an "AS IS" BASIS,
	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 See the License for the specific language governing permissions and
	 limitations under the License.
*/

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "focc.h"
#include "bocc.h"
#include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "dli.h"
#include "dta.h"
#include "index_btree.h"
#include "index_hash.h"
#include "index_rdma.h"
#include "index_rdma_btree.h"
#include "index_learned.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "message.h"
#include "msg_queue.h"
#include "occ.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "array.h"
#include "maat.h"
#include "wkdb.h"
#include "tictoc.h"
#include "ssi.h"
#include "wsi.h"
#include "manager.h"
#include "row_rdma_2pl.h"
#include "row_rdma_opt_2pl.h"
#include "row_rdma_bamboo.h"
#include "rdma_silo.h"
#include "rdma_mocc.h"
#include "rdma_mvcc.h"
#include "rdma_2pl.h"
#include "rdma_dslr_no_wait.h"
#include "rdma_opt_no_wait3.h"
#include "rdma_range_lock.h"
#include "rdma_bamboo.h"
#include "rdma_opt_2pl.h"
#include "rdma_maat.h"
#include "rdma_ts1.h"
#include "rdma_ts.h"
#include "rdma_cicada.h"
#include "cicada.h"
#include "row_cicada.h"
#include "rdma_null.h"
#include "transport.h"
#include "dbpa.hpp"
#include "routine.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "src/rdma/sop.hh"
#include "src/sshed.hh"
#include "global.h"

void TxnStats::init() {
	starttime=0;
	wait_starttime=get_sys_clock();
	total_process_time=0;
	process_time=0;
	total_local_wait_time=0;
	local_wait_time=0;
	total_remote_wait_time=0;
	remote_wait_time=0;
	total_twopc_time=0;
	twopc_time=0;
	write_cnt = 0;
	abort_cnt = 0;

	 total_work_queue_time = 0;
	 work_queue_time = 0;
	 total_cc_block_time = 0;
	 cc_block_time = 0;
	 total_cc_time = 0;
	 cc_time = 0;
	 total_work_queue_cnt = 0;
	 work_queue_cnt = 0;
	 total_msg_queue_time = 0;
	 msg_queue_time = 0;
	 total_abort_time = 0;

	 clear_short();
}

void TxnStats::clear_short() {

	 work_queue_time_short = 0;
	 cc_block_time_short = 0;
	 cc_time_short = 0;
	 msg_queue_time_short = 0;
	 process_time_short = 0;
	 network_time_short = 0;
}

void TxnStats::reset() {
	wait_starttime=get_sys_clock();
	total_process_time += process_time;
	process_time = 0;
	total_local_wait_time += local_wait_time;
	local_wait_time = 0;
	total_remote_wait_time += remote_wait_time;
	remote_wait_time = 0;
	total_twopc_time += twopc_time;
	twopc_time = 0;
	write_cnt = 0;

	total_work_queue_time += work_queue_time;
	work_queue_time = 0;
	total_cc_block_time += cc_block_time;
	cc_block_time = 0;
	total_cc_time += cc_time;
	cc_time = 0;
	total_work_queue_cnt += work_queue_cnt;
	work_queue_cnt = 0;
	total_msg_queue_time += msg_queue_time;
	msg_queue_time = 0;

	clear_short();

}

void TxnStats::abort_stats(uint64_t thd_id) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

	INC_STATS(thd_id,lat_s_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_s_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_s_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_s_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_s_rem_process_time,total_process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id,
														uint64_t timespan_long, uint64_t timespan_short) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN

	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);
	// latency from start of transaction at this node
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f\n", txn_id, batch_id, total_work_queue_cnt,
								(double)timespan_long / BILLION, (double)total_work_queue_time / BILLION,
								(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
								(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
#else
	// latency from start of transaction
	if (IS_LOCAL(txn_id)) {
	INC_STATS(thd_id,lat_l_loc_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
	INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);

	INC_STATS(thd_id,lat_short_work_queue_time,work_queue_time_short);
	INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
	INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
	INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
	INC_STATS(thd_id,lat_short_process_time,process_time_short);
	INC_STATS(thd_id,lat_short_network_time,network_time_short);
	} else {
	INC_STATS(thd_id,lat_l_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_rem_process_time,total_process_time);
	}
	if (IS_LOCAL(txn_id)) {
		PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)work_queue_time / BILLION,
									(double)msg_queue_time / BILLION, (double)cc_block_time / BILLION,
									(double)cc_time / BILLION, (double)process_time / BILLION);
	 /*
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f %f\n"
			, txn_id
			, total_work_queue_cnt
			, abort_cnt
			, (double) timespan_long / BILLION
			, (double) total_work_queue_time / BILLION
			, (double) total_msg_queue_time / BILLION
			, (double) total_cc_block_time / BILLION
			, (double) total_cc_time / BILLION
			, (double) total_process_time / BILLION
			, (double) total_abort_time / BILLION
			);
			*/
	} else {
		PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)total_work_queue_time / BILLION,
									(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
									(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
	}
	/*
	if (!IS_LOCAL(txn_id) || timespan_short < timespan_long) {
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
			, txn_id
			, work_queue_cnt
			, (double) timespan_short / BILLION
			, (double) work_queue_time / BILLION
			, (double) msg_queue_time / BILLION
			, (double) cc_block_time / BILLION
			, (double) cc_time / BILLION
			, (double) process_time / BILLION
			);
	}
	*/
#endif

	if (!IS_LOCAL(txn_id)) {
		return;
	}

	INC_STATS(thd_id,txn_total_process_time,total_process_time);
	INC_STATS(thd_id,txn_process_time,process_time);
	INC_STATS(thd_id,txn_total_local_wait_time,total_local_wait_time);
	INC_STATS(thd_id,txn_local_wait_time,local_wait_time);
	INC_STATS(thd_id,txn_total_remote_wait_time,total_remote_wait_time);
	INC_STATS(thd_id,txn_remote_wait_time,remote_wait_time);
	INC_STATS(thd_id,txn_total_twopc_time,total_twopc_time);
	INC_STATS(thd_id,txn_twopc_time,twopc_time);
	if(write_cnt > 0) {
	INC_STATS(thd_id,txn_write_cnt,1);
	}
	if(abort_cnt > 0) {
	INC_STATS(thd_id,unique_txn_abort_cnt,1);
	}

}


void Transaction::init() {
	timestamp = UINT64_MAX;
	start_timestamp = UINT64_MAX;
	end_timestamp = UINT64_MAX;
	txn_id = UINT64_MAX;
	batch_id = UINT64_MAX;
	DEBUG_M("Transaction::init array insert_rows\n");
	insert_rows.init(g_max_items_per_txn + 10);
#if CC_ALG == RDMA_BAMBOO_NO_WAIT
    dependency_txn.init(100);
#endif
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    locked_range_num = 0;
#endif
	DEBUG_M("Transaction::reset array accesses\n");
	accesses.init(MAX_ROW_PER_TXN);
    locked_node.init(MAX_ROW_PER_TXN);
	reset(0);
}

void Transaction::reset(uint64_t thd_id) {
	release_accesses(thd_id);
	release_locked_node(thd_id);
	accesses.clear();
    locked_node.clear();
	release_inserts(thd_id);
	insert_rows.clear();
	write_cnt = 0;
	row_cnt = 0;
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    locked_range_num = 0;
#endif
	twopc_state = START;
	rc = RCOK;
}

void Transaction::release_accesses(uint64_t thd_id) {
	for(uint64_t i = 0; i < accesses.size(); i++) {
	access_pool.put(thd_id,accesses[i]);
	}
}

void Transaction::release_locked_node(uint64_t thd_id) {
    for(uint64_t i = 0; i < locked_node.size(); i++) {
	    locked_node_pool.put(thd_id,locked_node[i]);
	    // access_pool.put(thd_id,locked_node[i]);
	}
}



void Transaction::release_inserts(uint64_t thd_id) {
	for(uint64_t i = 0; i < insert_rows.size(); i++) {
	row_t * row = insert_rows[i];
#if CC_ALG != MAAT && CC_ALG != OCC && CC_ALG != WOOKONG && \
		CC_ALG != TICTOC && CC_ALG != BOCC && CC_ALG != FOCC && CC_ALG != DTA && CC_ALG != DLI_MVCC_OCC && \
		CC_ALG != DLI_MVCC_BASE && CC_ALG != DLI_DTA && CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3 && \
		CC_ALG != DLI_BASE && CC_ALG != DLI_OCC && CC_ALG != RDMA_MVCC  && CC_ALG != RDMA_MAAT  && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_CNULL
		DEBUG_M("TxnManager::cleanup row->manager free\n");
		mem_allocator.free(row->manager, 0);
#endif

		row->free_row();
#if RDMA_ONE_SIDE == true
		// r2::AllocatorMaster<>::get_thread_allocator()->free(row);
        uint64_t size = row_t::get_row_size(row->get_schema()->get_tuple_size());
        mem_allocator.free(row,row_t::get_row_size(size));
#else
		DEBUG_M("Transaction::release insert_rows free\n")
        // mem_allocator.free(row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		row_pool.put(thd_id,row);
#endif
	}
}

void Transaction::release(uint64_t thd_id) {
	DEBUG("Transaction release\n");
	release_accesses(thd_id);
    release_locked_node(thd_id);
	DEBUG_M("Transaction::release array accesses free\n")
	accesses.release();
    locked_node.release();
	release_inserts(thd_id);
	DEBUG_M("Transaction::release array insert_rows free\n")
	insert_rows.release();
}

void TxnManager::init(uint64_t thd_id, Workload * h_wl) {
	uint64_t prof_starttime = get_sys_clock();
	if(!txn)  {
	DEBUG_M("Transaction alloc\n");
	txn_pool.get(thd_id,txn);

	}
	INC_STATS(get_thd_id(),mtx[15],get_sys_clock()-prof_starttime);
	prof_starttime = get_sys_clock();
	//txn->init();
	if(!query) {
	DEBUG_M("TxnManager::init Query alloc\n");
	qry_pool.get(thd_id,query);
	}
	INC_STATS(get_thd_id(),mtx[16],get_sys_clock()-prof_starttime);
	//query->init();
	//reset();
	sem_init(&rsp_mutex, 0, 1);
	return_id = UINT64_MAX;

	this->h_wl = h_wl;
#if CC_ALG == MAAT
	uncommitted_writes = new std::set<uint64_t>();
	uncommitted_writes_y = new std::set<uint64_t>();
	uncommitted_reads = new std::set<uint64_t>();
#endif
#if CC_ALG == RDMA_MAAT
	// uncommitted_writes = new std::set<uint64_t>();
	// uncommitted_writes_y = new std::set<uint64_t>();
	// uncommitted_reads = new std::set<uint64_t>();
	memset(write_set, 0, 100);
#endif
#if CC_ALG == RDMA_CICADA
	start_ts = get_sys_clock();
	memset(write_set, 0, 100);
	// uncommit_set = new std::set<uint64_t>();
#endif
#if CC_ALG == TICTOC
	_is_sub_txn = true;
	_min_commit_ts = glob_manager.get_max_cts();;
	_num_lock_waits = 0;
	_signal_abort = false;
	_timestamp = glob_manager.get_ts(get_thd_id());
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);
#endif
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
#if CC_ALG == SILO || CC_ALG == RDMA_SILO || CC_ALG == RDMA_MOCC
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else
		assert(false);
  _cur_tid = 0;
  num_locks = 0;
  memset(write_set, 0, 100);
  // write_set = (int *) mem_allocator.alloc(sizeof(int) * 100);
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
	num_atomic_retry = 0;
#endif
#if CC_ALG == WOUND_WAIT
	txn_state = RUNNING;
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif

#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    remote_operate_num = 0;
#endif
	registed_ = false;
	txn_ready = true;
	twopl_wait_start = 0;
    finished_server_count = 0;

	txn_stats.init();
}

// reset after abort
void TxnManager::reset() {
	lock_ready = false;
	lock_ready_cnt = 0;
	locking_done = true;
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
	ready_part = 0;
	rsp_cnt = 0;
	aborted = false;
	return_id = UINT64_MAX;
	twopl_wait_start = 0;

	//ready = true;

	// MaaT & DTA & WKDB
	greatest_write_timestamp = 0;
	greatest_read_timestamp = 0;
	commit_timestamp = 0;
#if CC_ALG == MAAT
	uncommitted_writes->clear();
	uncommitted_writes_y->clear();
	uncommitted_reads->clear();
#endif
#if CC_ALG == RDMA_MAAT
	uncommitted_writes.clear();
	uncommitted_writes_y.clear();
	uncommitted_reads.clear();
	unread_set.clear();
	unwrite_set.clear();
#endif
#if CC_ALG == RDMA_CICADA
	uncommitted_set.clear();
	start_ts = 0;
	version_num.clear();
#endif
#if CC_ALG == RDMA_MOCC
	lock_set.clear();
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.clear();
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    remote_operate_num = 0;
#endif
	assert(txn);
	assert(query);
	txn->reset(get_thd_id());

	// Stats
	txn_stats.reset();
}

void TxnManager::release() {
	uint64_t prof_starttime = get_sys_clock();
	qry_pool.put(get_thd_id(),query);
	INC_STATS(get_thd_id(),mtx[0],get_sys_clock()-prof_starttime);
	query = NULL;
	prof_starttime = get_sys_clock();
	txn_pool.put(get_thd_id(),txn);
	INC_STATS(get_thd_id(),mtx[1],get_sys_clock()-prof_starttime);
	txn = NULL;

#if CC_ALG == MAAT 
	delete uncommitted_writes;
	delete uncommitted_writes_y;
	delete uncommitted_reads;
#endif
#if CC_ALG == MAAT
	uncommitted_writes->clear();
	uncommitted_writes_y->clear();
	uncommitted_reads->clear();
#endif
#if CC_ALG == RDMA_MAAT
	uncommitted_writes.clear();
	uncommitted_writes_y.clear();
	uncommitted_reads.clear();
	unread_set.clear();
	unwrite_set.clear();
#endif
#if CC_ALG == RDMA_MOCC
	lock_set.clear();
#endif
#if CC_ALG == RDMA_CICADA
	uncommitted_set.clear();
	start_ts = 0;
	version_num.clear();
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	calvin_locked_rows.release();
#endif
#if CC_ALG == SILO
  num_locks = 0;
  memset(write_set, 0, 100);
  // mem_allocator.free(write_set, sizeof(int) * 100);
#endif
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    remote_operate_num = 0;
#endif 
	txn_ready = true;
}

void TxnManager::reset_query() {
#if WORKLOAD == YCSB
	((YCSBQuery*)query)->reset();
#elif WORKLOAD == TPCC
	((TPCCQuery*)query)->reset();
#elif WORKLOAD == PPS
	((PPSQuery*)query)->reset();
#endif
}

RC TxnManager::commit_continuous(yield_func_t &yield, uint64_t cor_id) {
    uint64_t locked_node_count = txn->locked_node.get_count();
    for(int i = 0;i < locked_node_count;i++){
        if(txn->locked_node[i]->server_id == g_node_id){
            bt_node * leaf_node =(bt_node*)(txn->locked_node[i]->bt_node_location);
            uint64_t faa_num = (-1)<<16;
			printf("commit_continuous\n");
            uint64_t faa_result = ATOM_FETCH_ADD(leaf_node->intent_lock,faa_num);
        }
    }
    return Commit;
}

RC TxnManager::abort_continuous(yield_func_t &yield, uint64_t cor_id) {
    uint64_t locked_node_count = txn->locked_node.get_count();
    for(int i = 0;i < locked_node_count;i++){
        if(txn->locked_node[i]->server_id == g_node_id){
            bt_node * leaf_node =(bt_node*)(txn->locked_node[i]->bt_node_location);
            uint64_t faa_num = (-1)<<16;
			printf("abort_continuous\n");
            uint64_t faa_result = ATOM_FETCH_ADD(leaf_node->intent_lock,faa_num);
        }
    }
    return Abort;
}

RC TxnManager::commit(yield_func_t &yield, uint64_t cor_id) {
	DEBUG("Commit %ld\n",get_txn_id());
#if CC_ALG == WOUND_WAIT
    txn_state = STARTCOMMIT;    
#endif
	release_locks(yield, RCOK, cor_id);
#if CC_ALG == MAAT
	time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == TICTOC
	tictoc_man.cleanup(RCOK, this);
#endif
#if CC_ALG == SSI
	inout_table.set_commit_ts(get_thd_id(), get_txn_id(), get_commit_timestamp());
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_COMMITTED);
#endif
	commit_stats();
#if LOGGING
	LogRecord * record = logger.createRecord(get_txn_id(),L_NOTIFY,0,0);
	if(g_repl_cnt > 0) {
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), g_node_id + g_node_cnt + g_client_node_cnt, Message::create_message(record, LOG_MSG));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
											g_node_id + g_node_cnt + g_client_node_cnt);
#endif
	}
	logger.enqueueRecord(record);
	return WAIT;
#endif
	return Commit;
}

RC TxnManager::abort(yield_func_t &yield, uint64_t cor_id) {
	if (aborted) {printf("[txn.cpp:644]\n");;return Abort;}
#if CC_ALG == SSI
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_ABORTED);
	inout_table.clear_Conflict(get_thd_id(), get_txn_id());
#endif
	DEBUG("Abort %ld\n",get_txn_id());
	//printf("Abort %ld\n",get_txn_id());
	txn->rc = Abort;
	INC_STATS(get_thd_id(),total_txn_abort_cnt,1);
	txn_stats.abort_cnt++;
	if(IS_LOCAL(get_txn_id())) {
	    INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
	} else {
        INC_STATS(get_thd_id(), remote_txn_abort_cnt, 1);
        txn_stats.abort_stats(get_thd_id());
	}
	aborted = true;
	//RDMA_SILO - ADD remote release lock by rdma
	release_locks(yield, Abort, cor_id);
#if CC_ALG == MAAT
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	dta_time_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == RDMA_SILO
    //TODO
	//rdam_rlease(get_thd_id(), get_txn_id());
#endif

	uint64_t timespan = get_sys_clock() - txn_stats.restart_starttime;
	if (IS_LOCAL(get_txn_id()) && warmup_done) {
		INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan);
	}

	/*
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld 0 %f %f %f %f %f %f 0.0\n"
			, get_txn_id()
			, txn_stats.work_queue_cnt
			, (double) timespan / BILLION
			, (double) txn_stats.work_queue_time / BILLION
			, (double) txn_stats.msg_queue_time / BILLION
			, (double) txn_stats.cc_block_time / BILLION
			, (double) txn_stats.cc_time / BILLION
			, (double) txn_stats.process_time / BILLION
			);
			*/
	//commit_stats();
	return Abort;
}

RC TxnManager::start_abort(yield_func_t &yield, uint64_t cor_id) {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
    INC_STATS(get_thd_id(), trans_process_count, 1);
	txn->rc = Abort;
	DEBUG("%ld start_abort\n",get_txn_id());

	uint64_t finish_start_time = get_sys_clock();
	txn_stats.finish_start_time = finish_start_time;
	uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
	INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    INC_STATS(get_thd_id(), trans_prepare_count, 1);
	//RDMA_SILO:keep message or not
	if(query->partitions_touched.size() > 1 && !rdma_one_side()) {
		send_finish_messages();
		abort(yield, cor_id);
		return Abort;
	}
	return abort(yield, cor_id);
}

RC TxnManager::start_abort_continuous(yield_func_t &yield, uint64_t cor_id) {
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
    INC_STATS(get_thd_id(), trans_process_count, 1);
	txn->rc = Abort;
    finished_server_count = 0;
	DEBUG("%ld start_abort\n",get_txn_id());

	uint64_t finish_start_time = get_sys_clock();
	txn_stats.finish_start_time = finish_start_time;
	uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
	INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    INC_STATS(get_thd_id(), trans_prepare_count, 1);

    rsp_cnt = 0;
	assert(IS_LOCAL(get_txn_id()));
	for(uint64_t i = 0; i < g_node_cnt; i++) {
		if(i == g_node_id)continue;
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), i, Message::create_message(this, CRFIN));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, CRFIN),i);
#endif
	}
    RC rc = abort_continuous(yield, cor_id);//local abort continuous txn本地回滚
    finished_server_count++;
	return rc;
}

#ifdef NO_2PC
RC TxnManager::start_commit() {
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
	_is_sub_txn = false;

	rc = validate();
	if(CC_ALG == SSI) {
		ssi_man.gene_finish_ts(this);
	}
	if(CC_ALG == WSI) {
		wsi_man.gene_finish_ts(this);
	}
	if(rc == RCOK)
		rc = commit();
	else
		start_abort();

		return rc;
}
#else
RC TxnManager::start_commit(yield_func_t &yield, uint64_t cor_id) {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	INC_STATS(get_thd_id(), trans_process_count, 1);
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
        // printf("read local WOUNDState:%ld\n", rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id));
		if(rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id) == WOUND_RUNNING) {
			rdma_txn_table.local_set_state(this,get_thd_id(),txn->txn_id, WOUND_COMMITTING);
		}
#endif
	if(is_multi_part() && !rdma_one_side()) {
		if(CC_ALG == TICTOC) {
			rc = validate(yield, cor_id);
			if (rc != Abort) {
				send_prepare_messages();
				rc = WAIT_REM;
			}
		} else if (!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT || CC_ALG == DLI_BASE ||
				CC_ALG == DLI_OCC || CC_ALG == SILO || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == CICADA) {
			// send prepare messages
			send_prepare_messages();
			rc = WAIT_REM;
		} else {
			uint64_t finish_start_time = get_sys_clock();
			txn_stats.finish_start_time = finish_start_time;
			uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
			INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      		INC_STATS(get_thd_id(), trans_prepare_count, 1);
			if(CC_ALG == WSI) {
				wsi_man.gene_finish_ts(this);
			}
			send_finish_messages();
			rsp_cnt = 0;
			rc = commit(yield, cor_id);
		}
	} 
	else { // is not multi-part or use rdma
        rc = validate(yield, cor_id);
		// rc = RCOK;
		uint64_t finish_start_time = get_sys_clock();
		txn_stats.finish_start_time = finish_start_time;

		uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
		INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    	INC_STATS(get_thd_id(), trans_prepare_count, 1);
		if(CC_ALG == SSI) {
			ssi_man.gene_finish_ts(this);
		}
		if(CC_ALG == WSI) {
			wsi_man.gene_finish_ts(this);
		}
		if(rc == RCOK){   //for NO_WAIT , rc == RCOK
			rc = commit(yield, cor_id);
		}		
		else {
			txn->rc = Abort;
			DEBUG("%ld start_abort\n",get_txn_id());
			if(query->partitions_touched.size() > 1 && CC_ALG != RDMA_SILO &&  CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2 && CC_ALG != RDMA_MAAT && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WOUND_WAIT && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_MOCC && CC_ALG != RDMA_OPT_NO_WAIT && CC_ALG != RDMA_OPT_WAIT_DIE && CC_ALG != RDMA_BAMBOO_NO_WAIT && CC_ALG != RDMA_OPT_NO_WAIT2 && CC_ALG != RDMA_OPT_NO_WAIT3 && CC_ALG != RDMA_DOUBLE_RANGE_LOCK && CC_ALG != RDMA_SINGLE_RANGE_LOCK) {
				send_finish_messages();
				abort(yield, cor_id);
				rc = Abort;
			}
			rc = abort(yield, cor_id);
		}
	}
	return rc;
}
#endif

RC TxnManager::start_commit_continuous(yield_func_t &yield, uint64_t cor_id) {
    uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	INC_STATS(get_thd_id(), trans_process_count, 1);
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());

    if(is_multi_part() && !rdma_one_side()) {
        uint64_t finish_start_time = get_sys_clock();
        txn_stats.finish_start_time = finish_start_time;
        uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
        INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
        INC_STATS(get_thd_id(), trans_prepare_count, 1);

        for(uint64_t i = 0; i < g_node_cnt; i++) {
            if(i == g_node_id) {
                continue;
            }
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), i, Message::create_message(this, CRFIN));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, CRFIN),i);
#endif
		}///通知远程提交
		rsp_cnt = 0;
		rc = commit_continuous(yield, cor_id);//local commit continuous txn本地提交
        finished_server_count++;
    }
	return rc;
}

RC TxnManager::sigle_commit_continuous(yield_func_t &yield, uint64_t cor_id) {
    uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	INC_STATS(get_thd_id(), trans_process_count, 1);
	RC rc = RCOK;

    if(is_multi_part() && !rdma_one_side()) {
        uint64_t finish_start_time = get_sys_clock();
        txn_stats.finish_start_time = finish_start_time;
        uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
        INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
        INC_STATS(get_thd_id(), trans_prepare_count, 1);

		rsp_cnt = 0;
		rc = commit_continuous(yield, cor_id);//local commit continuous txn本地提交
        finished_server_count++;
    }
	return rc;
}

void TxnManager::send_prepare_messages() {
	rsp_cnt = query->partitions_touched.size() - 1;
	DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
	if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
		continue;
	}
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(query->partitions_touched[i]), Message::create_message(this, RPREPARE));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RPREPARE),
											GET_NODE_ID(query->partitions_touched[i]));
#endif
	}
}

void TxnManager::send_finish_messages() {
	rsp_cnt = query->partitions_touched.size() - 1;
	assert(IS_LOCAL(get_txn_id()));
	DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
			continue;
    }
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(query->partitions_touched[i]), Message::create_message(this, RFIN));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN),
											GET_NODE_ID(query->partitions_touched[i]));
#endif
	}
}

int TxnManager::received_response(RC rc) {
	assert(txn->rc == RCOK || txn->rc == Abort);
	if (txn->rc == RCOK) txn->rc = rc;
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	++rsp_cnt;
#else
  if (rsp_cnt > 0)
	  --rsp_cnt;
#endif
	return rsp_cnt;
}

bool TxnManager::waiting_for_response() { return rsp_cnt > 0; }

bool TxnManager::is_multi_part() {
	return query->partitions_touched.size() > 1;
	//return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
	uint64_t commit_time = get_sys_clock();
	uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
	uint64_t timespan_long  = commit_time - txn_stats.starttime;
	INC_STATS(get_thd_id(),total_txn_commit_cnt,1);
	INC_STATS(get_thd_id(),remote_index_get_operation,remote_operate_num);

	uint64_t warmuptime = get_sys_clock() - simulation->run_starttime;
	DEBUG("Commit_stats execute_time %ld warmup_time %ld\n",warmuptime,g_warmup_timer);
	if (simulation->is_warmup_done())
		DEBUG("Commit_stats total_txn_commit_cnt %ld\n",stats._stats[get_thd_id()]->total_txn_commit_cnt);
	if(!IS_LOCAL(get_txn_id()) && (CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN)) {
		INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
		txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long,
													 timespan_short);
		return;
	}


	INC_STATS(get_thd_id(),txn_cnt,1);
	INC_STATS(get_thd_id(),local_txn_commit_cnt,1);
	INC_STATS(get_thd_id(), txn_run_time, timespan_long);
	if(query->partitions_touched.size() > 1) {
		INC_STATS(get_thd_id(),multi_part_txn_cnt,1);
		INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan_long);
	} else {
		INC_STATS(get_thd_id(),single_part_txn_cnt,1);
		INC_STATS(get_thd_id(),single_part_txn_run_time,timespan_long);
	}
	/*if(cflt) {
		INC_STATS(get_thd_id(),cflt_cnt_txn,1);
	}*/
	txn_stats.commit_stats(get_thd_id(),get_txn_id(),get_batch_id(),timespan_long, timespan_short);
	#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	return;
	#endif

	INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),last_start_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),first_start_commit_latency, timespan_long);

	assert(query->partitions_touched.size() > 0);
	INC_STATS(get_thd_id(),parts_touched,query->partitions_touched.size());
	INC_STATS(get_thd_id(),part_cnt[query->partitions_touched.size()-1],1);
	for(uint64_t i = 0 ; i < query->partitions_touched.size(); i++) {
		INC_STATS(get_thd_id(),part_acc[query->partitions_touched[i]],1);
	}
}
#if !USE_COROUTINE
void TxnManager::register_thread(Thread * h_thd) {
	this->h_thd = h_thd;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}
#else 
void TxnManager::register_thread(WorkerThread * h_thd, uint64_t cor_id) {
	this->h_thd = h_thd;
	this->_cor_id = cor_id;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}
#endif
void TxnManager::set_txn_id(txnid_t txn_id) { txn->txn_id = txn_id; }

txnid_t TxnManager::get_txn_id() { return txn->txn_id; }

Workload *TxnManager::get_wl() { return h_wl; }

uint64_t TxnManager::get_thd_id() {
	if(h_thd)
	return h_thd->get_thd_id();
	else
	return 0;
}

BaseQuery *TxnManager::get_query() { return query; }
void TxnManager::set_query(BaseQuery *qry) { query = qry; }

void TxnManager::set_timestamp(ts_t timestamp) { txn->timestamp = timestamp; }

ts_t TxnManager::get_timestamp() { assert(txn->timestamp!=1);return txn->timestamp; }

void TxnManager::set_start_timestamp(uint64_t start_timestamp) {
	txn->start_timestamp = start_timestamp;
}

ts_t TxnManager::get_start_timestamp() { return txn->start_timestamp; }

uint64_t TxnManager::incr_lr() {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}

uint64_t TxnManager::decr_lr() {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}
uint64_t TxnManager::incr_rsp(int i) {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}
uint64_t TxnManager::decr_rsp(int i) {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}

void TxnManager::release_last_row_lock(yield_func_t &yield, uint64_t cor_id,RC rc) {
	assert(txn->row_cnt > 0);
	row_t * orig_r = txn->accesses[txn->row_cnt-1]->orig_row;
	access_t type = txn->accesses[txn->row_cnt-1]->type;

#if CC_ALG == RDMA_BAMBOO_NO_WAIT
    uint64_t location = txn->accesses[txn->row_cnt-1]->location;
    uint64_t offset = txn->accesses[txn->row_cnt-1]->offset;
    row_t * remote_row = NULL;
    uint64_t try_lock = -1;
	if(type == RD){//release lock

        bool success_lock = false;
        while(success_lock == false && !simulation->is_done()){
            remote_row = read_remote_row(yield,location,offset,cor_id);
        
            uint64_t *lock_info = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
            *lock_info = remote_row->_tid_word;
            uint64_t new_lock_info;
            uint64_t lock_type;
            uint64_t lock_num;
            uint64_t new_lock_num;  
            Row_rdma_bamboo::info_decode(*lock_info,lock_type,lock_num);
            new_lock_num = lock_num-1;
            Row_rdma_bamboo::info_encode(new_lock_info,lock_type,new_lock_num);

            //remote CAS unlock
            try_lock = -1;
            try_lock = cas_remote_content(yield,location,offset,*lock_info,new_lock_info,cor_id);

            if(try_lock == *lock_info){
                success_lock = true;
                mem_allocator.free(lock_info, sizeof(uint64_t));
                break;
            }else{
                mem_allocator.free(lock_info, sizeof(uint64_t));
                continue;
            }
        }

    }else if(type == WR){//modify and release lock
        remote_row = read_remote_row(yield,location,offset,cor_id);
        remote_row->_tid_word = 0;
        uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
        for(int i = 0;i < RETIRE_NUM;i++){
            if(remote_row->lock_retire[i] == 0){
                remote_row->lock_retire[i] = get_txn_id();
                break;
            }else{
                if( i == RETIRE_NUM - 1){//no location in retire_lock
                    //TODO
			        rc = Abort;
                }
            }
        }
        assert(write_remote_row(yield,location,operate_size,offset,(char*)remote_row,cor_id) == true);
    }
#else
	orig_r->return_row(RCOK, type, this, NULL);
#endif
	//txn->accesses[txn->row_cnt-1]->orig_row = NULL;
}

void TxnManager::cleanup_row(yield_func_t &yield, RC rc, uint64_t rid, vector<vector<uint64_t>>& remote_access, uint64_t cor_id) {
	access_t type = txn->accesses[rid]->type;
	if (type == WR && rc == Abort && CC_ALG != MAAT && CC_ALG != RDMA_MAAT) {
		type = XP;
	}
    bool is_local = true;
	uint64_t version = 0;
	// Handle calvin elsewhere

#if CC_ALG == OPT_NO_WAIT3
	row_t * orig_r = txn->accesses[rid]->orig_row;
    version = orig_r->return_row_special(rc, type, this, txn->accesses[rid]->data,rid);  
    // version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);  
	if (type == WR) txn->accesses[rid]->version = version;
    // txn->accesses[rid]->data = NULL;
#endif  

#if CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN && CC_ALG != OPT_NO_WAIT3
#if ISOLATION_LEVEL != READ_UNCOMMITTED
	row_t * orig_r = txn->accesses[rid]->orig_row;
#if CC_ALG == RDMA_SILO || CC_ALG == RDMA_MOCC
  if (txn->accesses[rid]->location == g_node_id) {
    if (ROLL_BACK && type == XP &&
        (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE ||
        CC_ALG == HSTORE_SPEC)) {
      orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data);
    } else {
  #if ISOLATION_LEVEL == READ_COMMITTED
      if(type == WR) {
        version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
      }
  #else
      version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
  #endif
    }
  }
#elif CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT|| CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_BAMBOO_NO_WAIT  || CC_ALG == RDMA_OPT_NO_WAIT2 || CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK// ||  CC_ALG == RDMA_DSLR_NO_WAIT
	if(txn->accesses[rid]->location == g_node_id) is_local=true;
	else is_local=false;

	//recover local abort write
  	if (ROLL_BACK && type == XP && is_local){
       orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data); 
	} else {
#if ISOLATION_LEVEL == READ_COMMITTED
		if(type == WR) {
		version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
		}
#endif
	}
#elif CC_ALG == RDMA_MAAT || CC_ALG == RDMA_CICADA
    if(txn->accesses[rid]->location == g_node_id) is_local = true;
	else is_local = false;
#if ISOLATION_LEVEL == READ_COMMITTED
	if(type == WR) {
		version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
	}
#else
    version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
#endif
#elif CC_ALG ==RDMA_TS1 
	if (type == RD || type == SCAN) {
		version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
	} else if (type == WR || type == XP) { 
		// if(type == WR) assert(txn->accesses[rid]->data != NULL);
		
		//1.No distinction is made between local and remote
		// rdmats_man.commit_write(this, rid, type); //COMMIT
		//2.Distinguish between local and remote
		if (txn->accesses[rid]->location != g_node_id)
			is_local = false;
		if (is_local) {
			version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
		} else {
			if(type ==  XP) rdmats_man.commit_write(yield, this, rid, type, cor_id);
			else remote_access[txn->accesses[rid]->location].push_back(rid);
		}
	} else {
		assert(false);
	}
#elif CC_ALG == RDMA_TS
	if (type == RD || type == SCAN) {
		version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
	} else if (type == WR || type == XP) { 
		if(type == WR)
			assert(txn->accesses[rid]->data != NULL);

		if (txn->accesses[rid]->location != g_node_id)
			is_local = false;
		if (is_local) {
			version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
		} else {
			remote_access[txn->accesses[rid]->location].push_back(rid);
			rdmats_man.commit_write(yield, this, rid, type, cor_id);
		}
	} else {
		assert(false);
	}
#elif CC_ALG == CICADA
	version = orig_r->manager->commit(this, type);
#else
  if (ROLL_BACK && type == XP &&
      (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE ||
      CC_ALG == HSTORE_SPEC || CC_ALG == WOUND_WAIT)) {
    orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data); 
  } else {
#if ISOLATION_LEVEL == READ_COMMITTED
    if(type == WR) {
      version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
    }
#else
    version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);  
#endif
  }
#endif
#endif

#if ROLL_BACK && \
		(CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_BAMBOO_NO_WAIT || CC_ALG == RDMA_OPT_NO_WAIT2|| CC_ALG == RDMA_OPT_NO_WAIT3)
	if (type == WR && is_local) {
		//printf("free 10 %ld\n",get_txn_id());
		txn->accesses[rid]->orig_data->free_row();
		DEBUG_M("TxnManager::cleanup row_t free\n");
		row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		if(rc == RCOK) {
			INC_STATS(get_thd_id(),record_write_cnt,1);
			++txn_stats.write_cnt;
		}
	}
#endif
#endif

	if (type == WR) txn->accesses[rid]->version = version;
#if CC_ALG == TICTOC
	if (_min_commit_ts > glob_manager.get_max_cts())
		glob_manager.set_max_cts(_min_commit_ts);
#endif

#if CC_ALG != SILO && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2 && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_WOUND_WAIT && CC_ALG != RDMA_OPT_NO_WAIT && CC_ALG != RDMA_OPT_WAIT_DIE && CC_ALG != RDMA_BAMBOO_NO_WAIT && CC_ALG != RDMA_OPT_NO_WAIT2 && CC_ALG != RDMA_OPT_NO_WAIT3 && CC_ALG != RDMA_DOUBLE_RANGE_LOCK && CC_ALG != RDMA_SINGLE_RANGE_LOCK
  txn->accesses[rid]->data = NULL;
#endif
}

void TxnManager::cleanup(yield_func_t &yield, RC rc, uint64_t cor_id) {
#if CC_ALG == SILO
    finish(rc);
#endif
#if CC_ALG == OCC && MODE == NORMAL_MODE
	occ_man.finish(rc,this);
#endif
#if CC_ALG == BOCC && MODE == NORMAL_MODE
	bocc_man.finish(rc,this);
#endif
#if CC_ALG == FOCC && MODE == NORMAL_MODE
	focc_man.finish(rc,this);
#endif
#if (CC_ALG == WSI) && MODE == NORMAL_MODE
	wsi_man.finish(rc,this);
#endif
//TODO-relase lock
#if CC_ALG == RDMA_SILO
    rsilo_man.finish(yield,rc,this, cor_id);
#endif

#if CC_ALG == RDMA_MOCC
	rmocc_man.finish(yield,rc,this, cor_id);
#endif

#if CC_ALG == RDMA_CNULL
    rcnull_man.finish(rc,this);
#endif

#if CC_ALG == RDMA_MVCC
    rmvcc_man.finish(yield, rc, this, cor_id);
#endif

#if CC_ALG == RDMA_MAAT
    rmaat_man.finish(yield, rc, this, cor_id);
#endif
#if CC_ALG == RDMA_CICADA
	rcicada_man.finish(yield, rc, this, cor_id);
#endif
#if CC_ALG == RDMA_TS1
    rdmats_man.finish(rc,this);
#endif
#if CC_ALG == RDMA_TS
    rdmats_man.finish(rc,this);
#endif
#if  CC_ALG == RDMA_DSLR_NO_WAIT
    dslr_man.finish(yield,rc,this,cor_id);
#endif
   
	ts_t starttime = get_sys_clock();
	uint64_t row_cnt = txn->accesses.get_count();
	assert(txn->accesses.get_count() == txn->row_cnt);
	assert((WORKLOAD == YCSB && row_cnt <= g_req_per_query) || (WORKLOAD == TPCC && row_cnt <=
	g_max_items_per_txn*2 + 3));

	DEBUG("Cleanup %ld %ld\n",get_txn_id(),row_cnt);

	vector<vector<uint64_t>> remote_access(g_node_cnt); //for DBPA, collect remote abort write
#if CC_ALG != OPT_NO_WAIT3
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		assert(txn->accesses[rid]->data!=NULL);
		cleanup_row(yield, rc,rid,remote_access,cor_id);  //return abort write row		
	}
#else
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
    if(ycsb_query->query_type == YCSB_DISCRETE){
        assert(row_cnt == txn->locked_node.get_count());
        for (int rid = row_cnt - 1; rid >= 0; rid --) {
            assert(txn->accesses[rid]->data!=NULL);
            cleanup_row(yield, rc,rid,remote_access,cor_id);  //return abort write row		
	    }
    }else{
        uint64_t locked_node_count = txn->locked_node.get_count();
        for(int i = 0;i < locked_node_count;i++){
            if(txn->locked_node[i]->server_id == g_node_id){
                bt_node * leaf_node =(bt_node*)(txn->locked_node[i]->bt_node_location);
                uint64_t faa_num = -1;
                faa_num = faa_num<<16;
                uint64_t faa_result = ATOM_FETCH_ADD(leaf_node->intent_lock,faa_num);
                // printf("[txn.cpp:1352]faa_reslut = %ld , intent_lock = %ld,old S = %ld ,new S = %ld\n",faa_result,leaf_node->intent_lock,decode_s_lock(faa_result),decode_s_lock(leaf_node->intent_lock));
            }
        }
    }
    
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
    r2pl_man.finish(yield,rc,this,cor_id);
#endif
#if CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_OPT_NO_WAIT2
    o2pl_man.finish(yield, rc, this, cor_id);
#endif

#if CC_ALG == RDMA_BAMBOO_NO_WAIT
    bamboo_man.finish(yield, rc, this, cor_id);
#endif

#if CC_ALG == RDMA_OPT_NO_WAIT3
    nowait3_man.finish(yield, rc, this, cor_id);
#endif

#if CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    rangelock_man.finish(yield,rc,this,cor_id);
#endif
#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
		CC_ALG == DLI_MVCC_BASE
	dli_man.finish_trans(rc, this);
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	// cleanup locked rows
	for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
		row_t * row = calvin_locked_rows[i];
		row->return_row(rc,RD,this,row);
	}
#endif

#if CC_ALG == DTA
	dta_man.finish(rc, this);
#endif
	if (rc == Abort) {
		txn->release_inserts(get_thd_id());
		txn->insert_rows.clear();

		INC_STATS(get_thd_id(), abort_time, get_sys_clock() - starttime);
	}
}

RC TxnManager::get_lock(row_t * row, access_t type) {
	if (calvin_locked_rows.contains(row)) {
		return RCOK;
	}
	calvin_locked_rows.add(row);
	RC rc = row->get_lock(type, this);
	if(rc == WAIT) {
		INC_STATS(get_thd_id(), txn_wait_cnt, 1);
	}
	return rc;
}

RC TxnManager::get_row(yield_func_t &yield,row_t * row, access_t type, row_t *& row_rtn,uint64_t cor_id, uint64_t req_key,itemid_t *m_item) {
	uint64_t starttime = get_sys_clock();
	uint64_t timespan;
	RC rc = RCOK;
	DEBUG_M("TxnManager::get_row access alloc\n");
	Access * access = NULL;
    record_intent_lock * record_node = NULL;
	this->last_row = row;
	this->last_type = type;
  	uint64_t get_access_end_time = 0;
#if CC_ALG == TICTOC
	bool isexist = false;
	uint64_t size = get_write_set_size();
	size += get_read_set_size();
	// UInt32 n = 0, m = 0;
	for (uint64_t i = 0; i < size; i++) {
		if (txn->accesses[i]->orig_row == row) {
			access = txn->accesses[i];
			access->orig_row->get_ts(access->orig_wts, access->orig_rts);
			isexist = true;
			// DEBUG("TxnManagerTictoc::find the exist access \n", access->orig_data, access->orig_row, access->data, access->orig_rts, access->orig_wts);
			break;
		}
	}
	if (!access) {
		access_pool.get(get_thd_id(),access);

    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);

		rc = row->get_row(type, this, access->data, access->orig_wts, access->orig_rts);
		if (!OCC_WAW_LOCK || type == RD) {
			_min_commit_ts = _min_commit_ts > access->orig_wts ? _min_commit_ts : access->orig_wts;
		} else {
			if (rc == WAIT)
						ATOM_ADD_FETCH(_num_lock_waits, 1);
						if (rc == Abort || rc == WAIT)
								return rc;
		}
    INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
    INC_STATS(get_thd_id(), trans_get_row_count, 1);
	} else {
    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
  }
	if (!OCC_WAW_LOCK || type == RD) {
		access->locked = false;
	} else {
		_min_commit_ts = _min_commit_ts > access->orig_rts + 1 ? _min_commit_ts : access->orig_rts + 1;
		access->locked = true;
	}
#else
	access_pool.get(get_thd_id(),access);
    locked_node_pool.get(get_thd_id(),record_node);
	get_access_end_time = get_sys_clock();
	INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
	INC_STATS(get_thd_id(), trans_get_access_count, 1);
#endif
	//uint64_t row_cnt = txn->row_cnt;
	//assert(txn->accesses.get_count() - 1 == row_cnt);
#if CC_ALG == RDMA_TS1 || CC_ALG == RDMA_MVCC || CC_ALG == RDMA_TS
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG != TICTOC
  // uint64_t start_time = get_sys_clock();
  //for NO_WAIT, lock and preserve access
#if CC_ALG == WOUND_WAIT
	if (txn_state == WOUNDED) 
		rc = Abort;
	else 
		rc = row->get_row(yield,type, this, access,cor_id);
#else
	rc = row->get_row(yield, type, this, access, cor_id, req_key,m_item);
#endif
	INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
	INC_STATS(get_thd_id(), trans_get_row_count, 1);
#endif
#if CC_ALG == FOCC
	focc_man.active_storage(type, this, access);
#endif
  	uint64_t middle_time = get_sys_clock();
	if (rc == Abort || rc == WAIT) {
		row_rtn = NULL;
		DEBUG_M("TxnManager::get_row(abort) access free\n");
		access_pool.put(get_thd_id(),access);
        locked_node_pool.put(get_thd_id(),record_node);
		timespan = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
		INC_STATS(get_thd_id(), trans_store_access_count, 1);
		INC_STATS(get_thd_id(), txn_manager_time, timespan);
		INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
		//cflt = true;
#if DEBUG_TIMELINE
		printf("CONFLICT %ld %ld\n",get_txn_id(),get_sys_clock());
#endif
		return rc;
	}
#if CC_ALG == OPT_NO_WAIT3
if(rc != Abort){
    record_node->bt_node_location = m_item->parent;
    record_node->server_id = g_node_id;
}
    // txn->txn->locked_node.add(record);
#endif
	access->type = type;
	access->orig_row = row; //access->data == access->orig_row
#if CC_ALG == SILO
	access->tid = last_tid;
#endif
#if CC_ALG ==RDMA_NO_WAIT || CC_ALG ==RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_DSLR_NO_WAIT|| CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_BAMBOO_NO_WAIT || CC_ALG == RDMA_OPT_NO_WAIT2 || CC_ALG == RDMA_OPT_NO_WAIT2
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK
    access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
    access->leaf_offset = m_item->leaf_node_offset;
#endif
#if CC_ALG == RDMA_SILO
	access->timestamp = row->timestamp;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_MOCC
	access->timestamp = row->timestamp;
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_MVCC
   access->offset = (char*)row - rdma_global_buffer;
   access->old_version_num = row->version_num;
#endif

#if CC_ALG == RDMA_MAAT
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif

#if CC_ALG == RDMA_CICADA
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
	
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_BAMBOO_NO_WAIT || CC_ALG == RDMA_OPT_NO_WAIT2 || CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK)
	if (type == WR) {
	//printf("alloc 10 %ld\n",get_txn_id());
	uint64_t part_id = row->get_part_id();
	DEBUG_M("TxnManager::get_row row_t alloc\n")
	row_pool.get(get_thd_id(),access->orig_data);
	access->orig_data->init(row->get_table(), part_id, 0);
	access->orig_data->copy(row);
    //  for(int i = 0;i < this->txn->row_cnt; i++){
    //      if(txn->accesses[i]->type == WR)
    //             printf("txn %ld o_d[%ld] table_idx = %ld \n",this->get_txn_id(),i,txn->accesses[i]->orig_data->table_idx);
    // }
    //printf("\n");
	assert(access->orig_data->get_schema() == row->get_schema());

	// ARIES-style physiological logging
#if LOGGING
		// LogRecord * record =
		// logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
		LogRecord *record = logger.createRecord(
				get_txn_id(), L_UPDATE, row->get_table()->get_table_id(), row->get_primary_key());
	if(g_repl_cnt > 0) {
#if USE_RDMA == CHANGE_MSG_QUEUE
            tport_man.rdma_thd_send_msg(get_thd_id(), g_node_id + g_node_cnt + g_client_node_cnt, Message::create_message(record, LOG_MSG));
#else
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
#endif
	}
	logger.enqueueRecord(record);
#endif
	}
#endif

#if CC_ALG == TICTOC
	if (!isexist) {
		++txn->row_cnt;
		if (type == WR)
			++txn->write_cnt;
			txn->accesses.add(access);
	}
#else
	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
	txn->accesses.add(access);
    txn->locked_node.add(record_node);
#endif
   
	timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
  	INC_STATS(get_thd_id(), trans_store_access_count, 1);
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;

	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN) assert(rc == RCOK);
	assert(rc == RCOK);
	return rc;
}

RC TxnManager::get_row_post_wait(row_t *& row_rtn) {
	assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);

	uint64_t starttime = get_sys_clock();
	row_t * row = this->last_row;
	access_t type = this->last_type;
	assert(row != NULL);
	DEBUG_M("TxnManager::get_row_post_wait access alloc\n")
	Access * access;
	access_pool.get(get_thd_id(),access);

	row->get_row_post_wait(type,this,access->data);

	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_BAMBOO_NO_WAIT || CC_ALG == RDMA_OPT_NO_WAIT2 || CC_ALG == RDMA_OPT_NO_WAIT3)
	if (type == WR) {
		uint64_t part_id = row->get_part_id();
		//printf("alloc 10 %ld\n",get_txn_id());
		DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
		row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
		// for(int i = 0;i < this->txn->row_cnt;i++){
		// 	if(txn->accesses[i]->type == WR)
        //     // printf("txn %ld orgin_d[%ld] table %ld",this->get_txn_id(),i,this->txn->accesses[i]->orig_data->table_idx);
        // }
	}
#endif

	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
#if CC_ALG == RDMA_SILO
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_MOCC
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_MVCC
   access->offset = (char*)row - rdma_global_buffer;
   access->old_version_num = row->version_num;
#endif

	txn->accesses.add(access);
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
	return RCOK;
}

uint64_t TxnManager::get_part_num(uint64_t num,uint64_t part){
    uint64_t result = 0;
    switch(part){
        case 1:
            result = num>>48;
            break;
        case 2:
            result = (num<<16)>>48;
            break;
        case 3:
            result = (num<<32)>>48;
            break;
        case 4:
            result = (num<<48)>>48;
            break;
        default:
            assert(false);    
    }
    return result;
}


RC TxnManager::get_remote_row(yield_func_t &yield, access_t type, uint64_t loc, itemid_t *m_item, row_t *& row_local, uint64_t cor_id) {
	RC rc = RCOK;

	#if CC_ALG == RDMA_CNULL
		int one_cnt = RDMA_ONE_CNT;
		for (int i  = 0; i < one_cnt; i++) {
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			mem_allocator.free(test_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
		return rc;
	#endif

	#if CC_ALG == RDMA_SILO
		if(type == RD || type == WR){
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			// assert(test_row->get_primary_key() == req->key);

			RC rc = RCOK;
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		} else {
			assert(false);
		}
		return rc;
	#endif
	#if CC_ALG == RDMA_MOCC
		if(type == RD || type == WR){
			uint64_t tts = get_timestamp();
			RC rc = RCOK;
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			if(test_row->is_hot > HOT_VALUE * 10) {
				test_row->is_hot = 0;
			}
			if (test_row->is_hot > HOT_VALUE) {
			retry_lock:
				uint64_t try_lock = -1;
				uint64_t lock_type = 0;
				bool conflict = false;
				bool canwait = true;
				try_lock = cas_remote_content(yield, loc, m_item->offset, 0, txn->txn_id, cor_id);
				if(try_lock != 0) {
					mem_allocator.free(m_item, sizeof(itemid_t));
					rc = Abort;
					return rc;
					// goto retry_lock;	
					// printf("cas retry\n");
				}
				row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
				// assert(test_row->get_primary_key() == req->key);
				//! test_row->is_hot++;

				lock_type = test_row->lock_type;
				if(lock_type == 0) {
					test_row->lock_owner[0] = txn->txn_id;
					test_row->ts[0] = tts;
					test_row->lock_type == RD? 2:1;
					test_row->_tid_word = 0;
					lock_set.insert(test_row->get_primary_key());
					num_locks++;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					rc = RCOK;
				} else if(lock_type == 1 || type == WR) {
					conflict = true;
					if(conflict) {
						uint64_t i = 0; 
						for(i = 0; i < LOCK_LENGTH; i++) {
							if((tts > test_row->ts[i] && test_row->ts[i] != 0)) {
								canwait = false;
								test_row->_tid_word = 0;
								test_row->is_hot++;
								assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
								mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
								mem_allocator.free(m_item, sizeof(itemid_t));
								rc = Abort;
								return rc;
							}
							// if(test_row->ts[0] == 0) break;
						}
						// total_num_atomic_retry++;
						test_row->_tid_word = 0;
						assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
						mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));	
						if (!simulation->is_done()) goto retry_lock;
						else {
							DEBUG_M("TxnManager::get_row(abort) access free\n");
							row_local = NULL;
							test_row->is_hot++;
							txn->rc = Abort;
							mem_allocator.free(m_item, sizeof(itemid_t));
							mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
							return Abort; //原子性被破坏，CAS失败	
						}
					}
				} else {
					uint64_t i = 0; 
					for(i = 0; i < LOCK_LENGTH; i++) {
						if(test_row->ts[i] == 0) {
							test_row->ts[i] = tts;
							test_row->lock_owner[i] == txn->txn_id;
							test_row->lock_type == RD? 2:1;
							lock_set.insert(test_row->get_primary_key());
							num_locks++;
							test_row->_tid_word = 0;
							assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
							rc = RCOK;
							break;
						}
					}
					if(i == LOCK_LENGTH) {
						test_row->_tid_word = 0;
						test_row->is_hot++;
						assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
						mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						mem_allocator.free(m_item, sizeof(itemid_t));
						rc = Abort;
						return rc;
					}
				}
			}
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		} else {
			assert(false);
		}
		rc = RCOK;
		return rc;
	#endif
	#if CC_ALG == RDMA_MVCC
		row_t * test_row;
		if(type == RD){
			//read remote data
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			// assert(test_row->get_primary_key() == req->key);

			uint64_t change_num = 0;
			bool result = false;
			result = get_version(test_row,&change_num,txn);
			if(result == false){//no proper version: Abort
				INC_STATS(get_thd_id(), result_false, 1);
				// printf("remote %ld no version\n",test_row->get_primary_key());
				rc = Abort;
				return rc;
			}
			//check txn_id
			if(test_row->txn_id[change_num] != 0 && test_row->txn_id[change_num] != get_txn_id() + 1){
				INC_STATS(get_thd_id(), result_false, 1);
				// printf("remote %ld write by other %ld\n",test_row->get_primary_key(),test_row->txn_id[change_num]);
				rc = Abort;
				return rc;
			}
			//CAS(old_rts,new_rts)
			uint64_t version = change_num;
			uint64_t old_rts = test_row->rts[version];
			uint64_t new_rts = get_timestamp();
			uint64_t rts_offset = m_item->offset + 2*sizeof(uint64_t) + HIS_CHAIN_NUM*sizeof(uint64_t) + version*sizeof(uint64_t);
			uint64_t cas_result = cas_remote_content(yield,loc,rts_offset,old_rts,new_rts,cor_id);//lock
			if(cas_result!=old_rts){ //CAS fail, atomicity violated
				INC_STATS(get_thd_id(), result_false, 1);
				// printf("remote %ld rts update failed old %ld now %ld new %ld\n",test_row->get_primary_key(), old_rts, cas_result, new_rts);
				rc = Abort;
				return rc;			
			}
			//read success
			test_row->rts[version] = get_timestamp();
		}
		else if(type == WR){
			uint64_t lock = get_txn_id()+1;
			uint64_t try_lock = -1;
		#if USE_DBPAOR
			test_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,0,lock,cor_id);
			if(try_lock != 0){
				INC_STATS(get_thd_id(), lock_fail, 1);
				mem_allocator.free(test_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				// printf("remote %ld lock failed other %ld me %ld\n",test_row->get_primary_key(), try_lock, lock);
				rc = Abort;
				return rc;			
			}
		#else
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,lock,cor_id);//lock
			if(try_lock != 0){
				INC_STATS(get_thd_id(), lock_fail, 1);
				rc = Abort;
				// printf("remote %ld lock failed other %ld me %ld\n", test_row->get_primary_key(),try_lock, lock);
				return rc;
			}
			//read remote data
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
			// assert(test_row->get_primary_key() == req->key);

			uint64_t version = (test_row->version_num)%HIS_CHAIN_NUM;
			if((test_row->txn_id[version] != 0 && test_row->txn_id[version] != get_txn_id() + 1)||(get_timestamp() <= test_row->rts[version])){
				INC_STATS(get_thd_id(), ts_error, 1);
				//unlock and Abort
				uint64_t* temp__tid_word = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
				*temp__tid_word = 0;
				assert(write_remote_row(loc, sizeof(uint64_t),m_item->offset,(char*)(temp__tid_word))==true);
				// printf("remote %ld write by other other %ld (write op)\n", test_row->get_primary_key(),test_row->txn_id[version]);
				mem_allocator.free(temp__tid_word,sizeof(uint64_t));
				mem_allocator.free(test_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				rc = Abort;
				return rc;
			}

			test_row->txn_id[version] = get_txn_id() + 1;
			test_row->rts[version] = get_timestamp();
			//temp_row->version_num = temp_row->version_num + 1;
			test_row->_tid_word = 0;//release lock
			//write back row
			// printf("remote %ld write %ld\n",test_row->get_primary_key(),test_row->txn_id[version]);
			uint64_t operate_size = row_t::get_row_size(test_row->tuple_size);
			assert(write_remote_row(loc,operate_size,m_item->offset,(char*)test_row)==true);
		}
		//preserve the txn->access
		
		rc =  RCOK;
		rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		return rc;
	#endif

	#if CC_ALG == RDMA_NO_WAIT
	remote_atomic_retry_lock:
		if(type == RD || type == WR){
			uint64_t new_lock_info;
			uint64_t lock_info;
			row_t * test_row = NULL;

			if(type == RD){ //读集元素
				//第一次rdma read，得到数据项的锁信息
				row_t * lock_read = read_remote_row(yield,loc,m_item->offset,cor_id);

				new_lock_info = 0;
				lock_info = lock_read->_tid_word;
				mem_allocator.free(lock_read, row_t::get_row_size(ROW_DEFAULT_SIZE));
	
				bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

				if(conflict){
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					row_local = NULL;
					txn->rc = Abort;
                    INC_STATS(get_thd_id(), no_wait_abort1, 1);
					return Abort;
				}
				if(new_lock_info == 0){
					// printf("---thd：%lu, remote lock fail!!!!!!lock location: %lu; %p, txn: %lu, old lock_info: %lu, new_lock_info: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id(), lock_info, new_lock_info);
				} 
				assert(new_lock_info!=0);
			} else {
				lock_info = 0; //if lock_info!=0, CAS fail , Abort
				new_lock_info = 3; //binary 11, aka 1 read lock
			}
		#if USE_DBPAOR == true
			uint64_t try_lock = -1;
			test_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,lock_info,new_lock_info,cor_id);
			if(try_lock != lock_info && type == RD){ //CAS fail: Atomicity violated
				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				lock_info = try_lock;
				if (!simulation->is_done()) goto remote_atomic_retry_lock;
				else {
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					row_local = NULL;
					txn->rc = Abort;
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                    INC_STATS(get_thd_id(), no_wait_abort2, 1);
					return Abort; //原子性被破坏，CAS失败	
				}
			} else if (try_lock != lock_info && type == WR) {
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                INC_STATS(get_thd_id(), no_wait_abort3, 1);
				return Abort; //原子性被破坏，CAS失败	
			}
			// assert(test_row->get_primary_key() == req->key);
		#else
			uint64_t try_lock = -1;
			try_lock = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info,cor_id);
			if(try_lock != lock_info){
				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
				lock_info = try_lock;
				if (!simulation->is_done()) goto remote_atomic_retry_lock;
				else {
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					row_local = NULL;
					txn->rc = Abort;
					mem_allocator.free(m_item, sizeof(itemid_t));
					// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					return Abort; //原子性被破坏，CAS失败	
				}
			} else if (try_lock != lock_info && type == WR) {
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort; //原子性被破坏，CAS失败
			}
			//read remote data
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			// assert(test_row->get_primary_key() == req->key);
		#endif
			//preserve the txn->access
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
			return rc;
		}
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_DSLR_NO_WAIT//TODO
		int wait_slice = 1 * 1000UL;//1us
		int count_max = 32768;
		//get remote address
		uint64_t remote_address = m_item->offset;

		//get ticket
		uint64_t faa_result = 0;
		uint64_t add_value = 1;
		if(type == RD) {
			add_value = add_value << 16;//0x0010
		}
		else if(type == WR)add_value = 1;//0x0001
		faa_result = faa_remote_content(yield,loc,remote_address,add_value,cor_id);
		
		//checkticket
		uint64_t read_lock = get_part_num(faa_result,1);
		uint64_t write_lock = get_part_num(faa_result,2);
		uint64_t read_num = get_part_num(faa_result,3);
		uint64_t write_num = get_part_num(faa_result,4);

		uint64_t reset_from = 0, try_lock = 0;
		uint64_t reset_from_address = remote_address + sizeof(uint64_t);
		if((read_num == count_max - 1) && (type == RD)){
			//set lock from count_max|write_num|count_max|write_num
			//             (read_lock|write_lock|read_num|write_num) to 0 
			reset_from = (count_max<<48)|(write_num<<32)|(count_max<<16)|(write_num);
        	try_lock = cas_remote_content(yield,loc,reset_from_address,0,reset_from,cor_id);
        	// assert(try_lock == 0);
			// if (try_lock != 0) {
			// 	printf("reset_from %ld", try_lock);
			// }
		}
		else if((write_num == count_max -1) && (type == WR)){
			reset_from = (read_num<<48)||(count_max<<32)||(read_num<<16)||(count_max);
			try_lock = cas_remote_content(yield,loc,reset_from_address,0,reset_from,cor_id);
        	// assert(try_lock == 0);
			// if (try_lock != 0) {
			// 	printf("reset_from %ld", try_lock);
			// }
		}
		//case 1 :process lock overflow
		else if(write_num >= count_max || read_num >= count_max){
			if(type == RD) {
				add_value = 1;
				add_value = add_value << 16;
				add_value = -add_value;
			}
			else if(type == WR)add_value = -1;
			faa_result = faa_remote_content(yield,loc,remote_address,add_value,cor_id);
			// mem_allocator.free(m_item, sizeof(itemid_t));
			// return Abort;

     #if 1
            uint64_t wait = rand() % 4;
            if (wait != 0) return Abort;//random abort

            //reset lock to zero 
            int repeat_num = 0;
			row_t * reset_row = read_remote_row(yield,loc,remote_address,cor_id);
            uint64_t resetlock = reset_row->_reset_from;
			mem_allocator.free(reset_row,  row_t::get_row_size(ROW_DEFAULT_SIZE));
            uint64_t cas_result = 0;
            if(resetlock != 0){
                while(!simulation->is_done()){
                    cas_result = cas_remote_content(yield,loc,remote_address,resetlock,0,cor_id);
                    repeat_num ++;
                    uint64_t new_read_lock = get_part_num(cas_result,1);
                    uint64_t new_write_lock = get_part_num(cas_result,2);
                    uint64_t new_read_num = get_part_num(cas_result,3);
                    uint64_t new_write_num = get_part_num(cas_result,4);
                    if(cas_result == resetlock || cas_result == 0 ||
                        new_read_num < count_max && new_write_num < count_max) {
                    // printf("[87]process overflow success\n");
                        uint64_t reset_from_address = remote_address + sizeof(uint64_t);
                        try_lock = cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
                        return Abort;
                }
                    
                if(repeat_num < DSLR_MAX_RETRY_TIME)continue;


                // uint64_t expect_read_lock = get_part_num(resetlock,1);
                // uint64_t expect_write_lock = get_part_num(resetlock,2);
                // uint64_t expect_read_num = get_part_num(resetlock,3);
                // uint64_t expect_write_num = get_part_num(resetlock,4);
                // printf("[117]current lock:new_read_lock = %ld, new_write_lock = %ld, new_read_num = %ld, new_write_num = %ld; ****reset lock: expect_read_lock = %ld, expect_write_lock = %ld, expect_read_num = %ld, expect_write_num = %ld\n",new_read_lock,new_write_lock,new_read_num,new_write_num,expect_read_lock,expect_write_lock,expect_read_num,expect_write_num);

                if(new_read_lock == read_lock || new_write_lock == write_lock){
                    //detect deadlock
                    if((new_read_lock == count_max && new_read_num == count_max &&type == RD) || (new_write_lock == count_max && new_write_num == count_max && type == WR)){
                        continue;
                    }
                    if((new_read_lock > count_max && new_read_num == new_read_lock &&type == RD) || (new_write_lock > count_max && new_write_num == new_write_lock && type == WR)){
                        resetlock = (new_read_lock<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
                        continue;
                    }
                    uint64_t new_lock;
                    if(type == RD){
                        new_read_lock = read_num + 1;
                        new_lock = (new_read_lock<<48)|(write_num<<32)|(new_read_num<<16)|(new_write_num);
                    }else if(type == WR){
                        new_write_lock = write_num + 1;
                        new_lock = (read_num<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
                    }

                    uint64_t new_result = 0;
                    new_result = cas_remote_content(yield,loc,remote_address,cas_result,new_lock,cor_id);
                    // if(new_result == cas_result){//release deadlock
                    //     return Abort;
                    // }else{//release fail, retry
                    // }
                }//if(deadlock)
            }//while true
        }
    #endif

		}// else if(write_num >= count_max || read_num >= count_max)
			
		//case 2:get lock
		if(((write_lock == write_num) && (type == RD)) || //no exclusive lock
			((write_lock == write_num) && (read_lock == read_num) && (type == WR))){//no exclusive lock and no shared lock
			row_t * read_row = read_remote_row(yield,loc,remote_address,cor_id);
			rc = preserve_access(row_local, m_item, read_row, type,read_row->get_primary_key(), loc);
			
			// uint64_t new_faa_result = read_row->_tid_word;
			// uint64_t new_faa_read_lock = get_part_num(new_faa_result,1);
			// uint64_t new_faa_write_lock = get_part_num(new_faa_result,2);
			// uint64_t new_faa_read_num = get_part_num(new_faa_result,3);
			// uint64_t new_faa_write_num = get_part_num(new_faa_result,4);
			// printf("remote try to acquire %ld %s lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",read_row->get_primary_key(), type == RD ? "read" : "write",read_lock,write_lock,read_num,write_num,new_faa_read_lock,new_faa_write_lock,new_faa_read_num,new_faa_write_num);
			
			return rc;
		}
		//case 3:meet conflict
		else{//wait
			int repeat_num = 0; 
			while(!simulation->is_done()){
				row_t *read_row = read_remote_row(yield,loc,remote_address,cor_id);
				// if (repeat_num == 0) {
				// 	uint64_t new_faa_result = read_row->_tid_word;
				// 	uint64_t new_faa_read_lock = get_part_num(new_faa_result,1);
				// 	uint64_t new_faa_write_lock = get_part_num(new_faa_result,2);
				// 	uint64_t new_faa_read_num = get_part_num(new_faa_result,3);
				// 	uint64_t new_faa_write_num = get_part_num(new_faa_result,4);
				// 	printf("remote try to acquire %ld %s lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld repeat\n",read_row->get_primary_key(), type == RD ? "read" : "write",read_lock,write_lock,read_num,write_num,new_faa_read_lock,new_faa_write_lock,new_faa_read_num,new_faa_write_num);
				// }
				repeat_num ++;
				uint64_t new_faa_result = read_row->_tid_word;
				uint64_t new_read_lock = get_part_num(faa_result,1);
				uint64_t new_write_lock = get_part_num(faa_result,2);
				uint64_t new_read_num = get_part_num(faa_result,3);
				uint64_t new_write_num = get_part_num(faa_result,4);
				//case 3.1 : ignored because of deadlock
				if((new_read_lock > read_num) || (new_write_lock > write_num)){
					// printf("remote abort due to %ld jump ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, repeat_num%d prev:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",read_row->get_primary_key(),new_read_lock,new_write_lock,new_read_num,new_write_num,repeat_num,read_lock,write_lock,read_num,write_num);
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					rc = Abort;
					return rc;
				}
				//case 3.2 : get lock
				if(((new_write_lock == write_num) && (type == RD)) || //no exclusive lock
                ((new_write_lock == write_num) && (new_read_lock == read_num) && (type == WR))){//no exclusive and no share lock
					// read_row = read_remote_row(yield,loc,m_item->offset,cor_id);
					rc = preserve_access(row_local, m_item, read_row, type,read_row->get_primary_key(), loc);
					return rc;
				}
				//case 3.3 : detect deadlock
				if(repeat_num < DSLR_MAX_RETRY_TIME) {
					mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					continue;
				}

				if(new_read_lock == read_lock && new_write_lock == write_lock){
					//detect deadlock
					uint64_t new_lock;
					if(type == RD){
						new_read_lock = read_num + 1;
                    	new_lock = (new_read_lock<<48)|(write_num<<32)|(new_read_num<<16)|(new_write_num);
					}else if(type == WR){
						new_write_lock = write_num + 1;
                    	new_lock = (read_num<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
					}
					uint64_t new_result = 0;
					new_result = cas_remote_content(yield,loc,remote_address,new_faa_result,new_lock,cor_id);
					if(new_result != new_faa_result){//fail, retry 
					}else{
						//deadlock release but lock overflow
						if(new_read_num >= count_max || new_write_num >= count_max){
							uint64_t reset_from = read_row->_reset_from;
							bool success = false;
							int cas_num = 0;
							if (reset_from > 0) {
								success = loop_cas_remote(yield,loc,remote_address,reset_from,0,cor_id);
							}
							try_lock = cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
							// assert(try_lock == 0);
						}//if overflow
						// uint64_t reset_read_lock = get_part_num(new_lock,1);
						// uint64_t reset_write_lock = get_part_num(new_lock,2);
						// uint64_t reset_read_num = get_part_num(new_lock,3);
						// uint64_t reset_write_num = get_part_num(new_lock,4);
						// new_read_lock = get_part_num(new_faa_result,1);
						// new_write_lock = get_part_num(new_faa_result,2);
						// new_read_num = get_part_num(new_faa_result,3);
						// new_write_num = get_part_num(new_faa_result,4);
						// printf("remote abort due to handle dead lock %ld prev:%ld, origin_lock:%ld ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, reset:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n", read_row->get_primary_key(),new_lock,new_faa_result,new_read_lock,new_write_lock,new_read_num,new_write_num,reset_read_lock,reset_write_lock,reset_read_num,reset_write_num);
						mem_allocator.free(m_item, sizeof(itemid_t));
						mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						return Abort;
					}					
				}//if deadlock
				mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			}//while
		}//else wait
    #endif

	#if CC_ALG == RDMA_NO_WAIT2
		if(type == RD || type == WR){
			uint64_t try_lock = -1;
		#if USE_DBPAOR == true
			//cas result
			//read result
			row_t * test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,1,cor_id);
			
			if(try_lock != 0){ //if CAS failed, ignore read content
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;
			}
			//CAS success, now get read content
			assert(test_row->_tid_word == 1);		
		#else
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,1,cor_id);

			if(try_lock != 0){ //CAS fail
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));

				return Abort;
			}
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			// assert(test_row->get_primary_key() == req->key);
		#endif
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
			return rc;   		
		}
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_WAIT_DIE2
		uint64_t tts = get_timestamp();
		if(type == RD || type == WR){
		#if USE_DBPAOR == true
		retry_lock:
			uint64_t try_lock;
			row_t* test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,tts, cor_id);
			if(try_lock == 0) {
				test_row->lock_owner = txn->txn_id;			
				assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
			} else {
				if(tts <= try_lock && !simulation->is_done()){ //wait
					num_atomic_retry++;
					total_num_atomic_retry++;
					if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
					//sleep(1);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					goto retry_lock;			
				}
				else{ //abort
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					row_local = NULL;
					txn->rc = Abort;
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					return Abort;			
				}
			}
			// assert(test_row->get_primary_key() == req->key);
		#else
		retry_lock:
			uint64_t try_lock = -1;
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,tts,cor_id);
			if(try_lock != 0){ // cas fail
				if(tts <= try_lock && !simulation->is_done()){  //wait
					num_atomic_retry++;
					total_num_atomic_retry++;
					if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
					goto retry_lock;			
				}	
				else{ //abort
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					row_local = NULL;
					txn->rc = Abort;
					mem_allocator.free(m_item, sizeof(itemid_t));
					return Abort;
				}
			}
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			if(try_lock == 0) {
				test_row->lock_owner = txn->txn_id;			
				assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
			}
			// assert(test_row->get_primary_key() == req->key);
		#endif
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
			return rc;	
		}
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_WAIT_DIE
		if(type == RD || type == WR) {
			uint64_t tts = get_timestamp();
		retry_lock:
			uint64_t try_lock = -1;
			uint64_t lock_type = 0;
			bool conflict = false;
			bool canwait = true;
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,txn->txn_id,cor_id);
			if(try_lock != 0){ // cas fail
				mem_allocator.free(m_item, sizeof(itemid_t));
				rc = Abort;
				return rc;
			}
			row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			lock_type = test_row->lock_type;
			if(lock_type == 0) {
				test_row->lock_owner[0] = txn->txn_id;
				test_row->ts[0] = tts;
				test_row->lock_type == RD? 2:1;
				test_row->_tid_word = 0;
				assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
				rc = RCOK;
			} else if(lock_type == 1 || type == WR) {
				conflict = true;
				if(conflict) {
					uint64_t i = 0; 
					for(i = 0; i < LOCK_LENGTH; i++) {
						if((tts > test_row->ts[i] && test_row->ts[i] != 0)) {
							canwait = false;
							test_row->_tid_word = 0;
							assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
							mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
							mem_allocator.free(m_item, sizeof(itemid_t));
							rc = Abort;
							return rc;
						}
						// if(test_row->ts[0] == 0) break;
					}
					num_atomic_retry++;
					// total_num_atomic_retry++;
					if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
					test_row->_tid_word = 0;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));	
					//sleep(1);
					if (!simulation->is_done()) goto retry_lock;
					else {
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						// row_local = NULL;
						txn->rc = Abort;
						// mem_allocator.free(m_item, sizeof(itemid_t));
						// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						return Abort; //原子性被破坏，CAS失败	
					}
				}
			} else {
				uint64_t i = 0; 
				for(i = 0; i < LOCK_LENGTH; i++) {
					if(test_row->ts[i] == 0) {
						test_row->ts[i] = tts;
						test_row->lock_owner[i] == txn->txn_id;
						test_row->lock_type == RD? 2:1;
						test_row->_tid_word = 0;
						assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
						rc = RCOK;
						break;
					}
				}
				if(i == LOCK_LENGTH) {
					test_row->_tid_word = 0;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					rc = Abort;
					return rc;
				}
			}
			// assert(test_row->get_primary_key() == req->key);
			rc = preserve_access(row_local, m_item, test_row, type, test_row->get_primary_key(), loc);
			return rc;
		}
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_WOUND_WAIT
		if(type == RD || type == WR){
			uint64_t tts = get_timestamp();
			int retry_time = 0;
			bool is_wound = false;
			RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
		retry_lock:
			uint64_t try_lock = -1;
			row_t * test_row;
			uint64_t lock_type = 0;
			bool canwait = true;
			bool canwound = true;
			retry_time += 1;
		#if USE_DBPAOR
			test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,tts, cor_id);
		#else
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,txn->txn_id,cor_id);
			if(try_lock != 0) {
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				rc = Abort;
				return rc;
			}
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
			// assert(test_row->get_primary_key() == req->key);
			lock_type = test_row->lock_type;
			if(lock_type == 0) {
				test_row->lock_owner[0] = txn->txn_id;
				test_row->ts[0] = tts;
				test_row->lock_type = type == RD? 2:1;
				test_row->_tid_word = 0;
				assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
			} else if(lock_type == 1 || type == WR) {
				uint64_t i = 0; 
				for(i = 0; i < LOCK_LENGTH; i++) {
					if((tts > test_row->ts[i] && test_row->ts[i] != 0)) {
						canwound = false;
					}
					if((tts < test_row->ts[i] && test_row->ts[i] != 0)) {
						canwait = false;
					}
				}
				if(canwound == true) {
					WOUNDState state;
					char * local_buf;
					for(int i = 0; i < LOCK_LENGTH; i++) {
						if(test_row->lock_owner[i] != 0) {
							if(test_row->lock_owner[i] % g_node_cnt == g_node_id) {
								state = rdma_txn_table.local_get_state(get_thd_id(), test_row->lock_owner[i]);
							} else {
								local_buf = rdma_txn_table.remote_get_state(yield, this, test_row->lock_owner[i], cor_id);
								memcpy(value, local_buf, sizeof(RdmaTxnTableNode));
								state = value->state;
							}
							if(state != WOUND_COMMITTING && state != WOUND_ABORTING){  //wound
								
								if(test_row->lock_owner[i] % g_node_cnt == g_node_id) {
									rdma_txn_table.local_set_state(this,get_thd_id(), test_row->lock_owner[i], WOUND_ABORTING);

								} else {
									value->state = WOUND_ABORTING;
									rdma_txn_table.remote_set_state(yield, this, test_row->lock_owner[i], WOUND_ABORTING, cor_id);
								}
							}
						}
						test_row->_tid_word = 0;
						assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
						mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						if (!simulation->is_done() && retry_time <= MAX_RETRY_TIME) goto retry_lock;
						else {
							DEBUG_M("TxnManager::get_row(abort) access free\n");
							row_local = NULL;
							txn->rc = Abort;
							mem_allocator.free(m_item, sizeof(itemid_t));
							// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
							return Abort; //原子性被破坏，CAS失败	
						}		
					}	
				}
					
				if(canwait == true) {
					num_atomic_retry++;
					total_num_atomic_retry++;
					if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
					test_row->_tid_word = 0;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					if (!simulation->is_done() && retry_time <= MAX_RETRY_TIME) goto retry_lock;
					else {
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
						// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						return Abort; //原子性被破坏，CAS失败	
					}	
				}
				if(canwait == false && canwound == false) {
					test_row->_tid_word = 0;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(value, sizeof(RdmaTxnTableNode));
					rc = Abort;
					return rc;
				}
			} else {
				uint64_t i = 0; 
				for(i = 0; i < LOCK_LENGTH; i++) {
					if(test_row->ts[i] == 0) {
						test_row->ts[i] = tts;
						test_row->lock_owner[i] == txn->txn_id;
						test_row->lock_type = type == RD? 2:1;
						test_row->_tid_word = 0;
						assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
						rc = RCOK;
						break;
					}
				}
				if(i == LOCK_LENGTH) {
					test_row->_tid_word = 0;
					assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					rc = Abort;
					return rc;
				}
			}
			
			if(value) mem_allocator.free(value, sizeof(RdmaTxnTableNode));
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
			return rc;	
		}
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_WOUND_WAIT2
		// if(type == RD || type == WR){
			uint64_t tts = get_timestamp();
			int retry_time = 0;
			bool is_wound = false;
			RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
		retry_lock:
			uint64_t try_lock = -1;
			row_t * test_row;
			
		#if USE_DBPAOR
			test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,tts, cor_id);
		#else
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,tts,cor_id);
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
			// assert(test_row->get_primary_key() == req->key);
			retry_time += 1;
			if(try_lock == 0) {
				test_row->lock_owner = txn->txn_id;			
				assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
			}
			if(try_lock != 0 && retry_time <= MAX_RETRY_TIME){ // cas fail
				// printf("try_lock: %ld\n", retry_time);
				WOUNDState state;
				char * local_buf;
				if(test_row->lock_owner % g_node_cnt == g_node_id) {
					state = rdma_txn_table.local_get_state(get_thd_id(), test_row->lock_owner);
				} else {
					local_buf = rdma_txn_table.remote_get_state(yield, this, test_row->lock_owner, cor_id);
					memcpy(value, local_buf, sizeof(RdmaTxnTableNode));
					state = value->state;
				}
				// printf("read remote state:%ld", state);
				if(tts <= try_lock && state != WOUND_COMMITTING && state != WOUND_ABORTING && is_wound == false){  //wound
					
					if(test_row->lock_owner % g_node_cnt == g_node_id) {
						rdma_txn_table.local_set_state(this,get_thd_id(), test_row->lock_owner, WOUND_ABORTING);

					} else {
						value->state = WOUND_ABORTING;
						rdma_txn_table.remote_set_state(yield, this, test_row->lock_owner, WOUND_ABORTING, cor_id);

					}
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					is_wound = true;
					if (!simulation->is_done()) goto retry_lock;
					else {
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
						// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						return Abort; //原子性被破坏，CAS失败	
					}		
				}	
				else{ //wait
					num_atomic_retry++;
					total_num_atomic_retry++;
					if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
					// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					if (!simulation->is_done()) goto retry_lock;
					else {
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
						// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						return Abort; //原子性被破坏，CAS失败	
					}			
				}
				
			} else if(try_lock != 0 && retry_time > MAX_RETRY_TIME) {
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				rc = Abort;
				return rc;
			}       
			if(value) mem_allocator.free(value, sizeof(RdmaTxnTableNode));
			rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
			return rc;	
		// }
		rc = RCOK;
		return rc;
	#endif

	#if CC_ALG == RDMA_TS1		
		ts_t ts = get_timestamp();

		row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		uint64_t* wid = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*CASCADING_LENGTH);
		// assert(remote_row->get_primary_key() == req->key);
		if(type == RD) {
			if(ts < remote_row->wts){
				rc = Abort;
				// #if DEBUG_PRINTF
				// printf("[change read wts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
				// #endif
				mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				return rc;
			} 
			else {//CAS(old_rts, new_rts) and read again
				rc = RCOK;
				if (remote_row->rts < ts){ //if ">=", no need to CAS
					ts_t old_rts = remote_row->rts;
					ts_t new_rts = ts;
					ts_t cas_result;
					uint64_t rts_offset = m_item->offset + sizeof(uint64_t)+sizeof(uint64_t)*CASCADING_LENGTH+sizeof(ts_t);
		#if USE_DBPAOR
					row_t * second_row = cas_and_read_remote(yield,cas_result,loc,rts_offset,m_item->offset,old_rts,new_rts, cor_id);
					if(cas_result!=old_rts){ //cas fail, atomicity violated
						rc = Abort;
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
						mem_allocator.free(m_item, sizeof(itemid_t));
						mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
						return rc;
					}
		#else  
					cas_result = cas_remote_content(yield,loc,rts_offset,old_rts,new_rts, cor_id);
					if(cas_result!=old_rts){ //cas fail
						rc = Abort;
						// #if DEBUG_PRINTF
						// printf("[remote change rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
						// #endif
						mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
						mem_allocator.free(m_item, sizeof(itemid_t));
						return rc;
					}
					//cas successs
					row_t * second_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
					//cas success, now do double read check
					remote_row->rts = ts;
					bool diff = false;
					for (int i = 0; i < CASCADING_LENGTH; i++) {
						if (second_row->tid[i]!=remote_row->tid[i]) {diff = true; break;}
					}
					if(second_row->wts!=remote_row->wts || diff){ //atomicity violated
						rc = Abort;
						// #if DEBUG_PRINTF
						// printf("[remote change rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
						// #endif
						DEBUG_M("TxnManager::get_row(abort) access free\n");
						mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
						mem_allocator.free(m_item, sizeof(itemid_t));
						mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
						return rc;					
					}
					//read success
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				}
			}
		}
		else if(type == WR) {
			if (ts < remote_row->rts){
				rc = Abort;
				// #if DEBUG_PRINTF
				// printf("[remote write rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
				// #endif
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				return rc;
			} else if (ts < remote_row->wts) {
				goto end;
			}
			else {//CAS(old_mutx, new_mutx) and read again
				uint64_t old_mutx = 0;
				uint64_t new_mutx = get_txn_id() + 1;
				ts_t cas_result;
				// uint64_t mutx_offset = m_item->offset + sizeof(uint64_t);
				uint64_t mutx_offset = m_item->offset;
		#if USE_DBPAOR
				row_t * second_row = cas_and_read_remote(yield,cas_result,loc,mutx_offset,m_item->offset,old_mutx,new_mutx, cor_id);
				if(cas_result!=old_mutx){ //cas fail, atomicity violated
					rc = Abort;
					#if DEBUG_PRINTF
					printf("[remote lock failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),second_row->get_primary_key(),second_row->mutx,second_row->tid,second_row->rts,second_row->wts);
					#endif
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					return rc;
				}
		#else
				cas_result = cas_remote_content(yield,loc,mutx_offset,old_mutx,new_mutx, cor_id);
				if(cas_result!=old_mutx){ //cas fail, atomicity violated
					rc = Abort;
					#if DEBUG_PRINTF
					printf("[remote lock failed]txn:%ld, lock:%lu\n",get_txn_id(),cas_result);
					#endif
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					return rc;
				}
				//cas successs
				row_t * second_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
				if(ts < second_row->rts){
					rc = Abort;
					// #if DEBUG_PRINTF
					// printf("[remote write rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",get_txn_id(),second_row->get_primary_key(),second_row->mutx,second_row->tid,second_row->rts,second_row->wts);
					// #endif
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					//RDMA WRITE, SET mutx = 0
					uint64_t* temp_tid = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
					*temp_tid = 0;
					assert(write_remote_row(loc, sizeof(uint64_t),mutx_offset,(char*)(temp_tid))==true);
					mem_allocator.free(temp_tid,sizeof(uint64_t));
					mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					return rc;					
				}
				if (ts < second_row->wts) {
					uint64_t* temp_tid = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
					*temp_tid = 0;
					assert(write_remote_row(loc, sizeof(uint64_t),mutx_offset,(char*)(temp_tid))==true);
					goto end;
				}
				//read success
				for (int i = 0; i < CASCADING_LENGTH; i++) {
					wid[i] = second_row->tid[i];
				}
				
				second_row->mutx = 0;
				// second_row->tid = get_txn_id();
				for (int i = 0; i < CASCADING_LENGTH; i++) {
					if (second_row->tid[i]==0) {second_row->tid[i] = get_txn_id(); break;}
				}
				second_row->wts = ts, second_row->rts = ts;
				uint64_t operate_size = row_t::get_row_size(second_row->tuple_size);
				assert(write_remote_row(loc, operate_size, mutx_offset,(char*)(second_row))==true);
				mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			}
		} else {
			assert(false);
		}
	end:
		rc = RCOK;
		rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc,wid);
		mem_allocator.free(wid, sizeof(uint64_t)*CASCADING_LENGTH);
		return rc;
	#endif	

	#if CC_ALG == RDMA_TS	
		ts_t ts = get_timestamp();
		uint64_t offset = m_item->offset;
		uint64_t lock_num = get_txn_id() + 1;
	
		uint64_t try_lock;
		int retry = 0;
		retry_read:
		#if USE_DBPAOR == true
			row_t* remote_row = cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
			if(try_lock!=0){ //lock fail
				rc = Abort;
				mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
		#else
			try_lock = cas_remote_content(yield,loc,offset,0,lock_num,cor_id);
			if(try_lock!=0){ //lock fail
				rc = Abort;
				return rc;
			}
			row_t *remote_row = read_remote_row(yield,loc,offset,cor_id);
		#endif
		row_t *_row = remote_row;
		uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
		uint64_t wid = 0;
		if(type == RD) {
			if (ts < _row->wts) {
				rc = Abort;
				_row->mutx = 0;
				assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
				mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
			bool need_wait = false;
			if (_row->ur_size > 0)
				for (int i = 0; i < WAIT_QUEUE_LENGTH; i++) {
					if (_row->up[i].ts_ != 0 && !_row->up[i].commit_ && ts > _row->up[i].ts_) {
						need_wait = true; 
						printf("[need wait]current_txn:%ld,ts:%lu,wait for txn:%ld, ts:%lu, up size %ld\n",get_txn_id(),ts,_row->up[i].txn_id_,_row->up[i].ts_, _row->up_size);
						break;
					}
				}
			if (need_wait) {
				_row->mutx = 0;
				if (USE_READ_WAIT_QUEUE) {
					rdma_txn_table.local_set_state(get_thd_id(), get_txn_id(), TS_WAITING);
					if (_row->ur_size == WAIT_QUEUE_LENGTH) {
						rc = Abort;
						INC_STATS(get_thd_id(),read_retry_cnt,1);
						return rc;
					}
					else {
						int i = 0;
						for (i = 0; i < WAIT_QUEUE_LENGTH; i++) {
							if (_row->ur[i].ts_ == 0 && _row->ur[i].txn_id_ == 0) {
								break;
							}
						}
						_row->ur[i].ts_ = ts;
						_row->ur[i].txn_id_ = get_txn_id();
						_row->ur_size++;
						_row->mutx = 0;
						assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
						mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
						// printf("[waiting]current_txn:%ld,retry:%ld\n",get_txn_id(),retry);
						
						uint64_t starttime = get_sys_clock();
						rdma_txn_table.local_set_state(get_thd_id(), get_txn_id(),TS_WAITING);
					#if USE_COROUTINE && YIELD_WHEN_WAITING_READ
						uint64_t waitcomp_time;
						std::pair<int,ibv_wc> dbres1;
						INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
						while (!simulation->is_done() && rdma_txn_table.local_get_state(get_thd_id(), get_txn_id()) == TS_WAITING) {
							h_thd->start_wait_time = get_sys_clock();
							h_thd->last_yield_time = get_sys_clock();
							// printf("do\n");
							yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
							uint64_t yield_endtime = get_sys_clock();
							INC_STATS(get_thd_id(), worker_yield_cnt, 1);
							INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
							INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
							waitcomp_time = get_sys_clock();
							
							INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
							INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
						}
						h_thd->cor_process_starttime[cor_id] = get_sys_clock();
					#else
						while (!simulation->is_done() && rdma_txn_table.local_get_state(get_thd_id(), get_txn_id()) == TS_WAITING);
						uint64_t endtime = get_sys_clock();
						INC_STATS(get_thd_id(), worker_idle_time, endtime - starttime);
						INC_STATS(get_thd_id(), worker_waitcomp_time, endtime - starttime);
					#endif
						goto retry_read;
						// printf("[leave waiting]current_txn:%ld, wait_time:%ld\n",get_txn_id(),endtime-starttime);
						// 
					}
				} else {
					_row->mutx = 0;
					assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
					if (!simulation->is_done() && retry < TS_RETRY_COUNT){
						retry++;
						goto retry_read;
					} else {
						rc = Abort;
						INC_STATS(get_thd_id(),read_retry_cnt,1);
						return Abort;
					}
				}
				
				// goto retry_read;
			} else if (_row->rts < ts){
				_row->rts = ts;
			}
			_row->mutx = 0;
			rc = RCOK;
			cur_row->copy(_row);
			assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
		}
		else if(type == WR) {
			if (ts < _row->rts) {
				rc = Abort;
				_row->mutx = 0;
				assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
				mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
			if (ts < _row->wts) {
				rc = RCOK;
				_row->mutx = 0;
				assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
				goto end;
			}

			if (_row->up_size == WAIT_QUEUE_LENGTH) {
				_row->mutx = 0;
				INC_STATS(get_thd_id(),write_retry_cnt,1);
				rc = Abort;
				assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
				mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			} 
			else {
				int i = 0;
				for (i = 0; i < WAIT_QUEUE_LENGTH; i++) {
					if (_row->up[i].ts_ == 0 && _row->up[i].txn_id_ == 0) {
						break;
					}
				}
				_row->up[i].ts_ = ts;
				_row->up[i].commit_ = false;
				_row->up[i].txn_id_ = get_txn_id();
				_row->up_size++;
				_row->mutx = 0;
				cur_row->copy(_row);
				rc = RCOK;
				assert(write_remote_row(yield,loc,operate_size,offset,(char*)remote_row,cor_id) == true);
			}
		} else {
			assert(false);
		}
		end:
		rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
		// mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		return rc;
	#endif

	#if CC_ALG == RDMA_MAAT
		uint64_t lock = get_txn_id() + 1;
		ts_t ts = get_timestamp();
		uint64_t try_lock = -1;
		#if USE_DBPAOR == true
			row_t * remote_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,0,lock,cor_id);
			if(try_lock != 0) {
				rc = Abort;
				mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}	
		#else
			try_lock = cas_remote_content(yield,loc,m_item->offset,0,lock,cor_id);
			if(try_lock != 0) {
				rc = Abort;
				return rc;
			}
			row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		#endif
		// assert(remote_row->get_primary_key() == req->key);

		if(type == RD || WORKLOAD == TPCC) {
			if(remote_row->ucreads_len >= row_set_length - 1 ) {
				try_lock = cas_remote_content(yield,loc,m_item->offset,lock,0,cor_id);
				mem_allocator.free(m_item, sizeof(itemid_t));
				INC_STATS(get_thd_id(), maat_case6_cnt, 1);
				return Abort;
			}
			for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucwrites_len; i++) {
				if(remote_row->uncommitted_writes[i] == 0) {
					continue;
				} else {
					uncommitted_writes.insert(remote_row->uncommitted_writes[i]);
					j++;
				}	
			}
			if(greatest_write_timestamp < remote_row->timestamp_last_write) {
				greatest_write_timestamp = remote_row->timestamp_last_write;
			}
			for(uint64_t i = 0; i < row_set_length; i++) {
				if(remote_row->uncommitted_reads[i] == get_txn_id() || unread_set.find(remote_row->get_primary_key()) != unread_set.end()) {
					break;
				}
				if(remote_row->uncommitted_reads[i] == 0) {
					remote_row->ucreads_len += 1;
					unread_set.insert(remote_row->get_primary_key());
					remote_row->uncommitted_reads[i] = get_txn_id();
					break;
				}
			}
		}
		if(type == WR || WORKLOAD == TPCC) {
			if(remote_row->ucwrites_len >= row_set_length - 1 ) {
				try_lock = cas_remote_content(yield,loc,m_item->offset,lock,0,cor_id);
				mem_allocator.free(m_item, sizeof(itemid_t));
				INC_STATS(get_thd_id(), maat_case6_cnt, 1);
				return Abort;
			}
			for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucreads_len; i++) {
				if(remote_row->uncommitted_reads[i] == 0) {
					continue;
				} else {
					uncommitted_reads.insert(remote_row->uncommitted_reads[i]);
					j++;
				}
			}
			if(greatest_write_timestamp < remote_row->timestamp_last_write) {
				greatest_write_timestamp = remote_row->timestamp_last_write;
			}
			if(greatest_read_timestamp < remote_row->timestamp_last_read) {
				greatest_read_timestamp = remote_row->timestamp_last_read;
			}
			bool in_set = false;
			for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucwrites_len; i++) {
				if(remote_row->uncommitted_writes[i] == get_txn_id() || unwrite_set.find(remote_row->get_primary_key()) != unwrite_set.end()) {
					in_set = true;
					continue;
				}
				if(remote_row->uncommitted_writes[i] == 0 && in_set == false) {
					remote_row->ucwrites_len += 1;
					unwrite_set.insert(remote_row->get_primary_key());
					remote_row->uncommitted_writes[i] = get_txn_id();
					in_set = true;
					j++;
					continue;
				}
				if(remote_row->uncommitted_writes[i] != 0) {
					uncommitted_writes_y.insert(remote_row->uncommitted_writes[i]);
					j++;
				}
			}
		}
		remote_row->_tid_word = 0;

		uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);

		assert(write_remote_row(yield,loc,operate_size,m_item->offset,(char*)remote_row,cor_id) == true);

		if(rc == Abort) {
			mem_allocator.free(m_item, sizeof(itemid_t));
			return rc;
		}

		rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
		
		return rc;
	#endif


	#if CC_ALG == RDMA_CICADA	
		ts_t ts = get_timestamp();
		uint64_t retry_time = 0;
		row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		// assert(remote_row->get_primary_key() == req->key);
		rc = Abort;
		uint64_t version;

		if(type == RD) {
			assert(remote_row->version_cnt >= 0);
		#if 0
			uint64_t max_version = 0, max_wts = 0;
			for(int i = 0; i < HIS_CHAIN_NUM; i++) {
				if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(remote_row->cicada_version[i].Wts > this->get_timestamp()) {
					continue;
				}
				if(remote_row->cicada_version[i].Wts > max_wts) {
					max_wts = remote_row->cicada_version[i].Wts;
					max_version = remote_row->cicada_version[i].key;
				}
			}
			int i = max_version % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
					retry_time += 1;
					mem_allocator.free(remote_row, sizeof(row_t));
					remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
					// assert(remote_row->get_primary_key() == req->key);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else if (remote_row->cicada_version[i].state == Cicada_ABORTED){
						rc = Abort;
					} else {
						rc = RCOK;
						version = remote_row->cicada_version[i].key;
					}
					if(retry_time > 1) {
						rc = Abort;
					}
				}			
			} else {
				rc = RCOK;
				version = remote_row->cicada_version[i].key;
			}
		#else
		bool find = false;
			for(int cnt = remote_row->version_cnt; cnt >= remote_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(remote_row->cicada_version[i].Wts > this->get_timestamp()) {
					// printf("r large version:%d state:%d Wts:%lu: txnts:%lu\n", cnt, remote_row->cicada_version[i].state, remote_row->cicada_version[i].Wts, this->get_timestamp());
					continue;
				}
				// printf("r find version:%d state:%d Wts:%ld: txnts:%ld\n", cnt, remote_row->cicada_version[i].state, remote_row->cicada_version[i].Wts, this->get_timestamp());
				if(remote_row->cicada_version[i].state == Cicada_PENDING) {
					rc = WAIT;
					while(rc == WAIT && !simulation->is_done()) {
						retry_time += 1;
						mem_allocator.free(remote_row, sizeof(row_t));
						remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
						// assert(remote_row->get_primary_key() == req->key);

						if(remote_row->cicada_version[i].state == Cicada_PENDING) {
							rc = WAIT;
						} else if (remote_row->cicada_version[i].state == Cicada_ABORTED) {
							break;
						} else {
							rc = RCOK;
							find = true;
							version = remote_row->cicada_version[i].key;
						}
						if(retry_time > CICADA_MAX_RETRY_TIME) {
							rc = Abort;
							// printf("r find row %ld version and failed:%d state:%d Wts:%lu: txnts:%lu\n",remote_row->get_primary_key(),  cnt, remote_row->cicada_version[i].state, remote_row->cicada_version[i].Wts, this->get_timestamp());
							INC_STATS(this->get_thd_id(), cicada_case5_cnt, 1);
						}
					}				
				} else {
					rc = RCOK;
					find = true;
					version = remote_row->cicada_version[i].key;
				}	
				if (find || rc == Abort) break;
			}
		#endif
		}
		if(type == WR) {
		#if 0
			assert(remote_row->version_cnt >= 0);
			uint64_t max_version = 0, max_wts = 0;
			for(int i = 0; i < HIS_CHAIN_NUM; i++) {
				if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(remote_row->cicada_version[i].Wts > this->get_timestamp() || remote_row->cicada_version[i].Rts > this->get_timestamp()) {
					rc = Abort;
					break;
				}
				if(remote_row->cicada_version[i].Wts > max_wts) {
					max_wts = remote_row->cicada_version[i].Wts;
					max_version = remote_row->cicada_version[i].key;
				}
			}
			int i = max_version % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
					retry_time += 1;
					mem_allocator.free(remote_row, sizeof(row_t));
					remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else if (remote_row->cicada_version[i].state == Cicada_ABORTED){
						rc = Abort;
					} else {
						rc = RCOK;
						version = remote_row->cicada_version[i].key;
					}
					if(retry_time > 1) {
						rc = Abort;
					}
				}
			} else {	
				rc = RCOK;
				version = remote_row->cicada_version[i].key;
			}
		#else
			bool find = false;
			for(int cnt = remote_row->version_cnt; cnt >= remote_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(remote_row->cicada_version[i].Wts > this->get_timestamp() || remote_row->cicada_version[i].Rts > this->get_timestamp()) {
					rc = Abort;
					INC_STATS(this->get_thd_id(), cicada_case6_cnt, 1);
					break;
				}
				if(remote_row->cicada_version[i].state == Cicada_PENDING) {
					// --todo !---pendind need wait //
					rc = WAIT;
					while(rc == WAIT && !simulation->is_done()) {
						retry_time += 1;
						mem_allocator.free(remote_row, sizeof(row_t));
						remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
						// assert(remote_row->get_primary_key() == req->key);

						if(remote_row->cicada_version[i].state == Cicada_PENDING) {
							rc = WAIT;
						} else {
							version = remote_row->cicada_version[i].key;
							rc = RCOK;
							find = true;
						}
						if(retry_time > 1) {
							rc = Abort;
							INC_STATS(this->get_thd_id(), cicada_case6_cnt, 1);
						}
					}
				} else {	
					rc = RCOK;
					find = true;
					version = remote_row->cicada_version[i].key;
				}
				if (find || rc == Abort) break;
			}
		#endif
		}
		if(rc == Abort) {
			mem_allocator.free(m_item, sizeof(itemid_t));
			return rc;
		}
		this->version_num.push_back(version);
		rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
		return rc;
	#endif

    #if CC_ALG == RDMA_BAMBOO_NO_WAIT
        uint64_t lock_info,new_lock_info;
		 
        bool success_lock = false;
        uint64_t try_lock = -1;
        while(success_lock == false && !simulation->is_done()){
            if(type == RD){
                row_t * lock_read = read_remote_row(yield,loc,m_item->offset,cor_id);

			    new_lock_info = 0;
			    lock_info = lock_read->_tid_word;

			    mem_allocator.free(lock_read, row_t::get_row_size(ROW_DEFAULT_SIZE));
	
			    bool conflict = Row_rdma_bamboo::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

                if(conflict){//detect conflict
                    DEBUG_M("TxnManager::get_row(abort) access free\n");
                    row_local = NULL;
                    txn->rc = Abort;
                    return Abort;
                } 
            }else if(type == WR){
                lock_info = 0; //if lock_info!=0, CAS fail , Abort
                new_lock_info = 3; //binary 11, aka 1 read lock
            }

            //lock allow
            //CAS lock both read and write  
            try_lock = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info,cor_id);

            if(try_lock != lock_info && type == WR) {
                DEBUG_M("TxnManager::get_row(abort) access free\n");
                row_local = NULL;
                txn->rc = Abort;
                mem_allocator.free(m_item, sizeof(itemid_t));
                return Abort; //原子性被破坏，CAS失败
            }else if(try_lock != lock_info && type == RD) {
                continue;
            }else if(try_lock == lock_info){
                success_lock = true;
                break;
            }
        }

        row_t *remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        //preserve old value and dependent txn in retire
        rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
        mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

        return rc;
    #endif

    #if CC_ALG == RDMA_OPT_NO_WAIT
        uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(m_item->offset);
        uint64_t remote_rcnt_pos_pointer = row_t::get_rcnt_pos_pointer(m_item->offset);	
        uint64_t remote_conflict_pointer = m_item->offset;
        uint64_t new_lock_info = get_txn_id() + 1;
        rc = RCOK;
        lock_t lock_type = LOCK_NONE;
        row_t * test_row = NULL;
        if(type == RD || type == SCAN) {
            uint64_t try_lock = -1;
            #if USE_DBPAOR && 0
                // test_row = cas_and_read_remote(yield, try_lock,loc,remote_lock_info_pointer,remote_conflict_pointer,0,new_lock_info,cor_id);
                // if(try_lock != 0){ //CAS fail: Ignore read content 
                // 	faa_remote_content(yield,loc,remote_conflict_pointer,cor_id);
                // 	row_local = NULL;
                // 	txn->rc = Abort;
                // 	mem_allocator.free(m_item, sizeof(itemid_t));
                // 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                // 	return Abort; //原子性被破坏，CAS失败			
                // }		
            #else
                try_lock = cas_remote_content(yield,loc,remote_lock_info_pointer,0,new_lock_info,cor_id);
                if(try_lock != 0){ //cas fail 
    #if !ALL_ES_LOCK
    #if BATCH_FAA
                    local_record_faa(req->key, loc, remote_conflict_pointer);
    #else
                    faa_remote_content(yield,loc,remote_conflict_pointer,cor_id);
    #endif
    #endif 
                    row_local = NULL;
                    txn->rc = Abort;
                    mem_allocator.free(m_item, sizeof(itemid_t));
                    INC_STATS(get_thd_id(), opt_no_wait_abort5, 1);
                    return Abort; //原子性被破坏，CAS失败						
                }
                //cas success: read remote data
                test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);
            #endif
            // assert(test_row->get_primary_key() == req->key);
    #if !ALL_ES_LOCK
            if(test_row->rcnt_pos - test_row->rcnt_neg > 0){
    #if BATCH_FAA
                local_record_faa(req->key, loc, remote_conflict_pointer);
    #else
                faa_remote_content(yield,loc,remote_conflict_pointer,cor_id);
    #endif
            } 
    #endif
            if (test_row->is_hot) {
                test_row->rcnt_pos ++;
                test_row->lock_info = 0;
                lock_type = DLOCK_SH;
                uint64_t local_rcnt_pos_pointer = row_t::get_rcnt_pos_pointer((uint64_t)test_row);
                assert(write_remote_row(yield,loc,sizeof(test_row->lock_info)+sizeof(test_row->rcnt_pos),remote_rcnt_pos_pointer,(char *)local_rcnt_pos_pointer, cor_id) == true);
            } 
            else if(test_row->rcnt_pos - test_row->rcnt_neg > 0) {
                test_row->lock_info = 0;
                uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)test_row);
                assert(write_remote_row(yield,loc,sizeof(test_row->lock_info),remote_lock_info_pointer, (char*)local_lock_info_pointer, cor_id) == true);
                INC_STATS(get_thd_id(), opt_no_wait_abort6, 1);
                return Abort;
            } 
            else {
                lock_type = DLOCK_EX;
            }
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, lock_type);
            return rc;
        }else if (type == WR){
            uint64_t try_lock = -1;
        #if USE_DBPAOR == true && 0
            // test_row = cas_and_read_remote(yield, try_lock,loc,remote_lock_info_pointer,remote_conflict_pointer,0,new_lock_info,cor_id);
            // if(try_lock != 0){ //CAS fail: Ignore read content 
            // 	row_local = NULL;
            // 	txn->rc = Abort;
            // 	mem_allocator.free(m_item, sizeof(itemid_t));
            // 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
            // 	return Abort; //原子性被破坏，CAS失败			
            // }		
        #else
            try_lock = cas_remote_content(yield,loc,remote_lock_info_pointer,0,new_lock_info,cor_id);
            if(try_lock != 0){ //cas fail 
                    row_local = NULL;
                    txn->rc = Abort;
                    mem_allocator.free(m_item, sizeof(itemid_t));
                    INC_STATS(get_thd_id(), opt_no_wait_abort7, 1);
                    return Abort; //原子性被破坏，CAS失败						
            }
            //cas success: read remote data
            test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);
        #endif
            // assert(test_row->get_primary_key() == req->key);
            if(test_row->rcnt_pos - test_row->rcnt_neg > 0) {
                test_row->lock_info = 0;
                uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)test_row);
                // faa_remote_content(yield,loc,remote_conflict_pointer,cor_id);
                assert(write_remote_row(yield,loc,sizeof(uint64_t), remote_lock_info_pointer, (char *)local_lock_info_pointer, cor_id) == true);
                INC_STATS(get_thd_id(), opt_no_wait_abort8, 1);
                return Abort;
            } else {
                lock_type = DLOCK_EX;
            }
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, lock_type);
            return rc;
        }
    #endif

    #if CC_ALG == RDMA_OPT_WAIT_DIE
    uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(m_item->offset);
    uint64_t remote_rcnt_pos_pointer = row_t::get_rcnt_pos_pointer(m_item->offset);	
    uint64_t remote_conflict_pointer = m_item->offset;
	uint64_t tts = get_timestamp();
    lock_t lock_type = LOCK_NONE;

	if(type == RD || type == SCAN) {
		uint64_t try_lock = -1;
		row_t* test_row;
read_wait_here:
#if USE_DBPAOR
		test_row = cas_and_read_remote(yield,try_lock,loc,remote_lock_info_pointer,remote_conflict_pointer, 0, 1, cor_id);
#else
		try_lock = cas_remote_content(yield,loc,remote_lock_info_pointer,0,1,cor_id);
		test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);
#endif
		// assert(test_row->get_primary_key() == req->key);
		if(try_lock > 1){ //already locked by write
			//wait or abort
			if(tts < try_lock && !simulation->is_done()){ //wait
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				goto read_wait_here;
			}else{ //abort
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;				
			}
		}else if(try_lock == 1){ //already locked by read
#if !ALL_ES_LOCK
#if BATCH_FAA
			local_record_faa(req->key, loc, remote_conflict_pointer);
#else
			faa_remote_content(yield,loc,remote_conflict_pointer,cor_id);
#endif
#endif
			// if(test_row->is_hot){ //wait
			// 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			// 	goto read_wait_here;
			// }else{ 

			//find the minimum ts that is currently holding the lock
			uint64_t min_lock_ts = 0;
			bool find_one = false;
			for(int i=0;i<LOCK_LENGTH;i++){
				if(!find_one && test_row->ts[i]!=0){
					find_one = true;
					min_lock_ts = test_row->ts[i];
				}
				if(test_row->ts[i]!=0 && test_row->ts[i] < min_lock_ts){
					min_lock_ts = test_row->ts[i];
				}
			}
			//wait or abort
			if(tts < min_lock_ts && !simulation->is_done()){ //wait
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				goto read_wait_here;
			}else{ //abort
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;					
			}
			
			// }
		}
		//cas success
   		uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)test_row);
    	uint64_t local_rcnt_pos_pointer = row_t::get_rcnt_pos_pointer((uint64_t)test_row);	
		bool ts_updated = false;
		if(test_row->is_hot){
			test_row->rcnt_pos++;
			test_row->lock_info = 0;
			int iter;
			for(iter=0;iter<LOCK_LENGTH;iter++){
				if(test_row->ts[iter]==0){
					test_row->ts[iter] = tts;
					break;
				}
			}
			if(iter == LOCK_LENGTH){ //no empty place in ts array
				//unlock and abort
				write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;						
			}
			uint64_t remote_ts_pointer = row_t::get_ts_pointer(m_item->offset,iter);
			uint64_t local_ts_pointer = row_t::get_ts_pointer((uint64_t)test_row,iter);
			write_remote_row(yield, loc, sizeof(test_row->ts[iter]), remote_ts_pointer,(char*)local_ts_pointer,cor_id);
			uint64_t operate_size = sizeof(test_row->lock_info) + sizeof(test_row->rcnt_pos);
			// uint64_t operate_size = sizeof(test_row->rcnt_pos)+sizeof(test_row->lock_info) + sizeof(test_row->ts[0])*LOCK_LENGTH;
			write_remote_row(yield, loc, operate_size, remote_rcnt_pos_pointer,(char*)local_rcnt_pos_pointer,cor_id);
			ts_updated = true;
			lock_type = DLOCK_SH;
		}else if(test_row->rcnt_pos - test_row->rcnt_neg > 0){
			//find the minimum ts that is currently holding the lock
			uint64_t min_lock_ts = 0;
			bool find_one = false;
			for(int i=0;i<LOCK_LENGTH;i++){
				if(!find_one && test_row->ts[i]!=0){
					find_one = true;
					min_lock_ts = test_row->ts[i];
				}
				if(test_row->ts[i]!=0 && test_row->ts[i] < min_lock_ts){
					min_lock_ts = test_row->ts[i];
				}
			}
			//wait or abort
			if(tts < min_lock_ts){ //wait
				while(test_row->rcnt_pos - test_row->rcnt_neg > 0 && !simulation->is_done()){
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);				
				}
				if(test_row->rcnt_pos - test_row->rcnt_neg > 0){ //simulation is done
					//unlock and abort
					test_row->lock_info = 0;
					write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
					row_local = NULL;
					txn->rc = Abort;
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					return Abort;	
				}
				//now : test_row->rcnt_pos - test_row->rcnt_neg = 0
			}else{ //unlock and abort
				test_row->lock_info = 0;
				write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;						
			}
		}
		//write ts array
		if(!ts_updated){
			int iter;
			for(iter=0;iter<LOCK_LENGTH;iter++){
				if(test_row->ts[iter]==0){
					test_row->ts[iter] = tts;
					break;
				}
			}
			if(iter == LOCK_LENGTH){ //no empty place in ts array
				//unlock and abort
				test_row->lock_info = 0;
				write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort; 					
			}
			uint64_t remote_ts_pointer = row_t::get_ts_pointer(m_item->offset,iter);
			uint64_t local_ts_pointer = row_t::get_ts_pointer((uint64_t)test_row,iter);
			write_remote_row(yield, loc, sizeof(test_row->ts[iter]), remote_ts_pointer,(char*)local_ts_pointer,cor_id);
			lock_type = DLOCK_EX;
		}
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, lock_type);
#if DEBUG_PRINTF
		printf("---thread id: %lu, remote lock read suc, lock location: %lu; %p, txn id: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id());
#endif
		return rc;
	}else if (type == WR){
		uint64_t try_lock = -1;
		row_t* test_row;
write_wait_here:
#if USE_DBPAOR
		test_row = cas_and_read_remote(yield,try_lock,loc,remote_lock_info_pointer,remote_conflict_pointer, 0, tts, cor_id);
#else
		try_lock = cas_remote_content(yield,loc,remote_lock_info_pointer,0,tts,cor_id);
		test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);
#endif
		// assert(test_row->get_primary_key() == req->key);
		if(try_lock > 1){ //already locked by write
			//wait or abort
			if(tts < try_lock && !simulation->is_done()){ //wait
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// printf("---write wait here\n");
				goto write_wait_here;
			}else{ //abort
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;				
			}
		}else if(try_lock == 1){ //already locked by read
			// if(test_row->is_hot){ //wait
			// 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			// 	goto write_wait_here;
			// }else{ 

			//find the minimum ts that is currently holding the lock
			uint64_t min_lock_ts = 0;
			bool find_one = false;
			for(int i=0;i<LOCK_LENGTH;i++){
				if(!find_one && test_row->ts[i]!=0){
					find_one = true;
					min_lock_ts = test_row->ts[i];
				}
				if(test_row->ts[i]!=0 && test_row->ts[i] < min_lock_ts){
					min_lock_ts = test_row->ts[i];
				}
			}
			//wait or abort
			if(tts < min_lock_ts && !simulation->is_done()){ //wait
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				goto write_wait_here;
			}else{ //abort
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort; 					
			}

			// }
		}
		//cas success
		uint64_t local_lock_info_pointer = row_t::get_lock_info_pointer((uint64_t)test_row);
		if(test_row->rcnt_pos - test_row->rcnt_neg > 0){
			//find the minimum ts that is currently holding the lock
			uint64_t min_lock_ts = 0;
			bool find_one = false;
			for(int i=0;i<LOCK_LENGTH;i++){
				if(!find_one && test_row->ts[i]!=0){
					find_one = true;
					min_lock_ts = test_row->ts[i];
				}
				if(test_row->ts[i]!=0 && test_row->ts[i] < min_lock_ts){
					min_lock_ts = test_row->ts[i];
				}
			}
			//wait or abort
			if(tts < min_lock_ts && !simulation->is_done()){ 
				//unlock and wait
				test_row->lock_info = 0;
				write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				goto write_wait_here;
				// while(test_row->rcnt_pos - test_row->rcnt_neg > 0 && !simulation->is_done()){
				// 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// 	test_row = read_remote_row(yield,loc,remote_conflict_pointer,cor_id);				
				// }
				// if(test_row->rcnt_pos - test_row->rcnt_neg > 0){ //simulation is done
				// 	//unlock and abort
				// 	test_row->lock_info = 0;
				// 	write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				// 	row_local = NULL;
				// 	txn->rc = Abort;
				// 	mem_allocator.free(m_item, sizeof(itemid_t));
				// 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// 	return Abort;	
				// }
				//now : test_row->rcnt_pos - test_row->rcnt_neg = 0
			}else{ //unlock and abort
				test_row->lock_info = 0;
				write_remote_row(yield, loc, sizeof(uint64_t),remote_lock_info_pointer,(char*)local_lock_info_pointer,cor_id);
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;						
			}
		}
		lock_type = DLOCK_EX;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, lock_type);
#if DEBUG_PRINTF
		printf("---thread id: %lu, remote lock write suc, lock location: %lu; %p, txn id: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id());
#endif		
		return rc;
	}
    #endif

    #if CC_ALG == RDMA_OPT_NO_WAIT2
    
    
	rc = RCOK;
    lock_t lock_def = LOCK_NONE;
    row_t * test_row = NULL;

    //for READ OP
	if(type == RD || type == SCAN) {
	    test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        uint64_t lock_info = test_row->lock_info;
        uint64_t old_lock_info = 0;

		uint64_t try_lock = -1;
        bool succ_lock = false;
        uint64_t offset = m_item->offset +sizeof(uint64_t)*2;
        uint64_t new_lock_info = 0; 

        if(test_row->is_hot){
            while(!succ_lock && !simulation->is_done()){
                uint64_t read_lock_cnt = row_t::decode_lock_info_cnt(lock_info);
                uint64_t lock_type = row_t::decode_lock_info_type(lock_info);
                if(lock_type == 1 || lock_type == 2){
                    if(lock_type == 1)faa_remote_content(yield,loc,m_item->offset,1,cor_id);
                    mem_allocator.free(m_item, sizeof(itemid_t));
				    mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                    INC_STATS(get_thd_id(), opt_no_wait_abort1, 1);

                    return Abort;
                }

                if(lock_type == 0){
                    new_lock_info = 7; //TODO - chcek 0000...001 11
                    old_lock_info = 0;
                }else if(lock_type == 3){
                    new_lock_info = (read_lock_cnt + 1)<<2; 
                    new_lock_info = (new_lock_info | 0X03); //TODO - chcek 0000...000 11
                    old_lock_info = lock_info;
                }
                try_lock = cas_remote_content(yield,loc,offset,old_lock_info,new_lock_info,cor_id);
                if(try_lock == old_lock_info){
                    succ_lock = true;
                }else{
                    lock_info = try_lock;
                }
            }

            faa_remote_content(yield,loc,m_item->offset,1,cor_id);
	        test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, DLOCK_SH);
		    return rc; 
        }else{
            uint64_t read_lock_cnt = row_t::decode_lock_info_cnt(lock_info);
            uint64_t lock_type = row_t::decode_lock_info_type(lock_info);
            if(lock_type != 0){
                if(lock_type == 3 || lock_type == 2)faa_remote_content(yield,loc,m_item->offset,1,cor_id);
                mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                INC_STATS(get_thd_id(), opt_no_wait_abort2, 1);
                return Abort;
            }else{
                uint64_t new_lock_info = 2;//TODO - 0...0 010
                uint64_t offset = m_item->offset +sizeof(uint64_t)*2;
                try_lock = cas_remote_content(yield,loc,offset,0,new_lock_info,cor_id);

                if(try_lock != 0){
                    mem_allocator.free(m_item, sizeof(itemid_t));
				    mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                    INC_STATS(get_thd_id(), opt_no_wait_abort3, 1);
                   return Abort;
                }

                test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
                rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, DLOCK_EX);
		        return rc; 
            }
			
        }
	}else if (type == WR){
		uint64_t try_lock = -1;
        uint64_t offset = m_item->offset +sizeof(uint64_t)*2;
        try_lock = cas_remote_content(yield,loc,offset,0,1,cor_id);
        if(try_lock != 0){
            mem_allocator.free(m_item, sizeof(itemid_t));
            INC_STATS(get_thd_id(), opt_no_wait_abort4, 1);
            return Abort;
        }
        
        test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        // printf("[txn.cpp:3648]remote write lock key = %ld\n",test_row->get_primary_key());

        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc, DLOCK_EX);
		return rc;
	}
    #endif

    #if CC_ALG == RDMA_OPT_NO_WAIT3 

	rc = RCOK;
    lock_t lock_type = LOCK_NONE;
	row_t * test_row = NULL;

	if(type == RD || type == SCAN) {
        //range lock(IS)
        // uint64_t range_lock = m_item->range_lock;
		// if (is_lock_content(range_lock)) {
		// 	return Abort;//no X lock
		// }
        // uint64_t faa_num = 1<<48; //add IS lock
        // uint64_t faa_result = 0;
		// faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
		// if (decode_x_lock(faa_result) > 0) {
        //     faa_num = (-1)<<48;
        //     faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
        //     // printf("[txn.cpp:3815]txn %ld lock failed, has X lock\n", get_txn_id());
        //     return Abort;
        // }

        uint64_t range_lock;
        uint64_t faa_num,faa_result = 0;
        //data lock(S)
		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        uint64_t data_lock = test_row->_tid_word;
        if(s_lock_content(data_lock)){
            // faa_num = (-1)<<48;
            // faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
            // printf("[txn.cpp:3824] txn %ld lock failed, has X?%d lock, has IX?%d lock\n", get_txn_id(), decode_x_lock(data_lock), decode_ix_lock(data_lock));
            return Abort;
        }

        faa_num = 1<<16;
        faa_result = faa_remote_content(yield,loc,m_item->offset,faa_num,cor_id);
        if(s_lock_content(faa_result)){
            faa_num = (-1)<<16;
            faa_result = faa_remote_content(yield,loc,m_item->offset,faa_num,cor_id);
            return Abort;
        }

		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);

        int tmp = txn->locked_range_num;
        txn->range_node_set[tmp] = m_item->leaf_node_offset;
        txn->server_set[tmp] =loc;
        txn->locked_range_num = txn->locked_range_num + 1; 
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		return rc;
	}else if (type == WR){
		//range lock(IX)
        uint64_t range_lock = m_item->range_lock;
        if(ix_lock_content(range_lock))return Abort;
        uint64_t faa_num = 1<<32; // IX
        uint64_t faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
        if(ix_lock_content(faa_result)){
            faa_num = (-1)<<32;
            faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
            // printf("[txn.cpp:3852] txn %ld lock failed, has X lock\n", get_txn_id());
            return Abort;
        }

        //data lock(X)
		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        uint64_t data_lock = test_row->_tid_word;
        if(data_lock != 0){
            faa_num = (-1)<<48;
            faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
			// printf("[txn.cpp:3862] txn %ld lock failed, has X?%d lock, has IX?%d lock, has S?%d lock, has IS%d lock\n", get_txn_id(), decode_x_lock(data_lock),decode_ix_lock(data_lock),decode_s_lock(data_lock),decode_is_lock(data_lock));
            return Abort;
        }

        uint64_t try_lock = -1;
        try_lock = cas_remote_content(yield,loc,m_item->offset,0,1,cor_id);
        if(try_lock != 0){
            faa_num = (-1)<<48;
            faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
            // printf("[txn.cpp:3874] txn %ld lock failed, has X?%d lock, has IX?%d lock, has S?%d lock, has IS%d lock\n", get_txn_id(),decode_x_lock(try_lock),decode_ix_lock(try_lock),decode_s_lock(try_lock),decode_is_lock(try_lock));
            return Abort;
        }
        int tmp = txn->locked_range_num;
        txn->range_node_set[tmp] = m_item->leaf_node_offset;
        txn->server_set[tmp] =loc;
        txn->locked_range_num = txn->locked_range_num + 1; 

		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		return rc;
	}
    #endif

    #if CC_ALG == RDMA_DOUBLE_RANGE_LOCK 
      	rc = RCOK;
        lock_t lock_type = LOCK_NONE;
	    row_t * test_row = NULL;

	    if(type == RD) {
            
            //data lock(S)
            uint64_t faa_num;
            uint64_t faa_result = -1;
            // test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
            // uint64_t data_lock = test_row->_tid_word;
            // if(data_lock == 1){
            //     return Abort;
            // }

            faa_num = 1<<1;
            faa_result = faa_remote_content(yield,loc,m_item->offset,faa_num,cor_id);
            //TODO - check
            if(faa_result == 1){
                faa_num = (-1)<<1;
                faa_result = faa_remote_content(yield,loc,m_item->offset,faa_num,cor_id);
                return Abort;
            }

		    test_row = read_remote_row(yield,loc,m_item->offset,cor_id);

            int tmp = txn->locked_range_num;
            txn->range_node_set[tmp] = m_item->leaf_node_offset;
            txn->server_set[tmp] =loc;
            txn->locked_range_num = txn->locked_range_num + 1; 
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
            return rc;
	    }else if (type == WR){
        
            // printf("[txn.cpp:3932]\n");
            uint64_t range_lock = m_item->range_lock;
            uint64_t cas_result = -1;

            //data lock(X)
            cas_result = cas_remote_content(yield,loc,m_item->offset,0,1,cor_id);
            if(cas_result != 0){
                return Abort;
            }

            // int tmp = txn->locked_range_num;
            // txn->range_node_set[tmp] = m_item->leaf_node_offset;
            // txn->server_set[tmp] =loc;
            // txn->locked_range_num = txn->locked_range_num + 1; 

            test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
            
            return rc;
	    }
    #endif

    #if CC_ALG == RDMA_SINGLE_RANGE_LOCK
      	rc = RCOK;
        lock_t lock_type = LOCK_NONE;
	    row_t * test_row = NULL;
        rdma_bt_node * remote_leaf = NULL;

	    if(type == RD) {
            
            //range lock(S)
            uint64_t faa_num;
            uint64_t faa_result = -1;
            uint64_t leaf_lock = m_item->range_lock;
            if(leaf_lock == 1){
                return Abort;
            }

            faa_num = 1<<1;
            faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
            //TODO - check
            if(faa_result == 1){
                faa_num = (-1)<<1;
                faa_result = faa_remote_content(yield,loc,m_item->leaf_node_offset,faa_num,cor_id);
                return Abort;
            }

		    test_row = read_remote_row(yield,loc,m_item->offset,cor_id);

            int tmp = txn->locked_range_num;
            txn->range_node_set[tmp] = m_item->leaf_node_offset;
            txn->server_set[tmp] =loc;
            txn->locked_range_num = txn->locked_range_num + 1; 
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
            return rc;
	    }else if (type == WR){
            //range lock(X)
            // printf("[txn.cpp:3932]\n");
            uint64_t range_lock = m_item->range_lock;
            if(range_lock != 0)return Abort;
            uint64_t faa_num = 1; // X
            uint64_t cas_result = 1;
            cas_result = cas_remote_content(yield,loc,m_item->leaf_node_offset,0,1,cor_id);
            if(cas_result != 0){
                return Abort;
            }
          
            int tmp = txn->locked_range_num;
            txn->range_node_set[tmp] = m_item->leaf_node_offset;
            txn->server_set[tmp] =loc;
            txn->locked_range_num = txn->locked_range_num + 1; 

            test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
            rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
          
            return rc;
	    }
    #endif

    #if CC_ALG == RDMA_MIX_RANGE_LOCK

    #endif
}
 
RC TxnManager::get_continuous_row(yield_func_t &yield,uint64_t cor_id,itemid_t * m_item,uint64_t first_key,uint64_t last_key){
    bt_node * leaf = (bt_node *)(m_item->parent);
    RC rc = leaf->get_range_lock(m_item);
    if(rc == Abort){
        // printf("[txn.cpp:3921]get lock fail\n");
        return Abort;
    }
    else{
        record_intent_lock *record = (record_intent_lock*)mem_allocator.alloc(sizeof(record_intent_lock));
        // record->bt_node_location = leaf;
        // record->server_id = g_node_id;
        // txn->locked_node.add(record);
        preserve_continuous_access(m_item,first_key,last_key);
    }
    leaf = leaf->next;
    if(leaf == NULL)return RCOK;
    itemid_t *new_m_item = (itemid_t *)malloc(sizeof(itemid_t));
    new_m_item->parent = leaf;
    while(leaf->keys[0] >= first_key && leaf->keys[0] <= last_key){
        rc = leaf->get_range_lock(new_m_item);
        if(rc == Abort){
            // printf("[txn.cpp:3938]get lock fail\n");
            return Abort;
        }
        else{
            record_intent_lock *record = (record_intent_lock*)mem_allocator.alloc(sizeof(record_intent_lock));
            // record->bt_node_location = leaf;
            // record->server_id = g_node_id;
            // txn->locked_node.add(record);
            
            preserve_continuous_access(new_m_item,first_key,last_key);
        }
        leaf = leaf->next;
        new_m_item->parent = leaf;
        if(leaf == NULL)break;
    }
    mem_allocator.free(new_m_item, sizeof(itemid_t));

    return RCOK;
}

RC TxnManager::preserve_continuous_access(itemid_t *m_item,uint64_t first_key,uint64_t last_key){
    bt_node * leaf = (bt_node *)(m_item->parent);
    for(int i = 0; i < leaf->num_keys; i++){
        if(leaf->keys[i] > first_key && leaf->keys[i] < last_key&&(txn->accesses.get_count()<g_req_per_query)){
            RC rc = RCOK;
            row_t * read_row = (row_t *)(leaf->pointers[i]);
	        Access * access = NULL;
            
	        this->last_row = read_row;
            access_pool.get(get_thd_id(),access);

            row_t * newr = (row_t *) mem_allocator.alloc(row_t::get_row_size(read_row->get_tuple_size()));
		    newr->init(read_row->get_table(), read_row->get_part_id());
            // newr->copy(read_row);
            access->data = newr; 
            access->type = RD;
            access->orig_row = read_row;
            ++txn->row_cnt;
            txn->accesses.add(access);
        }
    }
    record_intent_lock * record_node = NULL;
    locked_node_pool.get(get_thd_id(),record_node);

    record_node->bt_node_location = m_item->parent;
    record_node->server_id = g_node_id;
    txn->locked_node.add(record_node);
}

// This function is useless
void TxnManager::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
	txn->insert_rows.add(row);
}

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item = NULL;
	RC rc = RCOK;
#if INDEX_STRUCT != IDX_BTREE
	rc = index->index_read(key, item, part_id, get_thd_id());
#else
    rc = index->index_read(key, item, get_thd_id(),(uint64_t)part_id);
#endif
	if (rc == Abort) item = NULL;
	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK ||  CC_ALG == RDMA_MIX_RANGE_LOCK
rdma_bt_node *TxnManager::index_node_read(INDEX *index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	rdma_bt_node * leaf_node = NULL;
	RC rc = RCOK;
#if !DYNAMIC_WORKLOAD
	rc = index->index_node_read(key, leaf_node, part_id, get_thd_id());
#else
	rc = index->index_node_read((double)key, leaf_node, part_id, get_thd_id());
	// index->index_node_read(key, leaf_node, part_id, get_thd_id());
#endif
	if (rc == Abort) {
        printf("[txn.cpp:4134]connot find key=%lf\n",(double)key);
        leaf_node = NULL;}
	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return leaf_node;
}

rdma_bt_node *TxnManager::find_index_node_to_insert(INDEX *index, double key, int part_id) {
	uint64_t starttime = get_sys_clock();

	rdma_bt_node * leaf_node = NULL;
	RC rc = RCOK;
#if DYNAMIC_WORKLOAD
	rc = index->find_index_node_to_insert(key, leaf_node, part_id, get_thd_id());
	// index->index_node_read(key, leaf_node, part_id, get_thd_id());
#endif
	if (rc == Abort) leaf_node = NULL;
	uint64_t t = get_sys_clock() - starttime;
	//txn_time_idx += t;

	return leaf_node;
}

LeafIndexInfo *TxnManager::learn_index_node_read(INDEX *index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	LeafIndexInfo * leaf_node;
	index->learn_index_node_read(key, leaf_node, part_id, get_thd_id());

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return leaf_node;
}
#endif

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id, int count) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
#if INDEX_STRUCT != IDX_BTREE
	index->index_read(key, count, item, part_id);
#else
	index->index_read(key, item, get_thd_id(),(int64_t)part_id);
#endif
	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

RC TxnManager::validate(yield_func_t &yield, uint64_t cor_id) {
#if MODE != NORMAL_MODE
	return RCOK;
#endif
	if (CC_ALG != OCC && CC_ALG != MAAT  && CC_ALG != WOOKONG &&
			CC_ALG != TICTOC && CC_ALG != BOCC && CC_ALG != FOCC && CC_ALG != WSI &&
			CC_ALG != SSI && CC_ALG != DLI_BASE && CC_ALG != DLI_OCC &&
			CC_ALG != DLI_MVCC_OCC && CC_ALG != DTA && CC_ALG != DLI_DTA &&
			CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3 && CC_ALG != DLI_MVCC && CC_ALG != SILO &&
			CC_ALG != RDMA_SILO && CC_ALG != RDMA_MVCC && CC_ALG != RDMA_MAAT && 
			CC_ALG != RDMA_CICADA && CC_ALG != CICADA && CC_ALG != RDMA_MOCC) {
		return RCOK; //no validate in NO_WAIT
	}
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	if (CC_ALG == OCC && rc == RCOK) rc = occ_man.validate(this);
	if(CC_ALG == BOCC && rc == RCOK) rc = bocc_man.validate(this);
	if(CC_ALG == FOCC && rc == RCOK) rc = focc_man.validate(this);
	if(CC_ALG == SSI && rc == RCOK) rc = ssi_man.validate(this);
	if(CC_ALG == WSI && rc == RCOK) rc = wsi_man.validate(this);
	if(CC_ALG == CICADA && rc == RCOK) rc = cicada_man.validate(this);
	if(CC_ALG == MAAT  && rc == RCOK) {
		rc = maat_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = maat_man.find_bound(this);
		}
	}
#if CC_ALG == RDMA_MAAT

	if(CC_ALG == RDMA_MAAT && rc == RCOK) {
		rc = rmaat_man.validate(yield, this, cor_id);
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = rmaat_man.find_bound(this);
		}
	}
#endif
	if(CC_ALG == TICTOC  && rc == RCOK) {
		rc = tictoc_man.validate(this);
		// Note: home node must be last to validate
		// if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
		//   rc = tictoc_man.find_bound(this);
		// }
	}
	if ((CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 ||
			 CC_ALG == DLI_MVCC) &&
			rc == RCOK) {
		rc = dli_man.validate(this);
		if (IS_LOCAL(get_txn_id()) && rc == RCOK) {
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
			rc = dli_man.find_bound(this);
#else
			set_commit_timestamp(glob_manager.get_ts(get_thd_id()));
#endif
		}
	}
	if(CC_ALG == WOOKONG  && rc == RCOK) {
		rc = wkdb_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = wkdb_man.find_bound(this);
		}
	}
	if ((CC_ALG == DTA) && rc == RCOK) {
		rc = dta_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
				rc = dta_man.find_bound(this);
		}
	}
#if CC_ALG == SILO
  if(CC_ALG == SILO && rc == RCOK) {
    rc = validate_silo();
	//rc = RCOK;
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif
#if CC_ALG == RDMA_SILO
  if(CC_ALG == RDMA_SILO && rc == RCOK) {
    rc = rsilo_man.validate_rdma_silo(yield, this, cor_id);
	//rc = RCOK;
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif
#if CC_ALG == RDMA_MOCC
  if(CC_ALG == RDMA_MOCC && rc == RCOK) {
    rc = rmocc_man.validate_rdma_mocc(yield, this, cor_id);
	// rc = RCOK;
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif

#if CC_ALG == RDMA_TS1
  if(CC_ALG == RDMA_SILO && rc == RCOK) {
    rc = rdmats_man.validate(yield, this, cor_id);
    // if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
    //   _cur_tid ++;
    //   commit_timestamp = _cur_tid;
    //   DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    // }
  }
#endif

#if CC_ALG == RDMA_MVCC
    //rc = rmvcc_man.lock_row(this);
    // if(CC_ALG == RDMA_MVCC && rc == RCOK){
    //      rc = rmvcc_man.validate_local(this);
    // }
	//validate done in read-write phase
	return RCOK;
#endif

#if CC_ALG == RDMA_CICADA
  if(CC_ALG == RDMA_CICADA && rc == RCOK) {
	  rc = rcicada_man.validate(yield, this, cor_id); 
  }
#endif
	INC_STATS(get_thd_id(),txn_validate_time,get_sys_clock() - starttime);
	INC_STATS(get_thd_id(),trans_validate_time,get_sys_clock() - starttime);
    INC_STATS(get_thd_id(),trans_validate_count, 1);
	return rc;
}

RC TxnManager::send_remote_reads() {
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
#if !YCSB_ABORT_MODE && WORKLOAD == YCSB
	return RCOK;
#endif
	assert(query->active_nodes.size() == g_node_cnt);
	for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
		if (i == g_node_id) continue;
	if(query->active_nodes[i] == 1) {
		DEBUG("(%ld,%ld) send_remote_read to %ld\n",get_txn_id(),get_batch_id(),i);
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), i, Message::create_message(this,RFWD));
#else
		msg_queue.enqueue(get_thd_id(),Message::create_message(this,RFWD),i);
#endif
	}
	}
	return RCOK;

}

bool TxnManager::calvin_exec_phase_done() {
	bool ready =  (phase == CALVIN_DONE) && (get_rc() != WAIT);
	if(ready) {
	DEBUG("(%ld,%ld) calvin exec phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

bool TxnManager::calvin_collect_phase_done() {
	bool ready =  (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == calvin_expected_rsp_cnt);
	if(ready) {
	DEBUG("(%ld,%ld) calvin collect phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

void TxnManager::release_locks(yield_func_t &yield, RC rc, uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t endtime;
	cleanup(yield, rc, cor_id);

	uint64_t timespan = (get_sys_clock() - starttime);
	INC_STATS(get_thd_id(), txn_cleanup_time,  timespan);
}
#if USE_DBPAOR == true
row_t * TxnManager::cas_and_read_remote(yield_func_t &yield, uint64_t& try_lock, uint64_t target_server, uint64_t cas_offset, uint64_t read_offset, uint64_t compare, uint64_t swap, uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	uint64_t *local_buf1 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	char *local_buf2 = Rdma::get_row_client_memory(thd_id,2);
	uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

	uint64_t starttime = get_sys_clock(), endtime;

	DBrequests dbreq(2);
	dbreq.init();
	dbreq.set_atomic_meta(0,compare,swap,local_buf1,(uint64_t)(remote_mr_attr[target_server].buf + cas_offset));
	dbreq.set_rdma_meta(1, IBV_WR_RDMA_READ, read_size, local_buf2, (uint64_t)(remote_mr_attr[target_server].buf + read_offset));
	auto dbres = dbreq.post_reqs(rc_qp[target_server][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
    //to do: add coroutine
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> dbres1;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		dbres1 = rc_qp[target_server][get_thd_id() + cor_id * g_total_thread_cnt]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (dbres1.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
	auto dbres1 = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(dbres1 == IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	try_lock = *local_buf1;
	row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    memcpy(test_row, local_buf2, read_size);
    return test_row;
}

row_t * TxnManager::faa_and_read_remote(yield_func_t &yield, uint64_t target_server, uint64_t faa_offset, uint64_t read_offset, uint64_t add,  uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	uint64_t *local_buf1 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	char *local_buf2 = Rdma::get_row_client_memory(thd_id,2);
    // printf("[txn.cpp:3953]local_buf1 = %d, local_buf2 = %d\n",local_buf1,local_buf2);
	uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

	uint64_t starttime = get_sys_clock(), endtime;

	DBrequests dbreq(2);
	dbreq.init();
	dbreq.set_faa_meta(0,add,local_buf1,(uint64_t)(remote_mr_attr[target_server].buf + faa_offset));
	dbreq.set_rdma_meta(1, IBV_WR_RDMA_READ, read_size, local_buf2, (uint64_t)(remote_mr_attr[target_server].buf + read_offset));
	auto dbres = dbreq.post_reqs(rc_qp[target_server][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
    //to do: add coroutine
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> dbres1;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		dbres1 = rc_qp[target_server][get_thd_id() + cor_id * g_total_thread_cnt]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (dbres1.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
	auto dbres1 = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(dbres1 == IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    memcpy(test_row, local_buf2, read_size);
    return test_row;
}

#if CC_ALG == RDMA_OPT_NO_WAIT2 || CC_ALG == RDMA_OPT_NO_WAIT
void TxnManager::opt_batch_unlock_remote(yield_func_t &yield, uint64_t cor_id, int loc, RC rc, TxnManager * txnMng , vector<vector<uint64_t>>& remote_index_origin, ts_t time){
	// vector<uint64_t> remote_access_noorder = remote_index_origin[loc];
	
	int count = 0;
	if(loc>=1){
		for(int i=0;i<loc;i++){
			count += remote_index_origin[i].size();
		}
	}

    DBrequests dbreq(remote_index_origin[loc].size());   
    uint64_t thd_id = txnMng->get_thd_id()  + cor_id * g_total_thread_cnt;
    dbreq.init(); 
    vector<uint64_t> remote_need_cas,remote_access;

    // for(int i=0;i<remote_access_noorder.size();i++){
    //     Access *access = txnMng->txn->accesses[remote_access_noorder[i]];
    //     if(access->type == WR){
    //         remote_access.push_back(remote_access_noorder[i]);
    //     }
    //     else remote_access.insert(remote_access.begin(),remote_access_noorder[i]);
    // } //sort: read before write

    for(int i=0; i<remote_index_origin[loc].size();i++){
        Access *access = txnMng->txn->accesses[remote_index_origin[loc][i]];
        uint64_t off = access->offset;
        uint64_t operate_size = 0;

        if(access->type == WR){
            row_t *data = access->data;
            data->lock_info = 0;
            char* local_lock_info_pointer =(char*)data + sizeof(uint64_t)*4;
			// row_t::get_lock_info_pointer(data);
            uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(access->offset);    
            operate_size = sizeof(uint64_t);

            char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
            memcpy(local_buf, local_lock_info_pointer, operate_size);

            dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,local_buf,remote_mr_attr[loc].buf+remote_lock_info_pointer);
        }
        else{//read
            operate_size = sizeof(uint64_t);
            lock_t lock_type = access->lock_type;
            uint64_t remote_lock_info_pointer = row_t::get_lock_info_pointer(access->offset);    
            if (lock_type == DLOCK_EX) {
                uint64_t* new_lock = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
                *new_lock= 0;
                char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
                memcpy(local_buf, (char*)new_lock, operate_size);
                mem_allocator.free(new_lock, sizeof(uint64_t));

                dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,local_buf,remote_mr_attr[loc].buf+remote_lock_info_pointer);
            } else if (lock_type == DLOCK_SH){
				//TODO
				uint64_t remote_rcnt_neg_pointer = row_t::get_rcnt_neg_pointer(access->offset);
				uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);

				dbreq.set_faa_meta(i,1,local_buf,remote_mr_attr[loc].buf+remote_rcnt_neg_pointer);         
            }
        }
    }
    auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);
	RDMA_ASSERT(dbres == IOCode::Ok);

#if 0
    auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
    RDMA_ASSERT(dbres1 == IOCode::Ok);       
#endif


}
#endif

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_SILO || CC_ALG == RDMA_TS1 || CC_ALG == RDMA_MVCC || CC_ALG == RDMA_CICADA || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_DSLR_NO_WAIT
void TxnManager::batch_unlock_remote(yield_func_t &yield, uint64_t cor_id, int loc, RC rc, TxnManager * txnMng , vector<vector<uint64_t>> remote_index_origin, ts_t time,vector<vector<uint64_t>> remote_num){
	vector<uint64_t> remote_access_noorder = remote_index_origin[loc];
	//when USE_OR==true, use count to avoid overlap write
	int count = 0;
	if(loc>=1){
		for(int i=0;i<loc;i++){
			count += remote_index_origin[i].size();
		}
	}
    DBrequests dbreq(remote_access_noorder.size());   
    uint64_t thd_id = txnMng->get_thd_id()  + cor_id * g_total_thread_cnt;
    dbreq.init(); 
    vector<uint64_t> remote_need_cas,remote_access;
    for(int i=0;i<remote_access_noorder.size();i++){
        Access *access = txnMng->txn->accesses[remote_access_noorder[i]];
        if(access->type == WR){
            remote_access.push_back(remote_access_noorder[i]);
        }
        else remote_access.insert(remote_access.begin(),remote_access_noorder[i]);
    } //sort: read before write
    for(int i=0; i<remote_access.size();i++){
        Access *access = txnMng->txn->accesses[remote_access[i]];
        uint64_t off = access->offset;
        uint64_t operate_size;
        if(access->type == WR){
            row_t *data = access->data;
            if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size);
            else operate_size = sizeof(uint64_t);
            char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_SILO || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_DSLR_NO_WAIT
            data->_tid_word = 0; //write data and unlock
#if CC_ALG == RDMA_SILO || CC_ALG == RDMA_MOCC
			data->timestamp = time;
#endif
#if CC_ALG == RDMA_WOUND_WAIT2
			data->lock_owner = 0;
#endif
            memcpy(local_buf, (char*)data, operate_size);
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off);
#endif
#if CC_ALG == RDMA_TS1
			uint64_t *temp_tid = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
			*temp_tid = 0;
            memcpy(local_buf, (char*)(temp_tid), operate_size);
			mem_allocator.free(temp_tid, sizeof(uint64_t));
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + sizeof(uint64_t));
#endif 
#if CC_ALG == RDMA_MVCC
			uint64_t *temp_txn_id = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
			*temp_txn_id = 0;
            memcpy(local_buf, (char*)(temp_txn_id), operate_size);
			mem_allocator.free(temp_txn_id, sizeof(uint64_t));
        	int version = access->old_version_num % HIS_CHAIN_NUM ;//version be locked
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + 2*sizeof(uint64_t) + version*sizeof(uint64_t));
#endif
#if CC_ALG == RDMA_CICADA
			vector<uint64_t> remote_num_current = remote_num[loc];
			operate_size = sizeof(uint64_t);
			CicadaState *temp_state = (CicadaState *)mem_allocator.alloc(sizeof(uint64_t));
			if(rc == Abort) *temp_state = Cicada_ABORTED;
			else *temp_state = Cicada_COMMITTED;
            memcpy(local_buf, (char*)(temp_state), operate_size);
			mem_allocator.free(temp_state, sizeof(uint64_t));
			uint64_t num = remote_num_current[i];
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM));
#endif
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,local_buf,remote_off);
        }
        else{
            operate_size = sizeof(uint64_t);
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
#if CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2
            *local_buf = 0;
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,(char*)local_buf,(uint64_t)(remote_mr_attr[loc].buf + off));
#endif
#if CC_ALG == RDMA_NO_WAIT
            remote_need_cas.push_back(remote_access[i]);
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,operate_size,(char*)local_buf,(uint64_t)(remote_mr_attr[loc].buf + off));            
#endif
        }
    }
    auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
//not use outstanding requests here for RDMA_NO_WAIT
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT //otherwise USE_OR is always true
    auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
    RDMA_ASSERT(dbres1 == IOCode::Ok);       
#endif

#if CC_ALG == RDMA_NO_WAIT
    vector<uint64_t> orig_lock_info;
    for(int i = 0;i<remote_need_cas.size();i++){
        uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
        orig_lock_info.push_back(*local_buf);
    }
    while(remote_need_cas.size()>0){
        DBrequests dbreq(remote_need_cas.size());   
        dbreq.init(); 
        for(int i=0;i<remote_need_cas.size();i++){
            Access *access = txnMng->txn->accesses[remote_need_cas[i]];
            uint64_t off = access->offset;
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
            uint64_t lock_info = orig_lock_info[i];
            uint64_t new_lock_info,lock_type,lock_num;
            Row_rdma_2pl::info_decode(lock_info,lock_type,lock_num);
            Row_rdma_2pl::info_encode(new_lock_info,lock_type,lock_num-1);
            assert((lock_type == 0)&&(lock_num > 0)); //must have at least 1 read lock
            dbreq.set_atomic_meta(i,lock_info,new_lock_info,local_buf,remote_mr_attr[loc].buf + off);            
        }
        auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

        //only one signaled request need to be polled
        RDMA_ASSERT(dbres == IOCode::Ok);
        auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
        RDMA_ASSERT(dbres1 == IOCode::Ok);
        
        vector<uint64_t> tmp;
        vector<uint64_t> tmp_lock_info;
        for(int i=0;i<remote_need_cas.size();i++){
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
            if(*local_buf != orig_lock_info[i]){ //CAS fail, atomicity violated
                txnMng->num_atomic_retry++;
                total_num_atomic_retry++;
                if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;
                tmp_lock_info.push_back(*local_buf);
                tmp.push_back(remote_need_cas[i]);
            }        
        }
        remote_need_cas = tmp;
        orig_lock_info = tmp_lock_info;
    }
#endif

}
#endif
#endif

#if BATCH_INDEX_AND_READ
void TxnManager::batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id){
	vector<uint64_t> remote_index = remote_index_origin[loc];
	int count = 0;
	if(loc>=1){
	for(int i=0;i<loc;i++){
		count += remote_index_origin[i].size();
	}
	}
	
	DBrequests dbreq(remote_index.size());
	dbreq.init();
	uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	for(int i=0;i<remote_index.size();i++){
		if(rtype == R_INDEX){
		ycsb_request * req = ycsb_query->requests[remote_index[i]];
		uint64_t index_key = req->key / g_node_cnt;
		uint64_t index_addr = (index_key) * sizeof(IndexInfo);
		char *local_buf = Rdma::get_index_client_memory(thd_id,count+i+1);
		dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,sizeof(IndexInfo),local_buf,(uint64_t)(remote_mr_attr[loc].buf + index_addr));
		}
		else if(rtype == R_ROW){
		uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
		itemid_t * m_item = reqId_index.find(remote_index[i])->second;
		char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
		dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,read_size,local_buf,(uint64_t)(remote_mr_attr[loc].buf + m_item->offset));
		}
	}
	auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
}
void TxnManager::get_batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id){
	int count = 0;
	if(loc>=1){
	for(int i=0;i<loc;i++){
		count += remote_index_origin[i].size();
	}
	}
	uint64_t starttime = get_sys_clock(), endtime;
	vector<uint64_t> remote_index = remote_index_origin[loc];
	uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	//to do: add coroutine
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> dbres1;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		dbres1 = rc_qp[loc][get_thd_id() + cor_id * g_total_thread_cnt]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (dbres1.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
	auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(dbres1 == IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	 for(int i=0;i<remote_index.size();i++){
		if(rtype == R_INDEX){
			char *local_buf = Rdma::get_index_client_memory(thd_id,count+i+1);
			ycsb_request * req = ycsb_query->requests[remote_index[i]];
	//		uint64_t index_key = req->key / g_node_cnt;
	//		uint64_t index_addr = (index_key) * sizeof(IndexInfo);
			assert(((IndexInfo*)local_buf)->key == req->key);
			itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

			item->location = ((IndexInfo*)local_buf)->address;
			item->type = ((IndexInfo*)local_buf)->type;
			item->valid = ((IndexInfo*)local_buf)->valid;
			item->offset = ((IndexInfo*)local_buf)->offset;
			item->table_offset = ((IndexInfo*)local_buf)->table_offset;
			reqId_index.insert(pair<int, itemid_t*>(remote_index[i],item));
		}
		else if(rtype == R_ROW){
			uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
			char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
			row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    		memcpy(test_row, local_buf, read_size);
			reqId_row.insert(pair<int, row_t*>(remote_index[i],test_row));
		}
	 }
 
 }
#endif

// lock 
// |--IS--|--IX--|--S--|--X--|
uint64_t TxnManager::decode_ix_lock(uint64_t lock){
    return (lock<<16)>>48;
}

uint64_t TxnManager::decode_x_lock(uint64_t lock){
    return (lock<<48)>>48;
}

uint64_t TxnManager::decode_is_lock(uint64_t lock){
    return lock>>48;
}

uint64_t TxnManager::decode_s_lock(uint64_t lock){
    return (lock<<32)>>48;
}

bool TxnManager::s_lock_content(uint64_t lock){
    uint64_t ix_lock = decode_ix_lock(lock);
    uint64_t x_lock = decode_x_lock(lock);
    if((ix_lock != 0)||(x_lock != 0)){
    // if((x_lock != 0)){
        return true;
    }else return false;
}

bool TxnManager::is_lock_content(uint64_t lock){
    uint64_t x_lock = decode_x_lock(lock);
    if(x_lock != 0){
        return true;
    }else return false;
}

bool TxnManager::ix_lock_content(uint64_t lock){
    uint64_t s_lock = decode_s_lock(lock);
    uint64_t x_lock = decode_x_lock(lock);
    if((s_lock != 0)||(x_lock != 0)){
        return true;
    }else return false;
}

bool TxnManager::x_lock_content(uint64_t lock){
    if(lock != 0)return true;
    else return false;
}

// bool TxnManager::s_lock_content63(uint64_t lock){
//     if(lock == 1)return true;
//     else return false;
// }
rdma_bt_node * TxnManager::read_left_index_node(yield_func_t &yield,uint64_t cor_id,uint64_t target_server,uint64_t left_range,uint64_t &left_range_node_offset){
    rdma_bt_node * remote_bt_node;
    uint64_t remote_offset = 0;
    uint64_t remote_index_offset = 0;
    //get offset of root of btree
    remote_offset = cas_remote_content(target_server,0,0,1);
    remote_operate_num++;
    assert(remote_offset != 0);
    remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
    left_range_node_offset = remote_offset;  
    remote_operate_num++;
    
    while(remote_bt_node->is_leaf == false){
        int i = 0;
        for(i = 0;i < remote_bt_node->num_keys;i++){
            if((double)left_range < remote_bt_node->keys[i]){
                break;
            }
        }
        remote_offset = remote_bt_node->child_offsets[i];
        mem_allocator.free(remote_bt_node,0);
        remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
        remote_operate_num++;
        left_range_node_offset = remote_offset;  
    }
    while(remote_bt_node->keys[remote_bt_node->num_keys-1]<left_range){
        mem_allocator.free(remote_bt_node,0);
        remote_bt_node = read_remote_bt_node(yield,target_server,remote_bt_node->next_node_offset,cor_id);
    }
    
    return remote_bt_node;
}

rdma_bt_node * TxnManager::read_insert_index_node(yield_func_t &yield,uint64_t cor_id,uint64_t target_server,double insert_key,uint64_t &range_node_insert_offset){
    rdma_bt_node * remote_bt_node;
    uint64_t remote_offset = 0;
    uint64_t remote_index_offset = 0;
    //get offset of root of btree
    remote_offset = cas_remote_content(target_server,0,0,1);
    remote_operate_num++;
    assert(remote_offset != 0);
    remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
    range_node_insert_offset = remote_offset;  
    remote_operate_num++;
    
    while(remote_bt_node->is_leaf == false){
        int i = 0;
        for(i = 0;i < remote_bt_node->num_keys;i++){
            if(insert_key < remote_bt_node->keys[i]){
                break;
            }
        }
        remote_offset = remote_bt_node->child_offsets[i];
        mem_allocator.free(remote_bt_node,0);
        remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
        remote_operate_num++;
        range_node_insert_offset = remote_offset;  
    }
    if(remote_bt_node->keys[remote_bt_node->num_keys - 1] < insert_key){
        while(remote_bt_node->next_node_offset != UINT64_MAX){
            remote_offset = remote_bt_node->next_node_offset;
            rdma_bt_node* next_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
            if(next_node->keys[0] < insert_key){
                range_node_insert_offset = remote_offset;  
                mem_allocator.free(remote_bt_node,0);
                remote_bt_node = next_node;
            }
            else break;
        }
        
    }
    return remote_bt_node;
}

void TxnManager::split_leaf_index_node(rdma_bt_node *&old_node,rdma_bt_node *&new_index_node,rdma_bt_node *&parent_node,double insert_key,uint64_t right_offset){
    // printf("[txn.cpp:4867]split leaf\n");
    uint64_t insert_idx = 0;
    rdma_idx_key_t temp_keys[BTREE_ORDER];
    uint64_t temp_child_odffsets[BTREE_ORDER];
	void * temp_pointers[BTREE_ORDER];
    int i = 0, j =0 ;

    int insertion_index = 0;
    //locate
    while (insertion_index < BTREE_ORDER - 1 && old_node->keys[insertion_index] < insert_key) insertion_index++;

    //insert
    for(i = 0;i < old_node->num_keys;i++){
        if(old_node->keys[i] < insert_key){
                insert_idx = i;
                temp_keys[i] = old_node->keys[i];
                temp_child_odffsets[i] = old_node->child_offsets[i];
                temp_pointers[i] = old_node->pointers[i];
        } 
        else{break;}
    }
    temp_keys[i] = insert_key;
    temp_child_odffsets[i] = 0;
    temp_pointers[i] = NULL;
    j = i+1;
    while(i < old_node->num_keys){
        temp_keys[j] = old_node->keys[i];
        temp_child_odffsets[j] = old_node->child_offsets[i];
        temp_pointers[j] = old_node->pointers[i];
        i++;j++;
    } 
    // for(int k=0;k<old_node->num_keys;k++){
    //     printf("[txn.cpp:4770]index_node_to_insert[%d] = %lf\n",old_node->keys[k],k);
    // }
    // for(int k=0;k<old_node->num_keys+1;k++){
    //     printf("[txn.cpp:4782]temp_keys[%ld] = %lf\n",k,temp_keys[k]);
    // }
    int split_num;
    //  = (BTREE_ORDER + 1)/2;
    if ((BTREE_ORDER-1) % 2 == 0)split_num = (BTREE_ORDER-1)/2;
    else split_num = (BTREE_ORDER-1)/2 + 1;

    // printf("[txn.cpp:4781]i=%d,j=%d\n",i,j);
    // 0 1 2 3 | 4 5 6 7 8
    //0 1 2 3 4 5 6 7 8 num=4
    //4 5 6 7 8 num=5
    i=0;j = 0;
    //split to new node
    new_index_node->num_keys=0;
    for(i = split_num,j=0;i< old_node->num_keys+1;i++,j++){
        new_index_node->child_offsets[j] = temp_child_odffsets[i];
        new_index_node->keys[j] = temp_keys[i];
        // printf("[txn.cpp:4794]txn%d right key[%d] = %lf\n",txn->txn_id,j,new_index_node->keys[j]);
        new_index_node->pointers[j] = temp_pointers[i];
        new_index_node->num_keys ++;
    }
    new_index_node->next_node_offset = old_node->next_node_offset;
    new_index_node->parent_offset = old_node->parent_offset;

    //modify parent
    modify_parent_node(old_node,new_index_node,parent_node,insert_key,temp_keys,right_offset); 

    //modify old node
    old_node->num_keys = 0;
    for(i = 0;i<split_num;i++){
        old_node->child_offsets[i] = temp_child_odffsets[i];
        old_node->keys[i] = temp_keys[i];
        old_node->pointers[i] = temp_pointers[i];
        old_node->num_keys ++;
    }
    old_node->next_node_offset = right_offset;

    // for(int k=0;k<old_node->num_keys;k++){
    //     printf("[txn.cpp:4952]num=%ld,old_node[%d] = %lf\n",old_node->num_keys,k,old_node->keys[k]);
    // }
    // for(int k=0;k<new_index_node->num_keys;k++){
    //     printf("[txn.cpp:4955]num=%ld,new_node[%d] = %lf\n",new_index_node->num_keys,k,new_index_node->keys[k]);
    // }

	for (i = old_node->num_keys; i < BTREE_ORDER - 1; i++) {
		old_node->keys[i] = 0;
		old_node->pointers[i] = NULL;
		old_node->child_offsets[i] = UINT64_MAX;
	}
	for (i = new_index_node->num_keys; i < BTREE_ORDER - 1; i++) {
		new_index_node->keys[i] = 0;
		new_index_node->child_offsets[i] = UINT64_MAX;
		new_index_node->pointers[i] = NULL;
	}
}

void TxnManager::modify_parent_node(rdma_bt_node *&index_node_to_insert,rdma_bt_node *&new_index_node,rdma_bt_node *&parent_node,double insert_key,double *temp_keys,uint64_t right_offset){
    // for(int j = 0;j<parent_node->num_keys;j++){
    //     printf("[txn.cpp:4975]old_parent_node[%d] = %lf\n",j,parent_node->keys[j]);
    // }
    rdma_bt_node *next_parent=(rdma_bt_node*)(rdma_global_buffer + parent_node->next_node_offset);
    // for(int j = 0;j<next_parent->num_keys;j++){
    //     printf("[txn.cpp:4983]next_parent_node[%d] = %lf\n",j,next_parent->keys[j]);
    // }
    // printf("[txn.cpp:4977]parent_node->num_keys = %ld\n",parent_node->num_keys);
    int insert_idx = 0;
    double new_key = new_index_node->keys[0];
    while (parent_node->keys[insert_idx] < new_key && insert_idx < parent_node->num_keys) insert_idx++;

    // if (parent->num_keys < order - 1) {
		for (int i = parent_node->num_keys-1; i >= insert_idx; i--) {
            assert(i+2<16);
			parent_node->keys[i + 1] = parent_node->keys[i];
			parent_node->pointers[i+2] = parent_node->pointers[i+1];
            parent_node->child_offsets[i+2] =  parent_node->child_offsets[i+1];
			assert((parent_node->child_offsets[i+2] - 8) % sizeof(rdma_bt_node) == 0);
			// printf("[txn.cpp:5060]parent_node %lu i %ld offset %ld\n",(char*)parent_node - rdma_global_buffer, (i+2), parent_node->child_offsets[i+2]);
		}
		parent_node->num_keys ++;
		parent_node->keys[insert_idx] = new_key;
		parent_node->pointers[insert_idx + 1] = (rdma_bt_node*)(rdma_global_buffer + right_offset);
        parent_node->child_offsets[insert_idx + 1] = right_offset;
		assert((right_offset - 8) % sizeof(rdma_bt_node) == 0);
		// printf("[txn.cpp:5060]parent_node %lu i %ld offset %ld\n",(char*)parent_node - rdma_global_buffer, (insert_idx + 1), right_offset);
}

RC TxnManager::try_insert_new_data(yield_func_t &yield,uint64_t cor_id,uint64_t target_server,double insert_key,INDEX *index){
    double int_key = floor(insert_key) + g_node_cnt;

    uint64_t remote_server = GET_NODE_ID((uint64_t)int_key);
    rdma_bt_node * index_node_to_insert;

    // printf("[txn.cpp:5118]insert_key = %lf\n",insert_key);

    if(remote_server != g_node_id){
        
       
        uint64_t range_node_insert_offset = 0;
        index_node_to_insert = read_insert_index_node(yield,cor_id,remote_server,insert_key,range_node_insert_offset);

        UInt32 num_of_key = index_node_to_insert->num_keys;
        // assert(index_node_to_insert->keys[num_of_key - 1] >= int_key);
        // assert(index_node_to_insert->keys[num_of_key-1] >= insert_key);

        //split
        if(index_node_to_insert->num_keys >= BTREE_ORDER - 2){//if leaf node need split
            //lock parent
            uint64_t try_lock = -1;
            try_lock = cas_remote_content(remote_server,index_node_to_insert->parent_offset,0,1);
            if(try_lock!=0)return Abort;
            // while(try_lock != 0 && !simulation->is_done()){
            //     try_lock = cas_remote_content(remote_server,index_node_to_insert->parent_offset,0,1);
            // }

            //check parent node
            rdma_bt_node *parent_node = read_remote_bt_node(yield,remote_server,index_node_to_insert->parent_offset,cor_id);
            if(parent_node->num_keys >= BTREE_ORDER - 2){
                // abort
                try_lock=0;
                while(try_lock!=1&&!simulation->is_done()){
                    // printf("[txn.cpp:5131]\n");
                    try_lock = cas_remote_content(remote_server,index_node_to_insert->parent_offset,1,0);
                }
                return Abort;
            }
            else if(parent_node->num_keys < BTREE_ORDER - 2){
                try_lock=0;
                while(try_lock!=1&&!simulation->is_done()){
                    // printf("[txn.cpp:5139]\n");
                    try_lock = cas_remote_content(remote_server,index_node_to_insert->parent_offset,1,0);
                }
                return Abort;
                //lock node to insert
                try_lock = -1;
                while(try_lock != 0 && !simulation->is_done()){
                    // printf("[5140]\n");
                    try_lock = cas_remote_content(remote_server,range_node_insert_offset,0,1);
                }

                uint64_t new_index_node_idx = faa_remote_content(yield,remote_server,rdma_index_size - sizeof(uint64_t),cor_id);
                
                int i = index_node_to_insert->num_keys - 1;
                int j = BTREE_ORDER/2 - 1;
                rdma_bt_node * new_index_node = (rdma_bt_node *)malloc(sizeof(rdma_bt_node));
                new_index_node->intent_lock = 0;
                new_index_node->num_keys = 0;
                new_index_node->is_leaf = true;
	            new_index_node->parent_offset = index_node_to_insert->parent_offset;
                new_index_node->next_node_offset = index_node_to_insert->next_node_offset;               

                //write new leaf node
                uint64_t remote_index_node_cnt = faa_remote_content(yield,remote_server,rdma_buffer_size - sizeof(uint64_t),cor_id);
                uint64_t new_offset = sizeof(root_offset_struct) + sizeof(rdma_bt_node)*remote_index_node_cnt;

                 //modify index node
                split_leaf_index_node(index_node_to_insert,new_index_node,parent_node,insert_key,new_offset);

                write_remote_index(remote_server,sizeof(rdma_bt_node),new_offset,(char*)new_index_node);

                //modify parent node
                write_remote_index(remote_server,sizeof(rdma_bt_node)-sizeof(uint64_t),index_node_to_insert->parent_offset+sizeof(uint64_t),(char*)(parent_node+sizeof(uint64_t)));

                //modify splited leaf node
                write_remote_index(remote_server,sizeof(rdma_bt_node)-sizeof(uint64_t),range_node_insert_offset+sizeof(uint64_t),(char*)(index_node_to_insert+sizeof(uint64_t)));

                //unlock
                try_lock = 0;
                while(try_lock!=1 && !simulation->is_done()){
                    cas_remote_content(remote_server,range_node_insert_offset,1,0);
                }
                try_lock = 0;
                while(try_lock!=1 && !simulation->is_done()){
                   cas_remote_content(remote_server,index_node_to_insert->parent_offset,1,0);
                }
                return RCOK;
            }    
        }//endif leaf node need split
        else if(index_node_to_insert->num_keys < BTREE_ORDER - 2){//dont need to split
            if(x_lock_content(index_node_to_insert->intent_lock))return Abort;

            //acquire X lock on range
            // rdma_bt_node * origin_bt_node = read_remote_bt_node(yield,remote_server,range_node_insert_offset,cor_id);
            // assert(origin_bt_node->keys[0] == index_node_to_insert->keys[0]);
            // mem_allocator.free(origin_bt_node,0);

            uint64_t add_value = 1;//0x0001
            uint64_t before_faa = -1;
            before_faa = cas_remote_content(yield,remote_server,range_node_insert_offset,0,1,cor_id);
            if(before_faa!=0)return Abort;

            //record
            txn->range_node_set[txn->locked_range_num] = range_node_insert_offset;
            txn->server_set[txn->locked_range_num] = remote_server;
            txn->locked_range_num = txn->locked_range_num + 1;
            
            int j = 0;
            for(j = 0;j <= num_of_key - 1;j++){
                if(index_node_to_insert->keys[j] > insert_key)break;
                // if(index_node_to_insert->keys[j] > last_key)break;
            }
            // for(int k = 0;k<index_node_to_insert->num_keys;k++){
            //     printf("[txn.cpp:5196]old_node[%d]=%lf\n",k,index_node_to_insert->keys[k]);
            // }
            int k = num_of_key;
            for(k = num_of_key ; k > j;k--){
				assert(k < 16);
                index_node_to_insert->keys[k] = index_node_to_insert->keys[k - 1];
                index_node_to_insert->child_offsets[k] = index_node_to_insert->child_offsets[k-1];
                index_node_to_insert->pointers[k] = index_node_to_insert->pointers[k-1];
            }
            index_node_to_insert->keys[j] = insert_key;
            //TODO
            uint64_t remote_row_num = faa_remote_content(yield,remote_server,rdma_buffer_size - sizeof(uint64_t),1,cor_id);
                
            uint64_t size = row_t::get_row_size(general_tuple_size);
            uint64_t offset = size * remote_row_num;  
            // row_t *ptr = (row_t*)(rdma_global_buffer + offset);
            index_node_to_insert->child_offsets[j] = rdma_index_size + offset;
            index_node_to_insert->pointers[j] = NULL;

            index_node_to_insert->num_keys++;
            // for(int k = 0;k<index_node_to_insert->num_keys;k++){
            //     printf("[txn.cpp:5217]key=%lf,inserted_node[%d]=%lf\n",insert_key,k,index_node_to_insert->keys[k]);
            // }
            write_remote_index(yield,remote_server,sizeof(rdma_bt_node) - sizeof(uint64_t),range_node_insert_offset+sizeof(uint64_t),(char*)index_node_to_insert+sizeof(uint64_t),cor_id);//修改index
            // write_remote_index(yield,remote_server,0,range_node_insert_offset,(char*)index_node_to_insert,cor_id);//修改index

            // write_remote_row(yield,remote_server,sizeof(rdma_bt_node), rdma_index_size + offset,(char*)index_node_to_insert,cor_id);//插入row

            //解锁
            before_faa = -1;
            while(before_faa != 1 && !simulation->is_done()){
                before_faa = cas_remote_content(yield,remote_server,range_node_insert_offset,1,0,cor_id);
            }
            return RCOK; 
        }//dont need to split
        else{
            assert(false);
        }  
        
    }else{
		//TODO local txn
		uint64_t part_id = ((uint64_t)insert_key) % g_part_cnt;
		// double int_key = floor(insert_key) + g_node_cnt;
		double int_key = floor(insert_key);
		index_node_to_insert = find_index_node_to_insert(index, insert_key, part_id);
		if (index_node_to_insert == NULL) return Abort;

		assert(part_id == g_node_id);
		UInt32 num_of_key = index_node_to_insert->num_keys;
		bool find_insert_slot = false;
		uint64_t range_node_insert_offset = (char*)index_node_to_insert - rdma_global_buffer;

		if(num_of_key == BTREE_ORDER - 2){
			uint64_t parent_offset = index_node_to_insert->parent_offset;
			uint64_t try_lock = -1;
			while(try_lock != 0 && !simulation->is_done()){
				try_lock = cas_remote_content(g_node_id,parent_offset,0,1);
			}

			//check parent node
			rdma_bt_node *parent_node = (rdma_bt_node*)(rdma_global_buffer+index_node_to_insert->parent_offset);
			// printf("[txn.cpp:5240]parent_node offset = %lu\n",index_node_to_insert->parent_offset);
			if(parent_node->num_keys >= BTREE_ORDER - 2){
				// abort
                // printf("[txn.cpp:5221]parent full\n");
				try_lock = 0;
				while((try_lock != 1)&&(!simulation->is_done())){
					try_lock = cas_remote_content(g_node_id,parent_offset,1,0);
				}
				INC_STATS(0, insert_abort1, 1);
				return Abort;
			}
			else if(parent_node->num_keys < BTREE_ORDER - 2){
				try_lock = -1;
				while(try_lock != 0 &&  !simulation->is_done()){
					try_lock = cas_remote_content(g_node_id,range_node_insert_offset,0,1);
				}

				int i = index_node_to_insert->num_keys - 1;
				int j;
				if ((BTREE_ORDER-1) % 2 == 0)j = (BTREE_ORDER-1)/2;
				else j = (BTREE_ORDER-1)/2 + 1;

				uint64_t new_index_node_cnt = faa_remote_content(yield,g_node_id,rdma_index_size - sizeof(uint64_t),1,cor_id);
				uint64_t new_offset = sizeof(root_offset_struct) + sizeof(rdma_bt_node)*new_index_node_cnt;
				rdma_bt_node * new_index_node;
				new_index_node = (rdma_bt_node *)(rdma_global_buffer + new_offset);

				new_index_node->intent_lock = 0;
				new_index_node->num_keys = 0;
				new_index_node->is_leaf = true;
				new_index_node->parent_offset = index_node_to_insert->parent_offset;
				new_index_node->next_node_offset = index_node_to_insert->next_node_offset;

				rdma_bt_node * old_index_node = (rdma_bt_node *)malloc(sizeof(rdma_bt_node));
				rdma_bt_node * old_parent_node = (rdma_bt_node *)malloc(sizeof(rdma_bt_node));
				rdma_bt_node * new_node = (rdma_bt_node *)malloc(sizeof(rdma_bt_node));
				
				new_node->init_new_leaf();

				memcpy(old_index_node,index_node_to_insert,sizeof(rdma_bt_node));
				memcpy(old_parent_node,parent_node,sizeof(rdma_bt_node));

				// for(int i = 0;i<index_node_to_insert->num_keys;i++){
				// 	for(int k=0;k<index_node_to_insert->num_keys;k++){
				// 		printf("[txn.cpp:5345]node offset %ld before old_node[%d] = %lf\n",range_node_insert_offset, k,index_node_to_insert->keys[k]);
				// 	}
				// 	for(int k=0;k<new_index_node->num_keys+1;k++){
				// 		printf("[txn.cpp:5348]node offset %ld before new_node[%d] = %lf\n", new_offset, k,new_index_node->keys[k]);
				// 	}
				// } 
		
				split_leaf_index_node(old_index_node,new_node,old_parent_node,insert_key,new_offset);
				
				// write new node
				memcpy((char*)new_index_node+sizeof(uint64_t),(char*)new_node+sizeof(uint64_t),sizeof(rdma_bt_node)-sizeof(uint64_t));

				// modify parent
				memcpy((char*)parent_node+sizeof(uint64_t),(char*)old_parent_node+sizeof(uint64_t),sizeof(rdma_bt_node)-sizeof(uint64_t));

				// modify old node
				memcpy((char*)index_node_to_insert+sizeof(uint64_t),(char*)old_index_node+sizeof(uint64_t),sizeof(rdma_bt_node)-sizeof(uint64_t));       

				// // for(int i = 0;i<index_node_to_insert->num_keys;i++){
				// // 	for(int k=0;k<index_node_to_insert->num_keys;k++){
				// // 		printf("[txn.cpp:5419]left node offset %ld after old_node[%d] = %lf\n",range_node_insert_offset, k,index_node_to_insert->keys[k]);
				// // 	}
				// // 	for(int k=0;k<new_index_node->num_keys;k++){
				// // 		printf("[txn.cpp:5422]right node offset %ld after new_node[%d] = %lf\n", new_offset, k,new_index_node->keys[k]);
				// // 	}
				// // 	for(int k=0;k<new_index_node->num_keys;k++){
				// // 		printf("[txn.cpp:5374]parent node offset %ld after new_node[%d] = %lf, %lu\n", index_node_to_insert->parent_offset, k,parent_node->keys[k],parent_node->child_offsets[k]);
				// // 	}
				// // } 

                //     //unlock
				try_lock = 0;
				while(try_lock!=1 && !simulation->is_done()){
					// uint64_t ix,x,is,s;
					// ix = decode_ix_lock(try_lock);
					// x = decode_x_lock(try_lock);
					// is = decode_is_lock(try_lock);
					// s = decode_s_lock(try_lock);
					try_lock=cas_remote_content(g_node_id,range_node_insert_offset,1,0);
				}

				try_lock=0;
				while(try_lock != 1 && !simulation->is_done()){
					try_lock = cas_remote_content(g_node_id,parent_offset,1,0);
				}
				
				mem_allocator.free(new_node,0);
				mem_allocator.free(old_parent_node,0);
				mem_allocator.free(old_index_node,0);
				
				return RCOK;
			}
			
		}else if(num_of_key < BTREE_ORDER - 2){
			uint64_t ix,x,is,s;
			uint64_t local_offset = (char*)index_node_to_insert - rdma_global_buffer;

			//range lock(X)
			uint64_t before_cas = -1;
			before_cas = cas_remote_content(yield,remote_server,local_offset,0,1,cor_id);
			if(before_cas != 0){
				// printf("[txn.cpp:5246]fail lock\n");
				return Abort;
			}

			num_of_key = index_node_to_insert->num_keys; 
			if(num_of_key == BTREE_ORDER - 2){
				before_cas = 0;
				while(before_cas != 1 && !simulation->is_done()){
					cas_remote_content(yield,remote_server,local_offset,1,0,cor_id);
				}
				printf("[txn.cpp:5254]no space\n");
				return Abort;
			}
			ix = decode_ix_lock(before_cas);
			x = decode_x_lock(before_cas);
			is = decode_is_lock(before_cas);
			s = decode_s_lock(before_cas);
			// printf("[txn.cpp:5249]txn%ld get lock,offset=%ld,ix%ld,x%ld,is%ld,s%ld\n",get_txn_id(),local_offset,ix,x,is,s);

			int j = 0;
			for(j = 0;j <= num_of_key - 1;j++){
				if(index_node_to_insert->keys[j] > insert_key)break;
			}
			rdma_bt_node *old_node = (rdma_bt_node*)malloc(sizeof(rdma_bt_node));
			memcpy((char*)old_node,(char*)index_node_to_insert,sizeof(rdma_bt_node));
			assert(old_node->num_keys == index_node_to_insert->num_keys);

//写新记录
			uint64_t local_row_num = faa_remote_content(yield,remote_server,rdma_buffer_size - sizeof(uint64_t),1,cor_id);
				
			uint64_t size = row_t::get_row_size(general_tuple_size);
			uint64_t row_offset = size * local_row_num;  
			row_t *ptr = (row_t*)(rdma_global_buffer + rdma_index_size + row_offset);

//修改index 
			assert(insert_key != 0);
			int k = num_of_key;
			old_node->num_keys++;
			for(k = num_of_key ; k > j;k--){
				assert(k < 16);
				old_node->keys[k] = old_node->keys[k - 1];
				old_node->child_offsets[k] = old_node->child_offsets[k-1];
				old_node->pointers[k] = old_node->pointers[k-1];
			}
			old_node->keys[j] = insert_key;                
			old_node->child_offsets[j] = rdma_index_size + row_offset;
			old_node->pointers[j] = NULL;

			// for(int i = 0;i < index_node_to_insert->num_keys;i++){
			//         printf("[txn.cpp:5521]insert%lf,old_node[%d]=%lf\n",insert_key,i,index_node_to_insert->keys[i]);
			// }
			// for(int i = 0;i < old_node->num_keys;i++){
			//         printf("[txn.cpp:5524]insert%lf,inserted_node[%d]=%lf\n",insert_key,i,old_node->keys[i]);
			// }
	
			memcpy((char*)(index_node_to_insert)+sizeof(uint64_t),
					(char*)(old_node)+sizeof(uint64_t),
					sizeof(rdma_bt_node)-sizeof(uint64_t));


			// write_remote_row(yield,remote_server,sizeof(rdma_bt_node),left_range_node_offset,(char*)index_node_to_insert,cor_id);//插入row
			ix = decode_ix_lock(index_node_to_insert->intent_lock);
			x = decode_x_lock(index_node_to_insert->intent_lock);
			is = decode_is_lock(index_node_to_insert->intent_lock);
			s = decode_s_lock(index_node_to_insert->intent_lock);

			// printf("[txn.cpp:5284]txn%ld release lock,offset=%ld,ix%ld,x%ld,is%ld,s%ld\n",get_txn_id(),local_offset,ix,x,is,s);
			//解锁
			before_cas = -1;
			while(before_cas!=1 && !simulation->is_done()){
				before_cas = cas_remote_content(yield,remote_server,local_offset,1,0,cor_id);
			}
			ix = decode_ix_lock(before_cas);
			x = decode_x_lock(before_cas);
			is = decode_is_lock(before_cas);
			s = decode_s_lock(before_cas);

			// printf("[txn.cpp:5293]txn%ld release lock,offset=%ld,ix%ld,x%ld,is%ld,s%ld\n",get_txn_id(),local_offset,ix,x,is,s);
			mem_allocator.free(old_node,0);
			return RCOK; 
		}//no split
		else{
			assert(false);
			return Abort;
		}
	}
}

#if INDEX_STRUCT == IDX_LEARNED
LeafIndexInfo * TxnManager::read_left_leaf_index_node(yield_func_t &yield,uint64_t cor_id,uint64_t target_server,uint64_t left_key,uint64_t &left_range_node_offset){
    auto position = pgm_index[target_server]->search(left_key);
    int subscript = position.pos;
    LeafIndexInfo * remote_learn_node;
    uint64_t remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
    uint64_t remote_index_offset = 0;
    
    remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id);  
    remote_operate_num++;
    
    bool get_data = false;
    int high = subscript+64 < ((g_synth_table_size/g_node_cnt)/range_size)?subscript+64:(g_synth_table_size/g_node_cnt)/range_size;
    int low = subscript-64 > 0 ? subscript-64 : 0;

    while(get_data == false){
        int cnt = remote_learn_node->key_cnt;
        // printf("[txn.cpp:4720]key = %ld,subscript = %ld,first_key = %ld,low = %ld,high = %ld\n",left_key,subscript,remote_learn_node->keys[0],low,high);
        if(left_key >= remote_learn_node->keys[0] && left_key <= remote_learn_node->keys[cnt - 1]){
            for(int i = 0;i<remote_learn_node->key_cnt;i++){
                if(remote_learn_node->keys[i] == left_key){       
                    left_range_node_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
                    get_data = true;
                    break;
                }
            }
        }
        if(get_data == true)break;

        if(subscript == 0){
            low = 0;
            high = 64;
            subscript = (low + high)/2;
            remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
            mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
            remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id); 
            remote_operate_num++;
            continue;
        }
        else if(subscript == ((g_synth_table_size/g_node_cnt)/range_size)){
            high = ((g_synth_table_size/g_node_cnt)/range_size);
            low = high -64;
            subscript = (low + high)/2;
            remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
            mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
            remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id); 
            remote_operate_num++;
            continue;
        }
        if(left_key > remote_learn_node->keys[cnt - 1]){
                low = subscript + 1;
        }
        else{
                high = subscript - 1;
        }
        if(low > high)break;

        subscript = (low + high)/2;
        remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
        mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
        remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id);  
        remote_operate_num++;
    }

    assert(get_data == true);
    
    return remote_learn_node;
}
#endif

void TxnManager::read_continuous_index(yield_func_t &yield,int target_server, int batch_num,uint64_t *batch_key_vector, itemid_t **batch_index_vector, uint64_t cor_id){
	int count = 0;
    count = (target_server - 1)*(g_req_per_query/g_node_cnt + 2);
	
	uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	
    uint64_t index_key = batch_key_vector[0] / g_node_cnt;
    uint64_t index_addr = (index_key) * sizeof(IndexInfo);
    uint64_t operate_size = batch_num * sizeof(IndexInfo);
    char *local_buf = Rdma::get_index_client_memory(thd_id,count+1);
    memset(local_buf, 0, operate_size);

    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = index_addr,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    //!check ; only need to read the first index
    char *tmp_index = (char*)mem_allocator.alloc(sizeof(IndexInfo));
    for(int i = 0;i < batch_num;i++){
        memcpy(local_buf + sizeof(IndexInfo)*i,tmp_index,sizeof(IndexInfo));
        batch_index_vector[i]->location = ((IndexInfo*)tmp_index)->address;
        batch_index_vector[i]->type = ((IndexInfo*)tmp_index)->type;
        batch_index_vector[i]->valid = ((IndexInfo*)tmp_index)->valid;
        batch_index_vector[i]->offset = ((IndexInfo*)tmp_index)->offset;
        batch_index_vector[i]->table_offset = ((IndexInfo*)tmp_index)->table_offset;

        assert(((IndexInfo*)local_buf+sizeof(IndexInfo)*i)->key == batch_key_vector[i]);
    }
    mem_allocator.free(tmp_index,0);
}

void TxnManager::read_continuous_row(yield_func_t &yield,int target_server, int batch_num,uint64_t *batch_key_vector, itemid_t **batch_index_vector,row_t * row_local, uint64_t cor_id){
    
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    uint64_t operate_size = sizeof(row_t)* batch_num;
    int count = 0;
    count = (target_server - 1)*(g_req_per_query/g_node_cnt + 2);
    char *local_buf = Rdma::get_index_client_memory(thd_id,count+1);
    memset(local_buf, 0, operate_size);

    uint64_t remote_address = batch_index_vector[0]->offset;

    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_address,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    char *tmp_row = (char*)mem_allocator.alloc(sizeof(row_t));
    for(int i = 0;i < batch_num;i++){
        memcpy(local_buf + sizeof(row_t)*i,tmp_row,sizeof(row_t));
		RC rc = preserve_access(row_local,batch_index_vector[i],(row_t *)tmp_row,RD,((row_t *)tmp_row)->get_primary_key(),target_server);
    }
}


row_t * TxnManager::read_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t cor_id){
    uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
   
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
    // RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	
	// INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - h_thd->start_wait_time);

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    row_t *test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

// rdma_bt_node * TxnManager::read_remote_bt_node(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){}
//TODO:读远程index节点信息

rdma_bt_node * TxnManager::read_remote_bt_node(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
    uint64_t operate_size = sizeof(rdma_bt_node);
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, operate_size);
    // assert((rdma_bt_node*)test_buf->intent_lock == 0);
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    rdma_bt_node* remote_node = (rdma_bt_node *)mem_allocator.alloc(sizeof(rdma_bt_node));
    for(int i = 0;i < BTREE_ORDER;i ++){
        remote_node->child_offsets[i] = ((rdma_bt_node*)test_buf)->child_offsets[i];
        remote_node->keys[i] = ((rdma_bt_node*)test_buf)->keys[i];
    }
    remote_node->intent_lock = ((rdma_bt_node*)test_buf)->intent_lock;
    remote_node->num_keys = ((rdma_bt_node*)test_buf)->num_keys;
    remote_node->parent_offset = ((rdma_bt_node*)test_buf)->parent_offset;
    remote_node->next_node_offset = ((rdma_bt_node*)test_buf)->next_node_offset;
    remote_node->is_leaf = ((rdma_bt_node*)test_buf)->is_leaf;

    return remote_node;
}

LeafIndexInfo * TxnManager::read_remote_learn_node(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
    uint64_t operate_size = sizeof(LeafIndexInfo);

    int count = 0;
    count = (target_server - 1)*(g_req_per_query/g_node_cnt + 2);

    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, operate_size);
    // assert((rdma_bt_node*)test_buf->intent_lock == 0);
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    LeafIndexInfo* remote_node = (LeafIndexInfo *)mem_allocator.alloc(sizeof(LeafIndexInfo));
    for(int i = 0;i < BTREE_ORDER;i ++){
        remote_node->offsets[i] = ((LeafIndexInfo*)test_buf)->offsets[i];
        remote_node->keys[i] = ((LeafIndexInfo*)test_buf)->keys[i];
    }
    remote_node->intent_lock = ((LeafIndexInfo*)test_buf)->intent_lock;
    remote_node->key_cnt = ((LeafIndexInfo*)test_buf)->key_cnt;
    // remote_node->parent_offset = ((rdma_bt_node*)test_buf)->parent_offset;
    // remote_node->next_node_offset = ((rdma_bt_node*)test_buf)->next_node_offset;
    // remote_node->is_leaf = ((rdma_bt_node*)test_buf)->is_leaf;

    return remote_node;
}


itemid_t * TxnManager::read_remote_btree_index(yield_func_t &yield, uint64_t target_server,uint64_t key, uint64_t cor_id){
    rdma_bt_node * remote_bt_node;
    uint64_t remote_offset = 0;
    uint64_t remote_index_offset = 0;
    //get offset of root of btree
    remote_offset = cas_remote_content(target_server,0,0,1);
    remote_operate_num++;
    assert(remote_offset != 0);
    // printf("[txn.cpp:5831]txn%ld read offset%ld\n",get_txn_id(),remote_offset);
    remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id); 
    remote_operate_num++;
    
    while(remote_bt_node->is_leaf == false){
        int i = 0;
        for(i = 0;i < remote_bt_node->num_keys;i++){
            if(key < remote_bt_node->keys[i]){
                break;
            }
        }
        remote_offset = remote_bt_node->child_offsets[i];
        mem_allocator.free(remote_bt_node,0);
    // printf("[txn.cpp:5844]txn%ld read offset%ld\n",get_txn_id(),remote_offset);
        remote_bt_node = read_remote_bt_node(yield,target_server,remote_offset,cor_id);
        remote_operate_num++;
    }

    while(remote_bt_node->keys[remote_bt_node->num_keys-1]<key){
        mem_allocator.free(remote_bt_node,0);
        if(remote_bt_node->next_node_offset!=UINT64_MAX){
    // printf("[txn.cpp:5852]txn%ld read offset%ld\n",get_txn_id(),remote_bt_node->next_node_offset);
            remote_bt_node = read_remote_bt_node(yield,target_server,remote_bt_node->next_node_offset,cor_id);
        }  
    }
    
    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
    for(int i = 0;i < remote_bt_node->num_keys;i++){
        if(remote_bt_node->keys[i] == key){
            // item->location = ?
            // item->type = ?
            // item->valid = ?
            item->offset = remote_bt_node->child_offsets[i];
            item->leaf_node_offset = remote_offset;
            item->range_lock = remote_bt_node->intent_lock;
            // item->table_offset = ?
        }
    }
    assert(item != NULL);
    assert((item->leaf_node_offset!=0)&&((item->leaf_node_offset-8)%504==0));
    return item;

 }

 itemid_t * TxnManager::read_remote_learn_index(yield_func_t &yield, uint64_t target_server,uint64_t key, uint64_t cor_id){

    auto position = pgm_index[target_server]->search(key);
    int subscript = position.pos;
    LeafIndexInfo * remote_learn_node;
    uint64_t remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
    uint64_t remote_index_offset = 0;
    
    remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id);  
    remote_operate_num++;
    
    bool get_data = false;
    int high = subscript+64 < ((g_synth_table_size/g_node_cnt)/range_size)?subscript+64:(g_synth_table_size/g_node_cnt)/range_size;
    int low = subscript-64 > 0 ? subscript-64 : 0;
    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

    // printf("[txn.cpp:5098]key = %ld, pos = %ld\n",key,position.pos);

    while(get_data == false){
        // printf("[ycsb_txn.cpp:5101]key = %ld,subscript = %ld,first_key = %ld\n",key,subscript,remote_learn_node->keys[0]);
        int cnt = remote_learn_node->key_cnt;
        if(key >= remote_learn_node->keys[0] && key <= remote_learn_node->keys[cnt - 1]){
            for(int i = 0;i<remote_learn_node->key_cnt;i++){
                if(remote_learn_node->keys[i] == key){
                    item->offset = remote_learn_node->offsets[i];
                    item->leaf_node_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
                    item->range_lock =  remote_learn_node->intent_lock;
                
                    get_data = true;
                    break;
                }
            }
        }
        if(get_data == true)break;

        if(subscript == 0){
            low = 0;
            high = 64;
            subscript = (low + high)/2;
            remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
            mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
            remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id); 
            remote_operate_num++;
            continue;
        }
        else if(subscript == ((g_synth_table_size/g_node_cnt)/range_size)){
            high = ((g_synth_table_size/g_node_cnt)/range_size);
            low = high -64;
            subscript = (low + high)/2;
            remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
            mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
            remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id);
            remote_operate_num++;
            continue;
        }
        if(key > remote_learn_node->keys[cnt - 1]){
                low = subscript + 1;
        }
        else{
                high = subscript - 1;
        }
        if(low > high)break;

        // printf("[txn.cpp:5140]low = %ld,high = %ld,key = %ld,pos = %ld\n",low,high,key,position.pos);
        subscript = (low + high)/2;
        remote_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
        mem_allocator.free(remote_learn_node,sizeof(LeafIndexInfo));
        remote_learn_node = read_remote_learn_node(yield,target_server,remote_offset,cor_id); 
        remote_operate_num++;
    }

    assert(get_data == true);
    return item;

 }


 itemid_t * TxnManager::read_remote_index(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t key, uint64_t cor_id){
    uint64_t operate_size = sizeof(IndexInfo);
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
	// printf("%ld:%ld:%ld  = thd_id:%ld\n", get_thd_id(), cor_id, g_total_thread_cnt, thd_id);
    char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, operate_size);
   
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
    // printf("[txn.cpp:4438]thd_id = %d, cor_id = %d, test_buf address = %d\n",get_thd_id(),cor_id,test_buf);
	item->location = ((IndexInfo*)test_buf)->address;
	item->type = ((IndexInfo*)test_buf)->type;
	item->valid = ((IndexInfo*)test_buf)->valid;
	item->offset = ((IndexInfo*)test_buf)->offset;
  	item->table_offset = ((IndexInfo*)test_buf)->table_offset;

	IndexInfo index = *(IndexInfo*)test_buf;
	uint64_t test_key = ((IndexInfo*)test_buf)->key;
	// printf("%ld %ld %ld\n",test_key,((IndexInfo*)test_buf)->key, index.key);
	assert(test_key == key);
    return item;
}

 bool TxnManager::write_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content, uint64_t cor_id){
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(local_buf, write_content , operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));

	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// yield(h_thd->_routines[0]);
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	

    return true;
}

bool TxnManager::write_remote_index(yield_func_t &yield,uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;

    char *local_buf = Rdma::get_index_client_memory(thd_id);
    ::memset(local_buf, 0, operate_size);
    memcpy(local_buf, write_content , operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();

	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

    return true;
}


uint64_t TxnManager::cas_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id){
    
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_cas(old_value, new_value);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    return *local_buf;
}

#if BATCH_FAA
uint64_t TxnManager::local_record_faa(uint64_t req_key,uint64_t loc,uint64_t pointer){
	pthread_mutex_lock( accum_faa_mutex );
	auto got = accum_faa.find(req_key);
	if(got == accum_faa.end()){  //no such key in map
		faa_info new_info;
		new_info.accum_num = 1;
		new_info.loc = loc;
		new_info.pointer = pointer;
		std::pair<uint64_t, faa_info> newpair (req_key, new_info);		
		auto ret = accum_faa.insert(newpair);
		assert(ret.second != false);
		// if(ret.second==false){ //insert fail, key already exist
		// 	ret.first->second.accum_num++;
		// }
	}
	else{ //key already exist
		got->second.accum_num++;
	}
	pthread_mutex_unlock( accum_faa_mutex );
}
#endif

uint64_t TxnManager::faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t add, uint64_t cor_id){
    // assert(add == (1<<16));
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_fetch_add(add);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    return *local_buf;
}

bool TxnManager::loop_cas_remote(yield_func_t &yield,uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id){
    uint64_t cas_result = -1;
    do{
        cas_result = cas_remote_content(yield,target_server,remote_offset,old_value,new_value,cor_id);
    }
    while(cas_result != old_value && cas_result != new_value && !simulation->is_done());

    return true;
}

row_t * TxnManager::read_remote_row(uint64_t target_server,uint64_t remote_offset){
    uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
    uint64_t thd_id = get_thd_id();
    char *local_buf = Rdma::get_row_client_memory(thd_id);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    row_t *test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

RdmaTxnTableNode * TxnManager::read_remote_timetable(uint64_t target_server,uint64_t remote_offset){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id();
    char *local_buf = Rdma::get_row_client_memory(thd_id);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

RdmaTxnTableNode * TxnManager::read_remote_timetable(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

char * TxnManager::read_remote_txntable(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    // RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    // memcpy(test_row, local_buf, operate_size);

    return local_buf;
}
itemid_t * TxnManager::read_remote_index(uint64_t target_server,uint64_t remote_offset,uint64_t key){
    uint64_t operate_size = sizeof(IndexInfo);
    uint64_t thd_id = get_thd_id();
    char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	assert(((IndexInfo*)test_buf)->key == key);

	item->location = ((IndexInfo*)test_buf)->address;
	item->type = ((IndexInfo*)test_buf)->type;
	item->valid = ((IndexInfo*)test_buf)->valid;
	item->offset = ((IndexInfo*)test_buf)->offset;
  	item->table_offset = ((IndexInfo*)test_buf)->table_offset;

    return item;
}

bool TxnManager::write_remote_row(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(local_buf, write_content , operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    return true;
}

bool TxnManager::write_remote_index(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_index_client_memory(thd_id);
    ::memset(local_buf, 0, operate_size);
    memcpy(local_buf, write_content , operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	INC_STATS(get_thd_id(), rdma_write_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_write_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    return true;
}

bool TxnManager::write_unlock_remote_content(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *local_buf){
    
}

#if CC_ALG == RDMA_MVCC
bool TxnManager::get_version(row_t * temp_row,uint64_t * change_num,Transaction *txn){
    bool result = false;
    uint64_t check_num = temp_row->version_num < HIS_CHAIN_NUM ?  temp_row->version_num : HIS_CHAIN_NUM;
               
    uint64_t k = 0;
    if(temp_row->version_num < HIS_CHAIN_NUM){//find txn matches the version
        for(k = 0;k <= check_num ; k++){
            if((temp_row->start_ts[k] < txn->timestamp || temp_row->start_ts[k] == 0)&& (temp_row->end_ts[k] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = k;
                break;
            }
        }
    }else{
        for( k = 0 ; k < HIS_CHAIN_NUM ; k++){
            uint64_t j = 0;
            j = (temp_row->version_num + k)%HIS_CHAIN_NUM;
            if((temp_row->start_ts[j] < txn->timestamp || temp_row->start_ts[j] == 0) && (temp_row->end_ts[j] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = j;
                break;
            }
        }
    }

    return result;
}
#endif
uint64_t TxnManager::faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t cor_id){
    
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id() + cor_id * g_total_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_fetch_add(1);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    return *local_buf;
}

uint64_t TxnManager::cas_remote_content(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value ){
    
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id();
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_cas(old_value, new_value);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
    RDMA_ASSERT(res_s2 == IOCode::Ok);
    auto res_p2 = rc_qp[target_server][thd_id]->wait_one_comp();
    RDMA_ASSERT(res_p2 == IOCode::Ok);

    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    return *local_buf;
}

bool TxnManager::loop_cas_remote(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value){
    uint64_t cas_result = -1;
    do{
        cas_result = cas_remote_content(target_server,remote_offset,old_value,new_value);
    }
    while(cas_result != old_value && cas_result != new_value && !simulation->is_done());

    return true;
}

RC TxnManager::preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc,uint64_t* wid){
    Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = test_row;
    this->last_type = type;

    RC rc = RCOK;
	rc = test_row->remote_copy_row(test_row, this, access);
    assert(test_row->get_primary_key() == access->data->get_primary_key());
    if (rc == Abort || rc == WAIT) {
        DEBUG_M("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

    access->type = type;

#if CC_ALG == RDMA_SILO || CC_ALG == RDMA_MOCC
    access->orig_row = test_row;
	access->tid = last_tid;
	access->timestamp = test_row->timestamp;
    access->key = test_row->get_primary_key();
    access->location = loc;
	access->offset = m_item->offset;	
#endif

#if CC_ALG == RDMA_MVCC
    access->orig_row = test_row;
    access->old_version_num = test_row->version_num;//record the locked version by txn_id when write
    access->key = test_row->get_primary_key();
    access->location =loc;
	access->offset = m_item->offset;
#endif

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_TS1 || CC_ALG == RDMA_TS || CC_ALG == RDMA_CNULL || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_DSLR_NO_WAIT
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
#endif

#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK ||  CC_ALG == RDMA_MIX_RANGE_LOCK
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
    access->leaf_offset = m_item->leaf_node_offset;
	// access->lock_type = lock_type;
#endif

#if CC_ALG == RDMA_BAMBOO_NO_WAIT
    access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;

    if(type == RD || type == WR){
        for(int i = 0;i < RETIRE_NUM;i++){
            if(test_row->lock_retire[i] == 0)break;
            // txn->dependency_txn.add(get_txn_id()); // TODO
        }
    }    
#endif

#if CC_ALG == RDMA_TS1
	for (int i = 0; i < CASCADING_LENGTH; i++) {
		access->wid[i] = wid[i];
	}
#endif

#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_CICADA
	access->orig_row = test_row;
	access->key = key;
	access->location = loc;
	access->offset = m_item->offset;
#endif

    row_local = access->data;
    ++txn->row_cnt;

    mem_allocator.free(m_item,0);

    if (type == WR) ++txn->write_cnt;//this->last_type = WR
	txn->accesses.add(access);
	
    return rc;
}

RC TxnManager::preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,double decimal_key,uint64_t loc,uint64_t* wid){
#if DYNAMIC_WORKLOAD
    Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = test_row;
    this->last_type = type;

    RC rc = RCOK;
	rc = test_row->remote_copy_row(test_row, this, access);
    assert(test_row->get_decimal_key() == access->data->get_decimal_key());
    if (rc == Abort || rc == WAIT) {
        DEBUG_M("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

    access->type = type;
#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == RDMA_DOUBLE_RANGE_LOCK || CC_ALG == RDMA_SINGLE_RANGE_LOCK || CC_ALG == RDMA_MIX_RANGE_LOCK
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
    access->leaf_offset = m_item->leaf_node_offset;
#endif

    row_local = access->data;
    ++txn->row_cnt;

    mem_allocator.free(m_item,0);

    if (type == WR) ++txn->write_cnt;
	txn->accesses.add(access);
	
    return rc;
#endif
}

RC TxnManager::preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc, lock_t lock_type){

    Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = test_row;
    this->last_type = type;

    RC rc = RCOK;
	rc = test_row->remote_copy_row(test_row, this, access);
    assert(test_row->get_primary_key() == access->data->get_primary_key());
    if (rc == Abort || rc == WAIT) {
        DEBUG_M("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

    access->type = type;

#if CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_OPT_NO_WAIT2
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
	access->lock_type = lock_type;
#endif

    row_local = access->data;
    ++txn->row_cnt;

    mem_allocator.free(m_item,0);

    if (type == WR) ++txn->write_cnt;//this->last_type = WR
	txn->accesses.add(access);
	
    return rc;
}