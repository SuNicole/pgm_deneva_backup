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
#include <map>  
#include <unordered_map>

#include "global.h"
#include "config.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_rdma.h"
#include "index_rdma_btree.h"
#include "transport/rdma.h"
#include "catalog.h"
#include "manager.h"
#include "row.h"
#include "row_lock.h"
#include "row_opt_no_wait3.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_rdma_mvcc.h"
#include "row_rdma_2pl.h"
#include "row_rdma_opt_2pl.h"
#include "row_rdma_ts1.h"
#include "row_rdma_ts.h"
#include "row_rdma_cicada.h"
#include "rdma_mvcc.h"
#include "rdma_ts1.h"
#include "rdma_ts.h"
#include "rdma_null.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"
#include "stats.h"

void YCSBTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
    finished_server_count = 0;
	_wl = (YCSBWorkload *) h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
  TxnManager::reset();
}

RC YCSBTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
  assert(ycsb_query->requests.size() == g_req_per_query);
  assert(phase == CALVIN_RW_ANALYSIS);
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		ycsb_request * req = ycsb_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
	DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype,
		  req->key, GET_NODE_ID(part_id));
	if (GET_NODE_ID(part_id) != g_node_id) continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		RC rc2 = get_lock(row,req->acctype);
	if(rc2 != RCOK) {
	  rc = rc2;
	}
	}
  if(decr_lr() == 0) {
	if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  /*
  if(rc == WAIT && lock_ready_cnt == 0) {
	if(ATOM_CAS(lock_ready,false,true))
	//lock_ready = true;
	  rc = RCOK;
  }
  */
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  locking_done = true;
  return rc;
}

RC YCSBTxnManager::run_txn(yield_func_t &yield, uint64_t cor_id) {

	RC rc = RCOK;
	assert(CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN);

	if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DEBUG_PRINTF
		// printf("[txn start]txn：%d，ts：%lu\n",txn->txn_id,get_timestamp());
#endif
		//query->print();
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
	}
	
	uint64_t starttime = get_sys_clock();

#if BATCH_INDEX_AND_READ
	//batch read all index for remote access
	ycsb_batch_read(yield,R_INDEX,cor_id);
	//batch read all row for remote access
	ycsb_batch_read(yield,R_ROW,cor_id);
#endif

#if CC_ALG == RDMA_OPT_NO_WAIT3 || CC_ALG == OPT_NO_WAIT3
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
    if(ycsb_query->query_type == YCSB_CONTINUOUS){
        if(rdma_one_side()){
            // if((get_sys_clock() - simulation->run_starttime) >= g_warmup_timer)printf("[ycsb_txn.cpp:144]continuous txn\n");
            rc = run_continuous_txn(yield,cor_id);
        }else{
            // if((get_sys_clock() - simulation->run_starttime) >= g_warmup_timer)printf("[ycsb_txn.cpp:144]continuous txn\n");
            rc = tcp_run_continuous_txn(yield,cor_id);//只有本地范围查询事务有该操作
        }
    }else{
        // if((get_sys_clock() - simulation->run_starttime) >= g_warmup_timer)printf("[ycsb_txn.cpp:148]normal txn\n");
        while(rc == RCOK && !is_done()) {
            rc = run_txn_state(yield, cor_id);
        }
    }
#else
	while(rc == RCOK && !is_done()) {
#if CC_ALG == WOUND_WAIT
		if (txn_state == WOUNDED) {
			rc = Abort;
			break;
		}  
#endif
		rc = run_txn_state(yield, cor_id);
	}
#endif
#if CC_ALG == WOUND_WAIT
	if (txn_state == WOUNDED) 
		rc = Abort;
#endif
#if BATCH_INDEX_AND_READ
	reqId_index.erase(reqId_index.begin(),reqId_index.end());
	reqId_row.erase(reqId_row.begin(),reqId_row.end());
#endif
    if(rc == Abort) total_num_atomic_retry++;
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
//RDMA_SILO:logic?
#if CC_ALG == OPT_NO_WAIT3
     if(ycsb_query->query_type == YCSB_CONTINUOUS){
        if(rc == Abort){
            // printf("[ycsb_txn.cpp:181]abort\n");
            if(IS_LOCAL(get_txn_id()) || g_node_cnt == 1){
                abort(yield, cor_id);//在本地执行时就失败，无需发送到远程，直接回滚
                return Abort;
            }else{
            //    return rc;//在本地外的节点执行失败，通知调度节点回滚
            }
        }else{
            // printf("[ycsb_txn.cpp:189]commit\n");
            if(g_node_cnt == 1){//只有一个节点，本地执行成功即成功，直接提交
                // rc = sigle_commit_continuous(yield, cor_id);
                rc = start_commit(yield, cor_id);
                // printf("[ycsb_txn.cpp:191]sigle commit\n");
            }else{
                //需要等待，无需继续操作
            }
        }
     }else{
        if(IS_LOCAL(get_txn_id())) {  //for one-side rdma, must be local
            if(is_done() && rc == RCOK) {
                rc = start_commit(yield, cor_id);
            }
            else if(rc == Abort)
                rc = start_abort(yield, cor_id);
	    } else if(rc == Abort){
		    rc = abort(yield, cor_id);
	    }
     }
#elif CC_ALG == RDMA_OPT_NO_WAIT3
     if(ycsb_query->query_type == YCSB_CONTINUOUS){
        if(rc == Abort){
            // printf("[ycsb_txn.cpp:212]continuous abort\n");
            rc = abort(yield, cor_id);
        }else if(rc == RCOK){
            // if((get_sys_clock() - simulation->run_starttime) >= g_warmup_timer)printf("[ycsb_txn.cpp:215]continuous commit\n");
            rc = start_commit(yield, cor_id);
        }
     }else{
        if(IS_LOCAL(get_txn_id())) {  //for one-side rdma, must be local
            if(is_done() && rc == RCOK) {
                rc = start_commit(yield, cor_id);
            }
            else if(rc == Abort)
		        rc = start_abort(yield, cor_id);
	    } else if(rc == Abort){
		    rc = abort(yield, cor_id);
	    }
     }
#else
	if(IS_LOCAL(get_txn_id())) {  //for one-side rdma, must be local
		if(is_done() && rc == RCOK) {
			// printf("a txn is done\n");
#if CC_ALG == WOUND_WAIT
      		txn_state = STARTCOMMIT;
#endif
			rc = start_commit(yield, cor_id);
		}
		else if(rc == Abort)
		rc = start_abort(yield, cor_id);
	} else if(rc == Abort){
		rc = abort(yield, cor_id);
	}
#endif	
  return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
	uint64_t starttime = get_sys_clock();
	get_row_post_wait(row);
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	next_ycsb_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - curr_time);
	return RCOK;
}

bool YCSBTxnManager::is_done() { 
	return next_record_id >= ((YCSBQuery*)query)->requests.size();
}

void YCSBTxnManager::next_ycsb_state() {
  switch(state) {
	case YCSB_0:
	  state = YCSB_1;
	  break;
	case YCSB_1:
	  next_record_id++;
	  if(send_RQRY_RSP || !IS_LOCAL(txn->txn_id) || !is_done()) {
		state = YCSB_0;
	  } else {
		state = YCSB_FIN;
	  }
	  break;
	case YCSB_FIN:
	  break;
	default:
	  assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((YCSBQuery*)query)->requests[idx]->key)) == g_node_id;
}

#if BATCH_INDEX_AND_READ
void YCSBTxnManager::ycsb_batch_read(yield_func_t &yield,BatchReadType rtype, uint64_t cor_id){
  	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	vector<vector<uint64_t>> remote_index(g_node_cnt);

	for(int i=0;i<ycsb_query->requests.size();i++){
		ycsb_request * req = ycsb_query->requests[i];
		uint64_t part_id = _wl->key_to_part( req->key );
		uint64_t loc = GET_NODE_ID(part_id);
		if(loc != g_node_id){  //remote
			remote_index[loc].push_back(i);
		}
	}
	for(int i=0;i<g_node_cnt;i++){
		if(remote_index[i].size()>0){
			batch_read(yield, rtype, i, remote_index, cor_id);
		}
	}
	for(int i=0;i<g_node_cnt;i++){
		if(remote_index[i].size()>0){
			get_batch_read(yield, rtype,i, remote_index, cor_id);
		}
	}
 }
#endif

itemid_t* YCSBTxnManager::ycsb_read_remote_index(yield_func_t &yield, ycsb_request * req, uint64_t cor_id) {
	uint64_t part_id = _wl->key_to_part( req->key );
  	uint64_t loc = GET_NODE_ID(part_id);
	// printf("loc:%d and g_node_id:%d\n", loc, g_node_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();
    itemid_t* item;
#if RDMA_ONE_SIDE
    #if INDEX_STRUCT != IDX_RDMA_BTREE
        //get corresponding index
    // uint64_t index_key = 0;
        uint64_t index_key = req->key / g_node_cnt;
        uint64_t index_addr = (index_key) * sizeof(IndexInfo);
        uint64_t index_size = sizeof(IndexInfo);
        item = read_remote_index(yield, loc, index_addr,req->key, cor_id);
    #else
        item = read_remote_btree_index(yield, loc,req->key, cor_id);
    #endif
#endif
	return item;
}

RC YCSBTxnManager::send_remote_one_side_request(yield_func_t &yield, ycsb_request * req, row_t *& row_local, uint64_t cor_id) {
	// get the index of row to be operated
	
	itemid_t * m_item;
#if BATCH_INDEX_AND_READ
	m_item = reqId_index.find(next_record_id)->second;
#else
    m_item = ycsb_read_remote_index(yield, req, cor_id);
#endif
	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
    
    RC rc = RCOK;
    uint64_t version = 0;

	rc = get_remote_row(yield, req->acctype, loc, m_item, row_local, cor_id);
	// mem_allocator.free(m_item, sizeof(itemid_t));
	return rc;
}


RC YCSBTxnManager::send_remote_request() {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
#if USE_RDMA == CHANGE_MSG_QUEUE
	tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, Message::create_message(this,RQRY));
#else
    // DEBUG("ycsb send remote request %ld, %ld\n",txn->txn_id,txn->batch_id);
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
#endif

	return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
#if !MIX_WORKLOAD
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	//msg->requests.init(ycsb_query->requests.size());
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
	while (next_record_id < ycsb_query->requests.size() && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
	#else
	while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
			GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
	#endif
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
	}
#else
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
    if(ycsb_query->query_type == YCSB_CONTINUOUS){
        uint64_t dest_node_id = next_record_id;
        for(int i = 0;i < g_req_per_query;i++){
            msg->requests.add(ycsb_query->requests[i]);
        }
    }else{
        YCSBQuery* ycsb_query = (YCSBQuery*) query;
        //msg->requests.init(ycsb_query->requests.size());
        uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
        #if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
        while (next_record_id < ycsb_query->requests.size() && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
        #else
        while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
                GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
        #endif
            YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
        }
    }
        
    
#endif
}

RC YCSBTxnManager::run_continuous_txn(yield_func_t &yield, uint64_t cor_id) {
#if CC_ALG == RDMA_OPT_NO_WAIT3
    RC rc = RCOK;
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
    ycsb_request * req = ycsb_query->requests[0];
    uint64_t first_key = req->key;
    uint64_t last_key = req->key + g_req_per_query - 1;


    uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t remote_server = GET_NODE_ID(part_id);//location server of first_key 

    rdma_bt_node * left_range_index_node;
    for(int i = 0;i < g_node_cnt;i++){
        uint64_t range_first_key = req->key;

        // int64_t start_offset = (i - remote_server + g_node_cnt) % g_node_cnt;
        // range_first_key = start_offset + range_first_key;

        if(i >= remote_server)range_first_key = range_first_key + (i - remote_server);
        else{
            range_first_key = range_first_key+ (i - remote_server);
            if(range_first_key < 0)range_first_key = 0;
            //TODO - what is the start key of data?
        }
        if(i != g_node_id){
            uint64_t left_range_node_offset = 0;
            left_range_index_node = read_left_index_node(yield,cor_id,i,range_first_key,left_range_node_offset);

            UInt32 num_of_key = left_range_index_node->num_keys;
            assert(left_range_index_node->keys[num_of_key - 1] >= range_first_key);
            assert(left_range_index_node->keys[0] <= range_first_key);

            if(s_lock_content(left_range_index_node->intent_lock)){
                printf("[ycsb_txn.cpp:437]\n");
                // printf("[ycsb_txn.cpp:438]intent_lock=%ld,IS=%ld,IX=%ld,S=%ld,X=%ld\n",left_range_index_node->intent_lock,decode_is_lock(left_range_index_node->intent_lock),decode_ix_lock(left_range_index_node->intent_lock),decode_s_lock(left_range_index_node->intent_lock),decode_x_lock(left_range_index_node->intent_lock));
                // printf("[ycsb_txn.cpp:439]num_key=%ld,[0]=%ld,[num]=%ld,req->key=%ld\n",left_range_index_node->num_keys,left_range_index_node->keys[0],left_range_index_node->keys[left_range_index_node->num_keys-1],req->key);
                return Abort;
            }

            //acquire S lock on range
            rdma_bt_node * origin_bt_node = read_remote_bt_node(yield,i,left_range_node_offset,cor_id);
            assert(origin_bt_node->keys[0] == left_range_index_node->keys[0]);

            uint64_t add_value = 1;
            add_value = add_value<<16;//0x0010
            uint64_t before_faa = 0;
            before_faa = faa_remote_content(yield,i,left_range_node_offset,add_value,cor_id);

            // rdma_bt_node * remote_bt_node = read_remote_bt_node(yield,i,left_range_node_offset,cor_id);
            // uint64_t tmp_intent = remote_bt_node->intent_lock;
            // if(decode_x_lock(tmp_intent) !=0 ){
            //     // printf("[ycsb_txn.cpp:452]origin=%ld,before_faa=%ld,intent=%ld,IS=%ld,IX=%ld,s=%ld,x=%ld,faa_value=%ld\n",origin_bt_node->intent_lock,before_faa,tmp_intent,decode_is_lock(tmp_intent),decode_ix_lock(tmp_intent),decode_s_lock(tmp_intent),decode_x_lock(tmp_intent),add_value);
            // }


            if(s_lock_content(before_faa)){
                add_value = -1;
                add_value = add_value<<16;//0x0010
                before_faa = faa_remote_content(yield,i,left_range_node_offset,add_value,cor_id);
                printf("[ycsb_txn.cpp:467]\n");
                return Abort;
            }

            //record
            txn->range_node_set[txn->locked_range_num] = left_range_node_offset;
            txn->server_set[txn->locked_range_num] = i;
            txn->locked_range_num = txn->locked_range_num + 1;

            int j = 0;
            for(j = 0;j < num_of_key;j++){
                if(left_range_index_node->keys[j] < first_key)continue;
                if(left_range_index_node->keys[j] > last_key)break;
                uint64_t remote_offset = left_range_index_node->child_offsets[j];
                row_t * test_row = read_remote_row(yield,i,remote_offset,cor_id);
                itemid_t *m_item = (itemid_t *)malloc(sizeof(itemid_t));
                m_item->offset = remote_offset;
                rc = preserve_access(row,m_item,test_row,RD,test_row->get_primary_key(),i);
                // printf("[ycsb_txn.cpp:466]req_key = %ld , row->key = %ld\n",req->key,test_row->get_primary_key());

            }

            rdma_bt_node * next_index_node = left_range_index_node;
            // while(next_index_node->keys[next_index_node->num_keys - 1] < last_key){
            while(next_index_node && (next_index_node->next_node_offset != UINT64_MAX)){
                uint64_t remote_offset = next_index_node->next_node_offset;
                next_index_node = read_remote_bt_node(yield,i,remote_offset,cor_id);
                num_of_key = next_index_node->num_keys;
                if(next_index_node->keys[0] > last_key)break;
                assert(next_index_node->keys[0] > range_first_key);

                if(s_lock_content(next_index_node->intent_lock)){
                    printf("[ycsb_txn.cpp:499]\n");
                    return Abort;
                }

                //acquire S lock on range
                uint64_t add_value = 1;
                add_value = add_value<<16;//0x0010
                uint64_t before_faa = faa_remote_content(yield,i,remote_offset,add_value,cor_id);

                if(s_lock_content(next_index_node->intent_lock)){
                    add_value = -1;
                    add_value = add_value<<16;//0x0010
                    before_faa = faa_remote_content(yield,i,remote_offset,add_value,cor_id);
                    printf("[ycsb_txn.cpp:512]\n");
                    return Abort;
                }

                txn->range_node_set[txn->locked_range_num] = remote_offset;
                txn->server_set[txn->locked_range_num] = i;
                txn->locked_range_num = txn->locked_range_num + 1;
                
                int j = 0;
                for(j = 0;j < num_of_key;j++){
                    if(next_index_node->keys[j] < range_first_key)continue;
                    if(next_index_node->keys[j] > last_key)break;
                    uint64_t remote_offset = next_index_node->child_offsets[j];
                    row_t * test_row = read_remote_row(yield,i,remote_offset,cor_id);
                    itemid_t *m_item = (itemid_t *)malloc(sizeof(itemid_t));
                    m_item->offset = remote_offset;
                    // printf("[ycsb_txn.cpp:505]req_key = %ld , row->key = %ld\n",req->key,test_row->get_primary_key());
                    rc = preserve_access(row,m_item,test_row,RD,test_row->get_primary_key(),i);
                }
                
            }
            
        }else{
            // printf("[ycsb_txn.cpp:539]in run_continuous_txn\n");
            // continue;
            //TODO local txn
            // printf("[ycsb_txn.cpp:509]req->key = %ld , first_key = %ld\n",req->key,range_first_key);
            rdma_bt_node * leaf_node;
            part_id = _wl->key_to_part(range_first_key);
            leaf_node = index_node_read(_wl->the_index, range_first_key, part_id);
            assert(part_id == g_node_id);
            UInt32 num_of_key = leaf_node->num_keys;
            // while(leaf_node->keys[num_of_key - 1] < last_key){
            while(leaf_node && num_of_key != 0){
                if(leaf_node->keys[0] > last_key || leaf_node->keys[num_of_key - 1] < first_key)break;
                if(s_lock_content(leaf_node->intent_lock)){
                    printf("[ycsb_txn.cpp:547]\n");
                    return Abort;
                }
                //range lock(S)
                uint64_t add_value = 1;
                add_value = add_value<<16;//0x0010
                uint64_t local_offset = (char*)leaf_node - rdma_global_buffer;
                uint64_t before_faa = 0;
                before_faa = faa_remote_content(yield,i,local_offset,add_value,cor_id);

                if(s_lock_content(before_faa)){
                    add_value = -1;
                    add_value = add_value<<16;//0x0010
                    before_faa = faa_remote_content(yield,i,local_offset,add_value,cor_id);
                    printf("[ycsb_txn.cpp:534]\n");
                    return Abort;
                }

                txn->range_node_set[txn->locked_range_num] = (char *)leaf_node - rdma_global_buffer;
                txn->server_set[txn->locked_range_num] = i;
                txn->locked_range_num = txn->locked_range_num + 1;
                //lock got, read data
                for(int j = 0;j < num_of_key;j++){
                        if(leaf_node->keys[j] < first_key)continue;
                        if(leaf_node->keys[j] > last_key)break;
                        row_t * new_row = (row_t *)(rdma_global_buffer + leaf_node->child_offsets[j]);

                        itemid_t *m_item = (itemid_t *)malloc(sizeof(itemid_t));
                        m_item->offset = leaf_node->child_offsets[j];
                        m_item->leaf_node_offset = (char *)leaf_node - rdma_global_buffer;
                        rc = preserve_access(row,m_item,new_row,RD,new_row->get_primary_key(),i);
                        // printf("[ycsb_txn.cpp:545]req_key = %ld , row->key = %ld\n",req->key,new_row->get_primary_key());
                        
                        // rc = get_row(yield,new_row, RD,row,cor_id, req->key,m_item);
                }
                leaf_node = (rdma_bt_node*)(rdma_global_buffer + leaf_node->next_node_offset);
                num_of_key = leaf_node->num_keys;
            }
            // printf("[ycsb_txn.cpp:590]out run_continuous_txn\n");
        }
    }

    return rc;
#endif
}

RC YCSBTxnManager::tcp_local_run_continuous_txn(yield_func_t &yield, uint64_t cor_id) {
    // printf("[ycsb_txn.cpp:544]tcp run continuous txn\n");
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[0];
    
    uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t remote_server = GET_NODE_ID(part_id);//location server of first_key
    uint64_t last_key = req->key + g_req_per_query;
    uint64_t range_first_key = req->key;
    if(g_node_id >= remote_server)range_first_key = range_first_key + (g_node_id - remote_server);
    else{
        range_first_key = range_first_key+ (g_node_id - remote_server);
        if(range_first_key < 0)range_first_key = 0;
            //TODO - what is the start key of data?
    }
    RC rc = RCOK;
    part_id = _wl->key_to_part(range_first_key);
    access_t type = req->acctype;
    itemid_t * m_item;
    m_item = index_read(_wl->the_index, range_first_key, part_id);
    rc = get_continuous_row(yield,cor_id,m_item,range_first_key,last_key);
    //TODO
    // if (INDEX_STRUCT == IDX_RDMA) {
    //     mem_allocator.free(m_item, sizeof(itemid_t));
    // }else if(INDEX_STRUCT == IDX_RDMA_BTREE){
    // }
    return rc;
}

RC YCSBTxnManager::tcp_run_continuous_txn(yield_func_t &yield, uint64_t cor_id) {
    int i = 0;
    // printf("[ycsb_txn.cpp:573]tcp run continuous txn start\n");
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
    RC rc = tcp_local_run_continuous_txn(yield,cor_id);
    if(g_node_cnt == 1 || rc == Abort){
        if(rc == Abort){
            // printf("[ycsb_txn.cpp:577]local lock fail\n");
        }
        return rc;
    }
    finished_server_count++;

    for(i = 0;i < g_node_cnt;i++){
        next_record_id = i;
        
        if(i == g_node_id){
            continue;
        }else{
            uint64_t dest_node_id = i;
            ycsb_query->partitions_touched.add_unique(dest_node_id);
        #if USE_RDMA == CHANGE_MSG_QUEUE
            tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, Message::create_message(this,CRQRY));
        #else
            msg_queue.enqueue(get_thd_id(),Message::create_message(this,CRQRY),dest_node_id);
        #endif
            rsp_cnt ++;
        }
    }
    return WAIT_REM;
}

RC YCSBTxnManager::run_txn_state(yield_func_t &yield, uint64_t cor_id) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  	bool loc = GET_NODE_ID(part_id) == g_node_id;
	
	RC rc = RCOK;
	switch (state) {
	case YCSB_0 :
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
        // printf("read local WOUNDState:%ld\n", rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id));
		if(rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id) == WOUND_ABORTING) {
			rc = Abort;
		} else {
#endif
		if(loc) {//local
			rc = run_ycsb_0(yield,req,row,cor_id);
		} else if (rdma_one_side()) {//remote with rdma
            rc = send_remote_one_side_request(yield, req, row, cor_id);
		} else {//remote without rdma
			rc = send_remote_request();
		}
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
		}
#endif
	  break;
	case YCSB_1 :
		//read local row,for message queue by TCP/IP,write set has actually been written in this point,
		//but for rdma, it was written in local, the remote data will actually be written when COMMIT
		rc = run_ycsb_1(req->acctype,row,yield,cor_id);  
		break;
    case YCSB_CON:

        break;
	case YCSB_FIN :
		state = YCSB_FIN;
		break;
	default:
		assert(false);
  }

  if (rc == RCOK) next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(yield_func_t &yield,ycsb_request * req,row_t *& row_local,uint64_t cor_id) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
  int part_id = _wl->key_to_part( req->key );
  access_t type = req->acctype;
  itemid_t * m_item;
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  m_item = index_read(_wl->the_index, req->key, part_id);
  assert(m_item->parent != NULL);
  starttime = get_sys_clock();
  row_t * row = ((row_t *)m_item->location);
//   printf("[ycsb_txn.cpp:656]row->key = %ld,req->key = %ld\n",row->get_primary_key(),req->key);
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  rc = get_row(yield,row, type,row_local,cor_id, req->key,m_item);
  if (INDEX_STRUCT == IDX_RDMA) {
    mem_allocator.free(m_item, sizeof(itemid_t));
  }else if(INDEX_STRUCT == IDX_RDMA_BTREE){
      //TODO
  }
  return rc;
}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t * row_local,yield_func_t &yield, uint64_t cor_id) {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  if (acctype == RD || acctype == SCAN) {
	int fid = 0;
	char * data = row_local->get_data();
	uint64_t fval __attribute__ ((unused));
	fval = *(uint64_t *)(&data[fid * 100]); //read fata and store to fval
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED || CC_ALG == RDMA_BAMBOO_NO_WAIT
	// Release lock after read
	release_last_row_lock(yield,cor_id,rc);
#endif

  } 
  else {
	assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0; //write data, set data[0]=0
#if YCSB_ABORT_MODE
	if (data[0] == 'a') return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED || CC_ALG == RDMA_BAMBOO_NO_WAIT
	// Release lock after write
	release_last_row_lock(yield,cor_id,rc);
#endif
  }
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
//   return RCOK;
    return rc;
}

RC YCSBTxnManager::run_calvin_txn(yield_func_t &yield,uint64_t cor_id) {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
	DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
	switch(this->phase) {
	  case CALVIN_RW_ANALYSIS:

		// Phase 1: Read/write set analysis
		calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
		if(query->participant_nodes[g_node_id] == 1) {
		  calvin_expected_rsp_cnt--;
		}
#else
		calvin_expected_rsp_cnt = 0;
#endif
		DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
			  calvin_expected_rsp_cnt);

		this->phase = CALVIN_LOC_RD;
		break;
	  case CALVIN_LOC_RD:
		// Phase 2: Perform local reads
		DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
		rc = run_ycsb(yield,cor_id);
		//release_read_locks(query);

		this->phase = CALVIN_SERVE_RD;
		break;
	  case CALVIN_SERVE_RD:
		// Phase 3: Serve remote reads
		// If there is any abort logic, relevant reads need to be sent to all active nodes...
		if(query->participant_nodes[g_node_id] == 1) {
		  rc = send_remote_reads();
		}
		if(query->active_nodes[g_node_id] == 1) {
		  this->phase = CALVIN_COLLECT_RD;
		  if(calvin_collect_phase_done()) {
			rc = RCOK;
		  } else {
			DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id,
				  txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
			rc = WAIT;
		  }
		} else { // Done
		  rc = RCOK;
		  this->phase = CALVIN_DONE;
		}

		break;
	  case CALVIN_COLLECT_RD:
		// Phase 4: Collect remote reads
		this->phase = CALVIN_EXEC_WR;
		break;
	  case CALVIN_EXEC_WR:
		// Phase 5: Execute transaction / perform local writes
		DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
		rc = run_ycsb(yield,cor_id);
		this->phase = CALVIN_DONE;
		break;
	  default:
		assert(false);
	}

  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
}

RC YCSBTxnManager::run_ycsb(yield_func_t &yield,uint64_t cor_id) {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  ycsb_request * req = ycsb_query->requests[i];
	if (this->phase == CALVIN_LOC_RD && req->acctype == WR) continue;
	if (this->phase == CALVIN_EXEC_WR && req->acctype == RD) continue;

		uint64_t part_id = _wl->key_to_part( req->key );
	bool loc = GET_NODE_ID(part_id) == g_node_id;

	if (!loc) continue;

	rc = run_ycsb_0(yield,req,row,cor_id);
	assert(rc == RCOK);

	rc = run_ycsb_1(req->acctype,row,yield,cor_id);
	assert(rc == RCOK);
  }
  return rc;

}

