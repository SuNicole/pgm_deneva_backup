#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_opt_2pl.h"
#include "global.h"

#if CC_ALG == RDMA_OPT_NO_WAIT || CC_ALG == RDMA_OPT_WAIT_DIE || CC_ALG == RDMA_OPT_NO_WAIT2

void Row_rdma_opt_2pl::init(row_t * row){
	_row = row;
}

#if CC_ALG != RDMA_OPT_NO_WAIT2
RC Row_rdma_opt_2pl::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, lock_t* lock_type, uint64_t req_key) {  //本地加锁

    RC rc = RCOK;
    uint64_t loc = g_node_id;
    uint64_t lock_info_pointer = row_t::get_lock_info_pointer((char*)_row - rdma_global_buffer);
    uint64_t conflict_pointer = (char*)_row - rdma_global_buffer;
    uint64_t tts = txn->get_timestamp();
    *lock_type = LOCK_NONE;

#if CC_ALG == RDMA_OPT_NO_WAIT
    uint64_t new_lock_info = txn->get_txn_id() + 1;
    if (type == RD || type == SCAN) {
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,new_lock_info,cor_id);
        if (try_lock != 0 ||_row->lock_info != new_lock_info) {
#if !ALL_ES_LOCK
#if BATCH_FAA
            txn->local_record_faa(req_key, loc, conflict_pointer);
#else
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
#endif
#endif
            // printf("txn %ld lock failed try_lock = %ld now it is %ld\n", new_lock_info, try_lock, _row->lock_info);
            INC_STATS(txn->get_thd_id(), opt_no_wait_abort1, 1);
            return Abort;
        }
#if !ALL_ES_LOCK
        if(_row->rcnt_pos - _row->rcnt_neg >0){
#if BATCH_FAA
            txn->local_record_faa(req_key, loc, conflict_pointer);
#else            
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
#endif
        } 
#endif

        if (_row->is_hot) {
            _row->rcnt_pos ++;
            _row->lock_info = 0;
            *lock_type = DLOCK_SH;
        } else if(_row->rcnt_pos - _row->rcnt_neg > 0) {
            _row->lock_info = 0;
            INC_STATS(txn->get_thd_id(), opt_no_wait_abort2, 1);
            return Abort;
        } else {
            *lock_type = DLOCK_EX;
        }
    } else if (type == WR) {
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,new_lock_info,cor_id);
        if (try_lock != 0 ||_row->lock_info != new_lock_info) {
            // txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            // printf("txn %ld lock failed try_lock = %ld now it is %ld\n", new_lock_info, try_lock, _row->lock_info);
            INC_STATS(txn->get_thd_id(), opt_no_wait_abort3, 1);
            return Abort;
        }
        if(_row->rcnt_pos - _row->rcnt_neg > 0) {
            // txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
            _row->lock_info = 0;
            INC_STATS(txn->get_thd_id(), opt_no_wait_abort4, 1);
            return Abort;
        }
        *lock_type = DLOCK_EX;
    }
#endif
#if CC_ALG == RDMA_OPT_WAIT_DIE
    if (type == RD || type == SCAN) {
        row_t * test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
read_wait_here:
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,1,cor_id);
        //local read row
        memcpy(test_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
        if (try_lock > 1) { //already locked by write
            //wait or abort
            if(tts < try_lock && !simulation->is_done()){ //wait
                goto read_wait_here;
            }else{ //abort
            	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                return Abort;
            }            
        }
        else if(try_lock == 1){ //already locked by read
#if !ALL_ES_LOCK
#if BATCH_FAA
            txn->local_record_faa(req_key, loc, conflict_pointer);
#else
            txn->faa_remote_content(yield,loc,conflict_pointer,cor_id);
#endif
#endif
            // if(test_row->is_hot){ // wait
            //     goto read_wait_here;
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
                goto read_wait_here;
            }else{ //abort
                mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                return Abort;					
            }

            // }
        }
        //cas success
		bool ts_updated = false;
        if(test_row->is_hot){
            int iter;
			for(iter=0;iter<LOCK_LENGTH;iter++){
				if(test_row->ts[iter]==0){
                    test_row->ts[iter] = tts;
					break;
				}
			}
			if(iter == LOCK_LENGTH){ //no empty place in ts array
				//unlock and abort
                _row->lock_info = 0;
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;						
			}
            _row->rcnt_pos++;
            _row->ts[iter] = tts;
            _row->lock_info = 0;
            ts_updated = true;
            *lock_type = DLOCK_SH;
        }else if(test_row->rcnt_pos-test_row->rcnt_neg > 0){
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
                    memcpy(test_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));                    
				}
				if(test_row->rcnt_pos - test_row->rcnt_neg > 0){ //simulation is done
					//unlock and abort
                    _row->lock_info = 0;
                    mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                    return Abort;	
				}                
				//now : test_row->rcnt_pos - test_row->rcnt_neg = 0
            }else{ //unlock and abort
                _row->lock_info = 0;
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
                _row->lock_info = 0;                
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort; 				
			}
            _row->ts[iter] = tts;
            *lock_type = DLOCK_EX;
        }
        mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
#if DEBUG_PRINTF
		printf("---thread id: %lu, local lock read suc, lock location: %lu; %p, txn id: %lu\n", txn->get_thd_id(), loc, &_row, txn->get_txn_id());
#endif
    }
    else if (type == WR) {
        row_t * test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
write_wait_here:
        uint64_t try_lock = txn->cas_remote_content(yield,loc,lock_info_pointer,0,tts,cor_id);
        //local read row
        memcpy(test_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
        if (try_lock > 1) { //already locked by write
            //wait or abort 
            if(tts < try_lock && !simulation->is_done()){ //wait
                goto write_wait_here;
            }else{ //abort
            	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                return Abort;
            }            
        }
        else if(try_lock == 1){ //already locked by read
            // if(test_row->is_hot){ // wait
            //     goto write_wait_here;
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
                goto write_wait_here;
            }else{ //abort
                mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                return Abort;					
            }

            // }
        }
        //cas success
        if(test_row->rcnt_pos-test_row->rcnt_neg > 0){
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
			if(tts < min_lock_ts && !simulation->is_done()){ //unlock and wait
                _row->lock_info = 0;
                goto write_wait_here;
				// while(test_row->rcnt_pos - test_row->rcnt_neg > 0 && !simulation->is_done()){
                //     memcpy(test_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// }
				// if(test_row->rcnt_pos - test_row->rcnt_neg > 0){ //simulation is done
				// 	//unlock and abort
                //     _row->lock_info = 0;
                //     mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                //     return Abort;
				// }     
				//now : test_row->rcnt_pos - test_row->rcnt_neg = 0
			}else{ //unlock and abort
                _row->lock_info = 0;
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;						
			}
        }       
        *lock_type = DLOCK_EX;
        mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
#if DEBUG_PRINTF
		printf("---thread i: %lu, local lock write suc, lock location: %lu; %p, txn id: %lu\n", txn->get_thd_id(), loc, &_row, txn->get_txn_id());
#endif        
    }
#endif
	return rc;
}

#else
RC Row_rdma_opt_2pl::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id, lock_t* lock_type, uint64_t req_key) {  //本地加锁
    RC rc = RCOK;
    uint64_t loc = g_node_id;
    uint64_t tts = txn->get_timestamp();
    *lock_type = LOCK_NONE;
    bool succ_lock = false;

    uint64_t new_lock_info;
    if (type == RD || type == SCAN) {
        uint64_t lock_info = _row->lock_info;
        uint64_t old_lock_info = 0;
        uint64_t try_lock = -1;
        uint64_t row_offset = (char*)_row - rdma_global_buffer;//TODO check
        if(_row->is_hot){
            while(!succ_lock && !simulation->is_done()){
                uint64_t read_lock_cnt = row_t::decode_lock_info_cnt(lock_info);
                uint64_t lock_num = row_t::decode_lock_info_type(lock_info);
                // printf("[row_rdma_opt_2pl.cpp:311]read_lock_cnt = %ld,lock_num = %ld\n",read_lock_cnt,lock_num);
                if(lock_num == 1 || lock_num == 2){
                    if(lock_num == 1)txn->faa_remote_content(yield,loc,row_offset,1,cor_id);
                    INC_STATS(txn->get_thd_id(), opt_no_wait_abort5, 1);
                    return Abort;
                }

                if(lock_num == 0){
                    new_lock_info = 7; //TODO - chcek 0000...001 11
                    old_lock_info = 0;
                }else if(lock_num == 3){
                    new_lock_info = (read_lock_cnt + 1)<<2; 
                    new_lock_info = (new_lock_info | 0X03); //TODO - chcek 0000...001 11
                    old_lock_info = lock_info;
                }
                try_lock = txn->cas_remote_content(yield,loc,row_offset + sizeof(uint64_t)*2,old_lock_info,new_lock_info,cor_id);
                if(try_lock == old_lock_info){
                    succ_lock = true;
                }else{
                    lock_info = try_lock;
                }
            }

            txn->faa_remote_content(yield,loc,row_offset,1,cor_id);
            // printf("[row_rdma_opt_2pl.cpp:335]_row->lock_info = %ld\n",_row->lock_info);
            *lock_type = DLOCK_SH;
		    return rc; 
        }else{
            uint64_t lock_num = row_t::decode_lock_info_type(lock_info);
            if(lock_num != 0){
                if(lock_num == 3 || lock_num == 2)txn->faa_remote_content(yield,loc,row_offset,1,cor_id);
                // if(!(stats._stats[txn->get_thd_id()]->opt_no_wait_abort6%9))printf("[row_rdma_opt_2pl.cpp:342]lock_info = %ld,lock_type = %ld\n",lock_info,lock_num);
                INC_STATS(txn->get_thd_id(), opt_no_wait_abort6, 1);
                return Abort;
            }else{
                uint64_t new_lock_info = 2;//TODO  0...0 10
                try_lock = txn->cas_remote_content(yield,loc,row_offset+sizeof(uint64_t)*2,0,new_lock_info,cor_id);

                if(try_lock != 0){
                   INC_STATS(txn->get_thd_id(), opt_no_wait_abort7, 1);
                   return Abort;
                }
                
                // printf("[row_rdma_opt_2pl.cpp:354]_row->lock_info = %ld\n",_row->lock_info);
                *lock_type = DLOCK_EX;
		        return rc; 
            }
        }                                                                                       
    } else if (type == WR) {
        uint64_t try_lock = -1;
        uint64_t row_offset = (char*)_row - rdma_global_buffer;//TODO check
        try_lock = txn->cas_remote_content(yield,loc,row_offset + sizeof(uint64_t)*2,0,1,cor_id);
        if(try_lock != 0){
            INC_STATS(txn->get_thd_id(), opt_no_wait_abort8, 1);
            // if(!(stats._stats[txn->get_thd_id()]->opt_no_wait_abort8%13))printf("[row_rdma_opt_2pl.cpp:368]lock_info = %ld,lock_type = %ld,try_lock = %ld\n",_row->lock_info,row_t::decode_lock_info_type(_row->lock_info),try_lock);

            return Abort;
        }

        // printf("[row_rdma_opt_2pl.cpp:366]local write lock key = %ld\n",_row->get_primary_key());
        
        *lock_type = DLOCK_EX;
		return rc;
    }
}
#endif

#endif