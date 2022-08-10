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
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "row_opt_no_wait3.h"
#include "index_btree.h"

void Row_opt_no_wait::init(row_t * row) {
    _row = row;
}

RC Row_opt_no_wait::lock_get(access_t type, itemid_t *m_item,TxnManager * txn){
    bt_node *range_node =(bt_node*)(m_item->parent);
    row_t *row = (row_t*)(m_item->location);
    uint64_t lock = range_node->intent_lock;
    if(type == RD){
        //1.lock range(IS)
        // printf("[row_opt_no_wait3.cpp:36]intent_lock = %ld,key = %ld\n",intent_lock,row->get_primary_key());
        if(txn->is_lock_content(lock)){
			// INC_STATS(txn->get_thd_id(), is_content_abort, 1);
            // printf("[row_opt_no_wait3.cpp:37]is content abort\n");
            return Abort;
        }
            //acquire is lock
        uint64_t add_value = 1;
        add_value = add_value<<48;
        uint64_t faa_result = 0;
        // ATOM_FETCH_ADD(range_node->intent_lock,add_value);

        if(txn->is_lock_content(faa_result)){
			// INC_STATS(txn->get_thd_id(), is_content_abort2, 1);
            add_value = -1;
            add_value = add_value<<48;
            faa_result = 0;
            // ATOM_FETCH_ADD(range_node->intent_lock,add_value);
            // printf("[row_opt_no_wait3.cpp:48]is content abort\n");
            return Abort;
        }

        //2.lock data(S)
        lock = row->_tid_word;
    //     if(intent_lock != 0)printf("[row_opt_no_wait3.cpp:55]lock = %ld,key = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",intent_lock,row->get_primary_key(),txn->decode_is_lock(intent_lock),txn->decode_ix_lock(intent_lock),txn->decode_s_lock(intent_lock),txn->decode_x_lock(intent_lock));
        if(txn->s_lock_content(lock)){
            // printf("[row_opt_no_wait3.cpp:59]lock = %ld,key = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",lock,row->get_primary_key(),txn->decode_is_lock(lock),txn->decode_ix_lock(lock),txn->decode_s_lock(lock),txn->decode_x_lock(lock));
            add_value = -1;
            add_value = add_value<<48;
            faa_result = 0;
            // ATOM_FETCH_ADD(range_node->intent_lock,add_value);
            // printf("[row_opt_no_wait3.cpp:57]s content abort\n");
            return Abort;
        }

        add_value = 1;
        add_value = add_value<<16;
        faa_result = ATOM_FETCH_ADD(row->_tid_word,add_value);
        if(txn->s_lock_content(faa_result)){
            add_value = (-1)<<48;
            faa_result = 0;
            // ATOM_FETCH_ADD(range_node->intent_lock,add_value);
            add_value = (-1)<<16;
            faa_result = 0;
            // ATOM_FETCH_ADD(row->_tid_word,add_value);
            // printf("[row_opt_no_wait3.cpp:68]s content abort\n");
            return Abort;
        }

        faa_result = row->_tid_word;
        // sif(faa_result != 0)printf("[row_opt_no_wait3.cpp:75]lock = %ld,key = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",faa_result,row->get_primary_key(),txn->decode_is_lock(faa_result),txn->decode_ix_lock(faa_result),txn->decode_s_lock(faa_result),txn->decode_x_lock(faa_result));


        // record_intent_lock *record = (record_intent_lock*)mem_allocator.alloc(sizeof(record_intent_lock));
        // record->bt_node_location = range_node;
        // record->server_id = g_node_id;
        // txn->txn->locked_node.add(record);

        return RCOK;
    }else if(type == WR){
        // printf("[row_opt_no_wait3.cpp:89]write txn\n");
        // printf("[row_opt_no_wait3.cpp:80]is = %ld,ix = %ld,s = %ld,x = %ld\n",txn->decode_is_lock(intent_lock),txn->decode_ix_lock(intent_lock),txn->decode_s_lock(intent_lock),txn->decode_x_lock(intent_lock));
        //lock range(IX)
        if(txn->ix_lock_content(lock)){
			// INC_STATS(txn->get_thd_id(), ix_content_abort, 1);
            // if(intent_lock != 0)printf("[row_opt_no_wait3.cpp:82]ix content abort\n");
            // printf("[row_opt_no_wait3.cpp:96]lock = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",lock,txn->decode_is_lock(lock),txn->decode_ix_lock(lock),txn->decode_s_lock(lock),txn->decode_x_lock(lock));
        //lock range(IX)
            return Abort;
        }

        uint64_t faa_num = 1;
        faa_num = faa_num<<32;
        uint64_t faa_result = 0;
        // ATOM_FETCH_ADD(range_node->intent_lock,faa_num);

        if(txn->ix_lock_content(faa_result)){
            faa_num = -1;
            faa_num = faa_num<<32;
            uint64_t faa_result = 0; 
            // ATOM_FETCH_ADD(range_node->intent_lock,faa_num);
            // printf("[row_opt_no_wait3.cpp:92]ix content abort\n");
            return Abort;
        }
        //lock data(X)
        lock = row->_tid_word;
        if(txn->x_lock_content(lock)){
            // printf("[row_opt_no_wait3.cpp:115]lock = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",lock,txn->decode_is_lock(lock),txn->decode_ix_lock(lock),txn->decode_s_lock(lock),txn->decode_x_lock(lock));
			// INC_STATS(txn->get_thd_id(), x_content_abort, 1);
            faa_num = -1;
            faa_num = faa_num<<32;
            faa_result = 0;
            // ATOM_FETCH_ADD(range_node->intent_lock,faa_num);
            // printf("[row_opt_no_wait3.cpp:115]x content abort\n");
            return Abort;
        }
        
        // if(row->_tid_word != 0)
        // printf("[row_opt_no_wait3.cpp:119]lock = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",row->_tid_word,txn->decode_is_lock(row->_tid_word),txn->decode_ix_lock(row->_tid_word),txn->decode_s_lock(row->_tid_word),txn->decode_x_lock(row->_tid_word));

        faa_num = 1;
        faa_result = ATOM_FETCH_ADD(row->_tid_word,faa_num);
        if(txn->x_lock_content(faa_result)){
            faa_num = -1;
            faa_num = faa_num<<32;
            faa_result = 0;
            // ATOM_FETCH_ADD(range_node->intent_lock,faa_num);
            faa_num = -1;
            faa_result = 0;
            // ATOM_FETCH_ADD(row->_tid_word,faa_num);
            // printf("[row_opt_no_wait3.cpp:127]x content abort\n");
            return Abort;
        }

        faa_result = row->_tid_word;
        // if(faa_result != 0)printf("[row_opt_no_wait3.cpp:123]lock = %ld,key = %ld,is = %ld,ix = %ld,s = %ld,x = %ld\n",faa_result,row->get_primary_key(),txn->decode_is_lock(faa_result),txn->decode_ix_lock(faa_result),txn->decode_s_lock(faa_result),txn->decode_x_lock(faa_result));

        // record_intent_lock *record = (record_intent_lock*)mem_allocator.alloc(sizeof(record_intent_lock));
        // record->bt_node_location = range_node;
        // record->server_id = g_node_id;
        // txn->txn->locked_node.add(record);
        
        return RCOK;
    }
}



RC Row_opt_no_wait::lock_release(TxnManager * txn,uint64_t rid) {
    bt_node * leaf_node = (bt_node *)(txn->txn->locked_node[rid]->bt_node_location);
    Access * access = txn->txn->accesses[rid];
	row_t * orig_r = txn->txn->accesses[rid]->orig_row;

    if(access->type == WR){
        //release data lock(X)
        uint64_t faa_num = -1;
        uint64_t faa_result = ATOM_FETCH_ADD(orig_r->_tid_word,faa_num);
        // printf("[row_opt_no_wait3.cpp:162]faa_result = %ld,intent_lock = %ld,old x = %ld,new x = %ld\n",faa_result,orig_r->_tid_word,txn->decode_ix_lock(faa_result),txn->decode_ix_lock(orig_r->_tid_word));


        //release range lock(IX)
        faa_num = -1;
        faa_num = faa_num<<32;
        faa_result =0;
        //  ATOM_FETCH_ADD(leaf_node->intent_lock,faa_num);
        // printf("[row_opt_no_wait3.cpp:169]faa_result = %ld,intent_lock = %ld,old ix = %ld,new ix = %ld\n",faa_result,leaf_node->intent_lock,txn->decode_ix_lock(faa_result),txn->decode_ix_lock(leaf_node->intent_lock));
    }else if(access->type == RD){
        // unlock data lock(S)
        uint64_t faa_num = -1;
        faa_num = faa_num<<16;
        uint64_t faa_result = ATOM_FETCH_ADD(orig_r->_tid_word,faa_num);

        // unlock range lock(IS)
        faa_num = -1;
        faa_num = faa_num<<48;
        faa_result = 0;
        // ATOM_FETCH_ADD(leaf_node->intent_lock,faa_num);
    }
    return RCOK;
}



