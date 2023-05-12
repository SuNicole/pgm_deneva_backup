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

#ifndef _INDEX_BASE_H_
#define _INDEX_BASE_H_

#include "global.h"

class table_t;


class LeafIndexInfo {
public:
	void init(){
        for(int i = 0;i < RANGE_SIZE;i++){
            keys[i] = 0;
            offsets[i] = 0;
            intent_lock = 0;
        }
		key_cnt = 0;
		// type = DT_row;
	};
    uint64_t intent_lock;//IS|IX|S|X
	uint64_t keys[RANGE_SIZE];
	uint64_t offsets[RANGE_SIZE];
    uint64_t key_cnt;
	// uint64_t length;
	// Data_type type;
};

class rdma_bt_node {
public:
  volatile uint64_t intent_lock; //IS|IX|S|X
  // uint64_t 
	bool is_leaf;
	bool latch;
	pthread_mutex_t locked;
	latch_t latch_type;
	UInt32 share_cnt;
  uint64_t child_offsets[BTREE_ORDER + 1];
  uint64_t parent_offset;
  uint64_t next_node_offset;
  uint64_t prev_node_offset;
	// idx_key_t keys[BTREE_ORDER];
#if !DYNAMIC_WORKLOAD
	idx_key_t keys[BTREE_ORDER];
#else
    double keys[BTREE_ORDER + 1];
#endif
	UInt32 num_keys;
    void * pointers[BTREE_ORDER + 1];
    void init(){
        intent_lock = 0;
        num_keys = 0;
    }
    void init_new_leaf(){
       intent_lock = 0; 
	   is_leaf = true;
       parent_offset = 0;
       next_node_offset = 0;
       prev_node_offset = 0;
       num_keys = 0;
       for(int i = 0;i < BTREE_ORDER;i++){
            child_offsets[i] = UINT64_MAX;
            keys[i] = 0;
            pointers[i] = NULL;
       }
    }
};

class index_base {
public:
  virtual RC init() {
    return RCOK;
  };

  virtual RC init(uint64_t size) {
    return RCOK;
  };

	virtual bool 		index_exist(idx_key_t key)=0; // check if the key exist.

  virtual RC index_insert(idx_key_t key, itemid_t *item, int part_id = -1) = 0;

  virtual RC index_insert_nonunique(idx_key_t key, itemid_t *item, int part_id = -1) = 0;

  virtual RC get_index_by_id(uint64_t index, itemid_t * &item, int part_id=-1) = 0;
  
  virtual RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1) = 0;

  virtual RC index_node_read(idx_key_t key, rdma_bt_node *&leaf_node, int part_id = -1, int thd_id = 0) = 0;
  virtual RC learn_index_node_read(idx_key_t key, LeafIndexInfo *&leaf_node, int part_id = -1, int thd_id = 0) = 0;
//! !!!!!!
  virtual RC index_read(idx_key_t key, itemid_t *&item, int part_id = -1, int thd_id = 0) = 0;

  virtual uint64_t get_count() = 0;
	// TODO implement index_remove
  virtual RC index_remove(idx_key_t key) {
    return RCOK;
  };

	// the index in on "table". The key is the merged key of "fields"
	table_t * 			table;
};

#endif
