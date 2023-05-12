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

#ifndef _RDMA_BTREE_H_
#define _RDMA_BTREE_H_

#include "global.h"
#include "helper.h"
#include "index_base.h"


typedef struct root_offset_struct{
    uint64_t root_offset; 
}root_offset_struct;


class IndexRdmaBtree : public index_base {
public:
	RC			init(uint64_t part_cnt);
	RC			init(uint64_t part_cnt, table_t * table);
	bool 		index_exist(idx_key_t key); // check if the key exist.
	RC 			index_insert(idx_key_t key, itemid_t * item, int part_id = -1);
	RC 			index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id = -1) { return RCOK;}
	RC 			get_index_by_id(uint64_t index, itemid_t * &item, int part_id=-1) {return RCOK;};
    // RC          index_read(idx_key_t key, itemid_t *&item, uint64_t thd_id, int64_t part_id = -1);
    RC          index_read(idx_key_t key, itemid_t *&item, int part_id = -1, int thd_id = 0);
    RC          index_node_read(idx_key_t key, rdma_bt_node *&leaf_node, int part_id = -1, int thd_id = 0);
	RC	 		index_read(idx_key_t key, itemid_t * &item, int part_id = -1);
	RC	 		index_read(idx_key_t key, itemid_t * &item);
    RC          index_read(idx_key_t key, uint64_t count, itemid_t *&item, int64_t part_id);
	RC 			index_next(uint64_t thd_id, itemid_t * &item, bool samekey = false);
	uint64_t 	get_count() {return 0;}
    RC          index_remove(idx_key_t key) {return RCOK;};
    RC          get_btree_layer();
    RC learn_index_node_read(idx_key_t key, LeafIndexInfo *&leaf_node, int part_id = -1, int thd_id = 0){}
     bool 		index_exist(rdma_idx_key_t key); // check if the key exist.
	RC 			index_insert(rdma_idx_key_t key, itemid_t * item, int part_id = -1);
    RC          index_read(rdma_idx_key_t key, itemid_t *&item, int part_id = -1, int thd_id = 0);
    RC          index_node_read(rdma_idx_key_t key, rdma_bt_node *&leaf_node, int part_id = -1, int thd_id = 0);
    RC          find_index_node_to_insert(rdma_idx_key_t key, rdma_bt_node *&leaf_node, int part_id, int thd_id);
	RC	 		index_read(rdma_idx_key_t key, itemid_t * &item);

private:
	// index structures may have part_cnt = 1 or PART_CNT.
	uint64_t part_cnt;
	RC			make_lf(uint64_t part_id, rdma_bt_node *& node);
	RC			make_nl(uint64_t part_id, rdma_bt_node *& node);
	RC		 	make_node(uint64_t part_id, rdma_bt_node *& node);

	RC 			start_new_tree(glob_param params, idx_key_t key, itemid_t * item);
 	RC find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, rdma_bt_node *&leaf,
			   rdma_bt_node *&last_ex);
	RC 			find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, rdma_bt_node *& leaf);
	RC			insert_into_leaf(glob_param params, rdma_bt_node * leaf, idx_key_t key, itemid_t * item);
	// handle split
	RC 			split_lf_insert(glob_param params, rdma_bt_node * leaf, idx_key_t key, itemid_t * item);
  	RC split_nl_insert(glob_param params, rdma_bt_node *node, UInt32 left_index, idx_key_t key,
					 rdma_bt_node *right);
	RC 			insert_into_parent(glob_param params, rdma_bt_node * left, idx_key_t key, rdma_bt_node * right);
	RC 			insert_into_new_root(glob_param params, rdma_bt_node * left, idx_key_t key, rdma_bt_node * right);

 	RC find_leaf(glob_param params, rdma_idx_key_t key, idx_acc_t access_type, rdma_bt_node *&leaf,rdma_bt_node *&last_ex);
    RC 			find_leaf(glob_param params, rdma_idx_key_t key, idx_acc_t access_type, rdma_bt_node *& leaf);
	RC			insert_into_leaf(glob_param params, rdma_bt_node * leaf, rdma_idx_key_t key, itemid_t * item);
	// handle split
	RC 			split_lf_insert(glob_param params, rdma_bt_node * leaf, rdma_idx_key_t key, itemid_t * item);
  	RC split_nl_insert(glob_param params, rdma_bt_node *node, UInt32 left_index, rdma_idx_key_t key,
					 rdma_bt_node *right);
	RC 			insert_into_parent(glob_param params, rdma_bt_node * left, rdma_idx_key_t key, rdma_bt_node * right);
	RC 			insert_into_new_root(glob_param params, rdma_bt_node * left, rdma_idx_key_t key, rdma_bt_node * right);
    // int			leaf_has_key(rdma_bt_node * leaf, rdma_idx_key_t key);
	int			leaf_has_key(rdma_bt_node * leaf, rdma_idx_key_t key);

	int			leaf_has_key(rdma_bt_node * leaf, idx_key_t key);

	UInt32 		cut(UInt32 length);
	UInt32	 	order; // # of keys in a node(for both leaf and non-leaf)
	rdma_bt_node ** 	roots; // each partition has a different root
	rdma_bt_node *   find_root(uint64_t part_id);

	bool 		latch_node(rdma_bt_node * node, latch_t latch_type);
	latch_t		release_latch(rdma_bt_node * node);
	RC		 	upgrade_latch(rdma_bt_node * node);
	// clean up all the LATCH_EX up tp last_ex
	RC 			cleanup(rdma_bt_node * node, rdma_bt_node * last_ex);

	// the leaf and the idx within the leaf that the thread last accessed.
	rdma_bt_node *** cur_leaf_per_thd;
	UInt32 ** 		cur_idx_per_thd;
    bool in_init;
    root_offset_struct *root_offset;
	rdma_bt_node *rdma_btree_node;    
};

#endif
