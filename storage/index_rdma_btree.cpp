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
#include "global.h"
#include "mem_alloc.h"
#include "index_rdma_btree.h"
#include "row.h"
#include "src/allocator_master.hh"

#if !DYNAMIC_WORKLOAD
RC IndexRdmaBtree::init(uint64_t part_cnt) {
    this->in_init = false;
    root_offset = (root_offset_struct*)rdma_global_buffer;
    r2::AllocatorMaster<>::init((char *)root_offset,sizeof(root_offset_struct));
    root_offset->root_offset = UINT64_MAX;

	this->part_cnt = part_cnt;
	order = BTREE_ORDER;

	//! check
	// "cur_xxx_per_thd" is only for SCAN queries.
	ARR_PTR(rdma_bt_node *, cur_leaf_per_thd, g_thread_cnt);
	ARR_PTR(UInt32, cur_idx_per_thd, g_thread_cnt);
    roots = (rdma_bt_node **) malloc(part_cnt * sizeof(rdma_bt_node *));
	// the index tree of each partition musted be mapped to corresponding l2 slices
	for (UInt32 part_id = 0; part_id < part_cnt; part_id ++) {
		RC rc;
		rc = make_lf(part_id, roots[part_id]);
		assert (rc == RCOK);
	}
    root_offset->root_offset = (char*)roots[g_node_id] - rdma_global_buffer;
    my_root_offset = root_offset->root_offset;
	return RCOK;
}
#else
RC IndexRdmaBtree::init(uint64_t part_cnt) {
    this->in_init = false;
    root_offset = (root_offset_struct*)rdma_global_buffer;
    r2::AllocatorMaster<>::init((char *)root_offset,sizeof(root_offset_struct));
    root_offset->root_offset = UINT64_MAX;

	this->part_cnt = part_cnt;
	order = BTREE_ORDER;

    uint64_t slot_num = (rdma_index_size - sizeof(root_offset_struct) - sizeof(uint64_t))/sizeof(rdma_bt_node);
    rdma_btree_node = (rdma_bt_node*)(rdma_global_buffer + sizeof(root_offset_struct)) ;
    for (int i = 0; i < slot_num; i ++) {
		rdma_btree_node[i].init();
	}

	ARR_PTR(rdma_bt_node *, cur_leaf_per_thd, g_thread_cnt);
	ARR_PTR(UInt32, cur_idx_per_thd, g_thread_cnt);
    roots = (rdma_bt_node **) malloc(part_cnt * sizeof(rdma_bt_node *));
	// the index tree of each partition musted be mapped to corresponding l2 slices
	for (UInt32 part_id = 0; part_id < part_cnt; part_id ++) {
		RC rc;
		rc = make_lf(part_id, roots[part_id]);
		assert (rc == RCOK);
	}
    root_offset->root_offset = (char*)roots[g_node_id] - rdma_global_buffer;
    my_root_offset = root_offset->root_offset;
	return RCOK;
}
#endif
RC IndexRdmaBtree::init(uint64_t part_cnt, table_t * table) {
	this->table = table;
	init(part_cnt);
	return RCOK;
}

rdma_bt_node * IndexRdmaBtree::find_root(uint64_t part_id) {
	assert (part_id < part_cnt);
	return roots[part_id];
}

bool IndexRdmaBtree::index_exist(idx_key_t key) {
	assert(false); // part_id is not correct now.
	glob_param params;
	params.part_id = key_to_part(key) % part_cnt;
	rdma_bt_node * leaf;
	// does not matter which thread check existence
	find_leaf(params, key, INDEX_NONE, leaf);
	if (leaf == NULL) return false;
	for (UInt32 i = 0; i < leaf->num_keys; i++)
    	if (leaf->keys[i] == key) {
            // the record is found!
			return true;
        }
	return false;
}

//never used function
RC IndexRdmaBtree::index_next(uint64_t thd_id, itemid_t * &item, bool samekey) {
	int idx = *cur_idx_per_thd[thd_id];
	rdma_bt_node * leaf = *cur_leaf_per_thd[thd_id];
#if !DYNAMIC_WORKLOAD
	idx_key_t cur_key = leaf->keys[idx] ;
#else
    rdma_idx_key_t cur_key = leaf->keys[idx] ;
#endif
	*cur_idx_per_thd[thd_id] += 1;
	if (*cur_idx_per_thd[thd_id] >= leaf->num_keys) {
		leaf = (rdma_bt_node*)(rdma_global_buffer + leaf->next_node_offset);
		*cur_leaf_per_thd[thd_id] = leaf;
		*cur_idx_per_thd[thd_id] = 0;
	}
	if (leaf == NULL)
		item = NULL;
	else {
		assert( leaf->is_leaf );
		if ( samekey && leaf->keys[ *cur_idx_per_thd[thd_id] ] != cur_key)
			item = NULL;
		else
			item = (itemid_t *) leaf->pointers[ *cur_idx_per_thd[thd_id] ];
	}
	return RCOK;
}

RC IndexRdmaBtree::index_read(idx_key_t key, itemid_t *& item) {
	assert(false);
	return RCOK;
}

RC IndexRdmaBtree::index_read(idx_key_t key, itemid_t *&item, int part_id) {

	return index_read(key, item, part_id,0);
}

// RC IndexRdmaBtree::index_read(idx_key_t key, itemid_t *&item, uint64_t thd_id, int64_t part_id) {
// 	RC rc = Abort;
// 	glob_param params;
// 	assert(part_id != -1);
// 	params.part_id = part_id;
// 	rdma_bt_node * leaf;
// 	find_leaf(params, key, INDEX_READ, leaf);
//     if (leaf == NULL) M_ASSERT(false, "the leaf does not exist!");
// 	for (UInt32 i = 0; i < leaf->num_keys; i++)
// 		if (leaf->keys[i] == key) {
// 			// item = (itemid_t *)leaf->pointers[i];

//             item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
//             item->offset = leaf->child_offsets[i];
// 	        item->location = (rdma_bt_node *)rdma_global_buffer + leaf->child_offsets[i];
//             //TODO usage of type and valid
// 	        // item->type = index_info[index_key].type;
// 	        // item->valid = index_info[index_key].valid;

// 			release_latch(leaf);
// 			(*cur_leaf_per_thd[thd_id]) = leaf;
// 			*cur_idx_per_thd[thd_id] = i;
// 			return RCOK;
// 		}
// 	// release the latch after reading the node

// 	printf("key = %ld\n", key);
// 	M_ASSERT(false, "the key does not exist!");
// 	return rc;
// }

RC IndexRdmaBtree::index_node_read(idx_key_t key, rdma_bt_node *&leaf_node, int part_id, int thd_id){
    RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	rdma_bt_node * leaf;
	find_leaf(params, key, INDEX_READ, leaf);
    if (leaf == NULL) M_ASSERT(false, "the leaf does not exist!");
    if(leaf->keys[leaf->num_keys - 1] < key){
        printf("[index_rdma_btree.cpp:181]key=%ld,lastKey=%lf\n",key,leaf->keys[leaf->num_keys - 1]);
        // while(leaf->next_node_offset != UINT64_MAX){
        //     leaf = (rdma_bt_node*)(rdma_global_buffer + leaf->next_node_offset);
        //     if(leaf->keys[leaf->num_keys - 1] >= key)break;
        // }
    }
	for (UInt32 i = 0; i < leaf->num_keys; i++){
		if (leaf->keys[i] == key) {
            leaf_node = leaf;
			release_latch(leaf);
			(*cur_leaf_per_thd[thd_id]) = leaf;
			*cur_idx_per_thd[thd_id] = i;
			return RCOK;
		}
    }
    printf("[index_rdma_btree.cpp:195]fail to find key = %lf\n",key);
    for(int i=0;i<leaf->num_keys;i++){
        printf("[index_rdma_btree.cpp:198]leaf->keys[%d]=%lf\n",i,leaf->keys[i]);
    }
	M_ASSERT(false, "the key does not exist!");
	return rc;
}

RC IndexRdmaBtree::index_read(idx_key_t target_key, itemid_t *&item, int part_id, int thd_id) {
#if DYNAMIC_WORKLOAD
    rdma_idx_key_t key = (rdma_idx_key_t)target_key;
#else
    idx_key_t key = target_key;
#endif
	RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	rdma_bt_node * leaf = NULL;
	rc = find_leaf(params, key, INDEX_READ, leaf);
    if (leaf == NULL) {
        // M_ASSERT(false, "the leaf does not exist!");
        return Abort;
    }
	for (UInt32 i = 0; i < leaf->num_keys; i++){
		if (leaf->keys[i] == key) {
			// item = (itemid_t *)leaf->pointers[i];
            // printf("[index_rdma_btree.cpp:144]get right node\n");
            
            item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
            item->offset = leaf->child_offsets[i];
	        item->location = (row_t *)(rdma_global_buffer + leaf->child_offsets[i]);
            item->leaf_node_offset = (char*)leaf - rdma_global_buffer;
            item->parent = leaf;
            if(((row_t*)(item->location))->get_primary_key() == key){
                // printf("[index_rdma_btree.cpp:150]get right index\n");
            }
            //TODO usage of type and valid
	        // item->type = index_info[index_key].type;
	        // item->valid = index_info[index_key].valid;

			release_latch(leaf);
			(*cur_leaf_per_thd[thd_id]) = leaf;
			*cur_idx_per_thd[thd_id] = i;
            // if(key==15){printf("[index_rdma_btree.cpp:226]*****get 15*****\n");}
			return RCOK;
		}
	// release the latch after reading the node
    }
#if DYNAMIC_WORKLOAD
	printf("key = %lf\n", key);
#else
	printf("key = %ld\n", key);
#endif
    assert(false);
	// M_ASSERT(false, "the key does not exist!");
	return rc;
}

RC IndexRdmaBtree::index_read(idx_key_t key, uint64_t count, itemid_t *&item, int64_t part_id) {
	RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	rdma_bt_node * leaf = NULL;
	find_leaf(params, key, INDEX_READ, leaf);
    if (leaf == NULL) {
        // M_ASSERT(false, "the leaf does not exist!");
        return Abort;
    }
	for (UInt32 i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] == key) {
			// item = (itemid_t *)leaf->pointers[i];

            item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
            item->offset = leaf->child_offsets[i];
	        item->location = (rdma_bt_node *)(rdma_global_buffer + leaf->child_offsets[i]);
            //TODO usage of type and valid
	        // item->type = index_info[index_key].type;
	        // item->valid = index_info[index_key].valid;

			release_latch(leaf);
			(*cur_leaf_per_thd[0]) = leaf;
			*cur_idx_per_thd[0] = i;
			return RCOK;
		}
	// release the latch after reading the node

	printf("key = %ld\n", key);
	M_ASSERT(false, "the key does not exist!");
	return rc;
}

RC IndexRdmaBtree::get_btree_layer(){
	glob_param params;
	params.part_id = g_node_id;
    rdma_bt_node * c = find_root(params.part_id);
    uint64_t layer = 0;
    while (!c->is_leaf) {
        layer ++;
        uint64_t child_offset = c->child_offsets[0];
        rdma_bt_node *child = (rdma_bt_node *)(rdma_global_buffer + child_offset);
		c = child;
	}
    btree_layer = layer;
    printf("[index_rdma_btree.cpp:239]btree_layer = %ld\n",btree_layer);
    return RCOK;
}

RC IndexRdmaBtree::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	glob_param params;
	if (WORKLOAD == TPCC) assert(part_id != -1);
	assert(part_id != -1);
    // printf("[index_rdma_btree.cpp:230]insert index to %d\n",part_id);
	params.part_id = part_id;
	// create a tree if there does not exist one already
	RC rc = RCOK;
	rdma_bt_node * root = find_root(params.part_id);
	assert(root != NULL);
	int depth = 0;
	// TODO tree depth < 100
	rdma_bt_node * ex_list[100];
	rdma_bt_node * leaf = NULL;
	rdma_bt_node * last_ex = NULL;
    //find position of left node to insert 
#if !DYNAMIC_WORKLOAD
    uint64_t tmp_key = key;
#else
    double tmp_key = (double)key;
#endif
	rc = find_leaf(params, tmp_key, INDEX_INSERT, leaf, last_ex);
	assert(rc == RCOK);

	rdma_bt_node * tmp_node = leaf;
	if (last_ex != NULL) {
		while (tmp_node != last_ex) {
			ex_list[depth++] = tmp_node;
            tmp_node = (rdma_bt_node *)(rdma_global_buffer + tmp_node->parent_offset);
			assert (depth < 100);
		}
		ex_list[depth ++] = last_ex;
	} else
		ex_list[depth++] = leaf;

	// insert into btree if the leaf is not full
	if (leaf->num_keys < order - 1 || leaf_has_key(leaf, tmp_key) >= 0) {
        // printf("[index_rdma_btree.cpp:226]insert key = %ld leaf->keys[0] = %ld\n",key,leaf->keys[0]);
		rc = insert_into_leaf(params, leaf, tmp_key, item);
		// only the leaf should be ex latched.
        for (int i = 0; i < depth; i++) release_latch(ex_list[i]);
    } else {  // split the nodes when necessary
		rc = split_lf_insert(params, leaf, tmp_key, item);
        for (int i = 0; i < depth; i++) release_latch(ex_list[i]);
	}
	return rc;
}

RC IndexRdmaBtree::make_lf(uint64_t part_id, rdma_bt_node *& node) {
	RC rc = make_node(part_id, node);
	if (rc != RCOK) return rc;
	node->is_leaf = true;
	return RCOK;
}

RC IndexRdmaBtree::make_nl(uint64_t part_id, rdma_bt_node *& node) {
	RC rc = make_node(part_id, node);
	if (rc != RCOK) return rc;
	node->is_leaf = false;
	return RCOK;
}

#if !DYNAMIC_WORKLOAD
RC IndexRdmaBtree::make_node(uint64_t part_id, rdma_bt_node *& node) {
    uint64_t size = sizeof(rdma_bt_node);
    rdma_bt_node * new_node;
    // if(this->in_init == true){
    //     new_node = (rdma_bt_node*)rdma_global_buffer;
	//     r2::AllocatorMaster<>::init((char *)new_node,size);
    // }else{
        //random locate
        new_node = (rdma_bt_node*)r2::AllocatorMaster<>::get_thread_allocator()->alloc(size);
    // }
    assert (new_node != NULL);
    // while((char *)new_node + size > rdma_global_buffer + rdma_index_size){
    //     //! check usage of pointer
    //     r2::AllocatorMaster<>::get_thread_allocator()->dealloc(new_node);
    //     rdma_bt_node * new_node = (rdma_bt_node*)r2::AllocatorMaster<>::get_thread_allocator()->alloc(size);
    // }

    //TODO - pointer
	for(int i = 0;i < BTREE_ORDER;i++){
        // new_node->keys[i] = UINT64_MAX;
        new_node->keys[i] = 0;
        new_node->pointers[i] = NULL;
        new_node->child_offsets[i] = UINT64_MAX;
    }
	// new_node->keys = (idx_key_t *) mem_allocator.alloc((order - 1) * sizeof(idx_key_t));
	// new_node->pointers = (void **) mem_allocator.alloc(order * sizeof(void *));
	// assert (new_node->keys != NULL && new_node->pointers != NULL);
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent_offset = UINT64_MAX;
	new_node->next_node_offset = UINT64_MAX;
    new_node->intent_lock = 0;
	new_node->latch = false;
	new_node->latch_type = LATCH_NONE;

	node = new_node;
	return RCOK;
}
#else
RC IndexRdmaBtree::make_node(uint64_t part_id, rdma_bt_node *& node) {
    uint64_t slot_id = ATOM_FETCH_ADD(*last_index_node_order,1);
    // printf("[index_rdma_btree.cpp:402]last_index_node_order=%ld\n",slot_id);
    // printf("[rdma.cpp:390]last_index_node_order = %ld\n",last_index_node_order);
    rdma_bt_node *new_node = rdma_btree_node + slot_id; 
    assert (new_node != NULL);

    //TODO - pointer
	for(int i = 0;i < BTREE_ORDER;i++){
        new_node->keys[i] = 0;
        new_node->pointers[i] = NULL;
        new_node->child_offsets[i] = UINT64_MAX;
    }
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent_offset = UINT64_MAX;
	new_node->next_node_offset = UINT64_MAX;
    new_node->prev_node_offset = UINT64_MAX;
    new_node->intent_lock = 0;
	new_node->latch = false;
	new_node->latch_type = LATCH_NONE;

	node = new_node;
	return RCOK;
}
#endif
RC IndexRdmaBtree::start_new_tree(glob_param params, idx_key_t key, itemid_t * item) {
	RC rc;
	uint64_t part_id = params.part_id;
	rc = make_lf(part_id, roots[part_id % part_cnt]);
	if (rc != RCOK) return rc;
	rdma_bt_node * root = roots[part_id % part_cnt];
	assert(root != NULL);
	root->keys[0] = key;
	root->pointers[0] = (void *)item;
    root->child_offsets[0] = -1;
	root->parent_offset = -1;
	root->num_keys++;
	return RCOK;
}

bool IndexRdmaBtree::latch_node(rdma_bt_node * node, latch_t latch_type) {
	// TODO latch is disabled
  if (!ENABLE_LATCH) return true;
	bool success = false;
//		printf("%s : %d\n", __FILE__, __LINE__);
//	if ( g_cc_alg != HSTORE )
  while (!ATOM_CAS(node->latch, false, true)) {
  }
//		pthread_mutex_lock(&node->locked);
//		printf("%s : %d\n", __FILE__, __LINE__);

	latch_t node_latch = node->latch_type;
  if (node_latch == LATCH_NONE || (node_latch == LATCH_SH && latch_type == LATCH_SH)) {
		node->latch_type = latch_type;
    if (node_latch == LATCH_NONE) M_ASSERT((node->share_cnt == 0), "share cnt none 0!");
    if (node->latch_type == LATCH_SH) node->share_cnt++;
		success = true;
  } else  // latch_type incompatible
		success = false;
//	if ( g_cc_alg != HSTORE )
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert(ATOM_CAS(node->locked, true, false));
	return success;
}

latch_t IndexRdmaBtree::release_latch(rdma_bt_node * node) {
  if (!ENABLE_LATCH) return LATCH_SH;
	latch_t type = node->latch_type;
//	if ( g_cc_alg != HSTORE )
  while (!ATOM_CAS(node->latch, false, true)) {
  }
//		pthread_mutex_lock(&node->locked);
//		while (!ATOM_CAS(node->locked, false, true)) {}
	M_ASSERT((node->latch_type != LATCH_NONE), "release latch fault");
	if (node->latch_type == LATCH_EX)
		node->latch_type = LATCH_NONE;
	else if (node->latch_type == LATCH_SH) {
		node->share_cnt --;
    if (node->share_cnt == 0) node->latch_type = LATCH_NONE;
	}
//	if ( g_cc_alg != HSTORE )
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert(ATOM_CAS(node->locked, true, false));
	return type;
}

RC IndexRdmaBtree::upgrade_latch(rdma_bt_node * node) {
  if (!ENABLE_LATCH) return RCOK;
	bool success = false;
//	if ( g_cc_alg != HSTORE )
  while (!ATOM_CAS(node->latch, false, true)) {
  }
//		pthread_mutex_lock(&node->locked);
//		while (!ATOM_CAS(node->locked, false, true)) {}
	M_ASSERT( (node->latch_type == LATCH_SH), "" );
	if (node->share_cnt > 1)
		success = false;
	else { // share_cnt == 1
		success = true;
		node->latch_type = LATCH_EX;
		node->share_cnt = 0;
	}

//	if ( g_cc_alg != HSTORE )
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert( ATOM_CAS(node->locked, true, false) );
  if (success)
    return RCOK;
  else
    return Abort;
}

RC IndexRdmaBtree::cleanup(rdma_bt_node * node, rdma_bt_node * last_ex) {
	if (last_ex != NULL) {
		do {
			// node = node->parent;
            node = (rdma_bt_node * )(rdma_global_buffer + node->parent_offset);
			release_latch(node);
    } while (node != last_ex);
	}
	return RCOK;
}

RC IndexRdmaBtree::find_leaf(glob_param params, rdma_idx_key_t key, idx_acc_t access_type, rdma_bt_node *& leaf) {
	rdma_bt_node * last_ex = NULL;
	// assert(access_type != INDEX_INSERT);
	RC rc = find_leaf(params, key, access_type, leaf, last_ex);
	return rc;
}

RC IndexRdmaBtree::find_leaf(glob_param params, rdma_idx_key_t key, idx_acc_t access_type, rdma_bt_node *&leaf, rdma_bt_node *&last_ex) {
    UInt32 i;
	rdma_bt_node * c = find_root(params.part_id);
    rdma_bt_node * parent;
    if (access_type == INDEX_READ){
        //
    }
	assert(c != NULL);
	rdma_bt_node * child;
    uint64_t child_offset;
	if (access_type == INDEX_NONE) {
		while (!c->is_leaf) {
			for (i = 0; i < c->num_keys; i++) {
                if (key < c->keys[i]) break;
			}
			c = (rdma_bt_node *)(rdma_global_buffer + c->child_offsets[i]);
		}
		leaf = c;
		return RCOK;
	}
    if (!latch_node(c, LATCH_SH)) return Abort;
	while ((!c->is_leaf )) {
        if(c->intent_lock==1) {
            // printf("[index_rdma_btree.cpp:567]search key=%lf,offset=%lu has been locked\n",key,(char*)c-rdma_global_buffer);
            return Abort;
        }
		for (i = 0; i < c->num_keys; i++) {
            if (key < c->keys[i]) break;
		}
        child_offset = c->child_offsets[i];
        assert(child_offset != 0);
        // printf("[index_rdma_btree.cpp:577]search key=%lf,new offset=%lu, key range [%lf,%lf]\n",key,child_offset, i>0?c->keys[i-1]:0,i<c->num_keys? c->keys[i]:0);
        child = (rdma_bt_node *)(rdma_global_buffer + child_offset);
		if (!latch_node(child, LATCH_SH)) {//cannot update
			release_latch(c);
			cleanup(c, last_ex);
			last_ex = NULL;
            // printf("[index_rdma_btree.cpp:583]search key=%lf,offset=%lu has been locked\n",key,child_offset);
			return Abort;
		}
		if (access_type == INDEX_INSERT) {
			if (child->num_keys == order - 1) {
				if (upgrade_latch(c) != RCOK) {
					release_latch(c);
					release_latch(child);
					cleanup(c, last_ex);
					last_ex = NULL;
					return Abort;
				}
                if (last_ex == NULL) last_ex = c;
            } else {
				cleanup(c, last_ex);
				last_ex = NULL;
				release_latch(c);
			}
		} else
			release_latch(c); // release the LATCH_SH on c
        parent = c;
		c = child;
	}
	if (access_type == INDEX_INSERT) {
		if (upgrade_latch(c) != RCOK) {
        	release_latch(c);
            cleanup(c, last_ex);
            return Abort;
        }
	} else {
        // printf("[index_rdma_btree.cpp:603]\n");
        while(c->keys[c->num_keys-1]<key){
            // printf("[index_rdma_btree.cpp:604]search key=%lf,new offset=%lu, key %ld:%ld\n",key,c->next_node_offset,c->keys[c->num_keys-1],key);
            c=(rdma_bt_node*)(rdma_global_buffer+c->next_node_offset);
        }
    }
	leaf = c;
    // printf("[index_rdma_btree.cpp:609]find leaf search key=%lf, offset=%lu\n",key, (char*)leaf-rdma_global_buffer);
	assert (leaf->is_leaf);
	return RCOK;
}

RC IndexRdmaBtree::find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, rdma_bt_node *& leaf) {
	rdma_bt_node * last_ex = NULL;
	assert(access_type != INDEX_INSERT);
	RC rc = find_leaf(params, key, access_type, leaf, last_ex);
	return rc;
}

RC IndexRdmaBtree::find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, rdma_bt_node *&leaf,
                          rdma_bt_node *&last_ex) {
//	RC rc;
	UInt32 i;
	rdma_bt_node * c = find_root(params.part_id);
    if (access_type == INDEX_READ){
            // printf("[index_rdma_btree.cpp:410]part_id = %d c->keys = ",params.part_id);
            // for(int j = 0;j < c->num_keys;j++)printf("%d ",c->keys[j]);
            // printf("\n");
    }
	assert(c != NULL);
	rdma_bt_node * child;
    uint64_t child_offset;
	if (access_type == INDEX_NONE) {
		while (!c->is_leaf) {
			for (i = 0; i < c->num_keys; i++) {
                if (key < c->keys[i]) break;
			}
			c = (rdma_bt_node *)(rdma_global_buffer + c->child_offsets[i]);
		}
		leaf = c;
		return RCOK;
	}
	// key should be inserted into the right side of i
  if (!latch_node(c, LATCH_SH)) return Abort;
	while (!c->is_leaf) {
        // if (access_type == INDEX_READ){
        //     printf("[index_rdma_btree.cpp:426]c->keys = ");
        //     for(int j = 0;j < c->num_keys;j++)printf("%d ",c->keys[j]);
        //     printf("\n");
        // }
		// assert(get_part_id(c) == params.part_id);
		// assert(get_part_id(c->keys) == params.part_id);
		for (i = 0; i < c->num_keys; i++) {
            if(key == 0){
                // printf("[index_rdma_btree.cpp:639]c->key[%d]=%lf,key=%ld\n",i,c->keys[i],key);
            }
            if (key < c->keys[i]) break;
		}
        child_offset = c->child_offsets[i];
        child = (rdma_bt_node *)(rdma_global_buffer + child_offset);
		if (!latch_node(child, LATCH_SH)) {//cannot update
			release_latch(c);
			cleanup(c, last_ex);
			last_ex = NULL;
			return Abort;
		}
		if (access_type == INDEX_INSERT) {
			if (child->num_keys == order - 1) {
				if (upgrade_latch(c) != RCOK) {
					release_latch(c);
					release_latch(child);
					cleanup(c, last_ex);
					last_ex = NULL;
					return Abort;
				}
                if (last_ex == NULL) last_ex = c;
            } else {
				cleanup(c, last_ex);
				last_ex = NULL;
				release_latch(c);
			}
		} else
			release_latch(c); // release the LATCH_SH on c
		c = child;
	}
	// c is leaf
	// at this point, if the access is a read, then only the leaf is latched by LATCH_SH
	// if the access is an insertion, then the leaf is sh latched and related nodes in the tree
	// are ex latched.
	if (access_type == INDEX_INSERT) {
		if (upgrade_latch(c) != RCOK) {
        	release_latch(c);
            cleanup(c, last_ex);
            return Abort;
        }
	}
	leaf = c;
	assert (leaf->is_leaf);
	return RCOK;
}

RC IndexRdmaBtree::insert_into_leaf(glob_param params, rdma_bt_node * leaf, idx_key_t key, itemid_t * item) {
	UInt32 i, insertion_point;
    insertion_point = 0;
	int idx = leaf_has_key(leaf, key);
	if (idx >= 0) {//节点已存在(重复树节点)
        // TODO - special case
		// item->next = (itemid_t *)leaf->pointers[idx];
		// leaf->pointers[idx] = (void *) item;
		// return RCOK;
	}
    while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < (double)key){
        insertion_point++;
    }     
	for (i = leaf->num_keys; i > insertion_point; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->child_offsets[i] = leaf->child_offsets[i - 1];
        leaf->pointers[i] = leaf->pointers[i - 1];
    }
    leaf->keys[insertion_point] = (double)key;
    //offset to rdma_global_buffer of new data(item->location)
    leaf->child_offsets[insertion_point] = (char *)(item->location) - (char *)rdma_global_buffer;
    leaf->pointers[insertion_point] = (void *)(item->location);
    leaf->num_keys++;

    ((row_t *)(item->location))->parent_offset = (char *)leaf - rdma_global_buffer;
    // if(leaf->keys[0] > 16384)printf("[index_rdma_btree.cpp]warnning\n");
	M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    return RCOK;
}
 
RC IndexRdmaBtree::split_lf_insert(glob_param params, rdma_bt_node * leaf, idx_key_t key, itemid_t * item) {
    RC rc;
	UInt32 insertion_index, split, i, j;
	idx_key_t new_key;

	uint64_t part_id = params.part_id;
    rdma_bt_node * new_leaf;

	rc = make_lf(part_id, new_leaf);
	if (rc != RCOK) return rc;

	M_ASSERT(leaf->num_keys == order - 1, "trying to split non-full leaf!");

	idx_key_t temp_keys[BTREE_ORDER];
    uint64_t temp_child_odffsets[BTREE_ORDER];
	void * temp_pointers[BTREE_ORDER];
    insertion_index = 0;
    //locate
    while (insertion_index < order - 1 && leaf->keys[insertion_index] < key) insertion_index++;

    for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->keys[i];
        temp_child_odffsets[j] = leaf->child_offsets[i];
        temp_pointers[j] = (row_t *)leaf->pointers[i];
    }
    temp_keys[insertion_index] = key;
    temp_child_odffsets[insertion_index] = (char *)(item->location) - (char *)rdma_global_buffer;
    temp_pointers[insertion_index] = item->location;

   	// leaf is on the left of new_leaf
    split = cut(order - 1);//half of origin leaf
    if(split < insertion_index){
        ((row_t *)(item->location))->parent_offset = (char *)leaf - rdma_global_buffer;
    }else{
        ((row_t *)(item->location))->parent_offset = (char *)new_leaf - rdma_global_buffer;
    }
    leaf->num_keys = 0;
    for (i = 0; i < split; i++) {
        leaf->pointers[i] = temp_pointers[i];
        leaf->keys[i] = temp_keys[i];
        leaf->child_offsets[i] = temp_child_odffsets[i];
        leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    }
	for (i = split, j = 0; i < order; i++, j++) {
        new_leaf->pointers[j] = temp_pointers[i];
        new_leaf->child_offsets[j] = temp_child_odffsets[i];
        new_leaf->keys[j] = temp_keys[i];
        new_leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    }

	new_leaf->next_node_offset = leaf->next_node_offset;
	leaf->next_node_offset = (char *)new_leaf - (char*)rdma_global_buffer;

    for (i = leaf->num_keys; i < order - 1; i++) {
        // leaf->keys[i] = UINT64_MAX;
        leaf->keys[i] = 0;
        leaf->pointers[i] = NULL;
        leaf->child_offsets[i] = UINT64_MAX;
    }
    for (i = new_leaf->num_keys; i < order - 1; i++) {
        // new_leaf->keys[i] = UINT64_MAX;
        new_leaf->keys[i] = 0;
        new_leaf->child_offsets[i] = UINT64_MAX;
        new_leaf->pointers[i] = NULL;
    }
    // if(new_leaf->keys[0] > 16384 || leaf->keys[0] > 16384)printf("[index_rdma_btree.cpp:569]warnning\n");


    new_leaf->parent_offset = leaf->parent_offset;
    new_key = new_leaf->keys[0];
    if(leaf->keys[0]==8)printf("[index_rdma_btree.cpp:776]parent node insert\n");
    rc = insert_into_parent(params, leaf, new_key, new_leaf);
	return rc;
}

RC IndexRdmaBtree::insert_into_parent(glob_param params, rdma_bt_node *left, idx_key_t key,rdma_bt_node * right) {

    rdma_bt_node * parent = (rdma_bt_node *)(rdma_global_buffer + left->parent_offset);

    /* Case: new root. */
    //   if (parent == NULL) 
    if(left->parent_offset == UINT64_MAX)return insert_into_new_root(params, left, key, right);
    if(parent->keys[0]==8)printf("[index_rdma_btree.cpp:788]parent node insert\n");

	UInt32 insert_idx = 0;
    while (parent->keys[insert_idx] < key && insert_idx < parent->num_keys) insert_idx++;
	// the parent has enough space, just insert into it
    if (parent->num_keys < order - 1) {
		for (UInt32 i = parent->num_keys-1; i >= insert_idx; i--) {
			parent->keys[i + 1] = parent->keys[i];
			parent->pointers[i+2] = parent->pointers[i+1];
            //! need check
            parent->child_offsets[i+2] =  parent->child_offsets[i+1];
		}
		parent->num_keys ++;
		parent->keys[insert_idx] = key;
		parent->pointers[insert_idx + 1] = right;
        parent->child_offsets[insert_idx + 1] = (char*)right - (char*)rdma_global_buffer;

        // if(parent->keys[0] > 16384)printf("[index_rdma_btree.cpp:603]warnning\n");
		return RCOK;
	}

    /* Harder case:  split a node in order
     * to preserve the B+ tree properties.
     */
    if(parent->keys[0]==8)printf("[index_rdma_btree.cpp:812]parent node split\n");
	return split_nl_insert(params, parent, insert_idx, key, right);
//	return RCOK;
}

RC IndexRdmaBtree::insert_into_new_root(glob_param params, rdma_bt_node *left, idx_key_t key,
                                     rdma_bt_node *right) {
	RC rc;
	uint64_t part_id = params.part_id;
	rdma_bt_node * new_root;
//	printf("will make_nl(). part_id=%lld. key=%lld\n", part_id, key);
	rc = make_nl(part_id, new_root);
	if (rc != RCOK) return rc;
    new_root->keys[0] = key;
    new_root->pointers[0] = left;
    new_root->child_offsets[0] = (char *)left - (char*)rdma_global_buffer;
    new_root->pointers[1] = right;
    new_root->child_offsets[1] = (char *)right - (char*)rdma_global_buffer;
    new_root->num_keys++;
	M_ASSERT( (new_root->num_keys < order), "too many keys in leaf" );
    new_root->parent_offset = UINT64_MAX;
    left->parent_offset = (char *)new_root - rdma_global_buffer;
    right->parent_offset = (char *)new_root -rdma_global_buffer;
	left->next_node_offset = (char *)right - rdma_global_buffer;

    if(new_root->keys[0] > 16384)printf("[index_rdma_btree.cpp:633]warnning\n");

    //! check
    //将新的root节点放到共享内存的开始处
	this->roots[part_id] = new_root;
    root_offset->root_offset = (char*)(this->roots[g_node_id]) - rdma_global_buffer;
    my_root_offset = root_offset->root_offset;
    // rdma_bt_node * tmp_root = (rdma_bt_node *)malloc(sizeof(rdma_bt_node));
    // for(int i = 0;i < BTREE_ORDER;i++){
    //     tmp_root->child_offsets[i] =  this->roots[part_id]->child_offsets[i];
    //     tmp_root->pointers[i] =  this->roots[part_id]->pointers[i];
    //     tmp_root->keys[i] =  this->roots[part_id]->keys[i];
    // }
    // tmp_root->is_leaf =  this->roots[part_id]->is_leaf;
    // tmp_root->num_keys =  this->roots[part_id]->num_keys;
    // tmp_root->latch =  this->roots[part_id]->latch;
    // tmp_root->locked =  this->roots[part_id]->locked;
    // tmp_root->latch_type =  this->roots[part_id]->latch_type;
    // tmp_root->share_cnt =  this->roots[part_id]->share_cnt;
    // tmp_root->parent_offset =  this->roots[part_id]->parent_offset;
    // tmp_root->next_node_offset =  this->roots[part_id]->next_node_offset;

    // for(int i = 0;i < BTREE_ORDER;i++){
    //     this->roots[part_id]->child_offsets[i] =  new_root->child_offsets[i];
    //     this->roots[part_id]->pointers[i] =  new_root->pointers[i];
    //     this->roots[part_id]->keys[i] =  new_root->keys[i];
    // }
    // this->roots[part_id]->is_leaf =  new_root->is_leaf;
    // this->roots[part_id]->num_keys =  new_root->num_keys;
    // this->roots[part_id]->latch =  new_root->latch;
    // this->roots[part_id]->locked =  new_root->locked;
    // this->roots[part_id]->latch_type =  new_root->latch_type;
    // this->roots[part_id]->share_cnt = new_root->share_cnt;
    // this->roots[part_id]->parent_offset =  new_root->parent_offset;
    // this->roots[part_id]->next_node_offset =  new_root->next_node_offset;

    // for(int i = 0;i < BTREE_ORDER;i++){
    //     new_root->child_offsets[i] =  tmp_root->child_offsets[i];
    //     new_root->pointers[i] =  tmp_root->pointers[i];
    //     new_root->keys[i] =  tmp_root->keys[i];
    // }
    // new_root->is_leaf =  tmp_root->is_leaf;
    // new_root->num_keys =  tmp_root->num_keys;
    // new_root->latch =  tmp_root->latch;
    // new_root->locked =  tmp_root->locked;
    // new_root->latch_type =  tmp_root->latch_type;
    // new_root->share_cnt = tmp_root->share_cnt;
    // new_root->parent_offset =  tmp_root->parent_offset;
    // new_root->next_node_offset =  tmp_root->next_node_offset;
	// TODO this new root is not latched, at this point, other threads
	// may start to access this new root. Is this ok?
    return RCOK;
}

RC IndexRdmaBtree::split_nl_insert(glob_param params, rdma_bt_node *old_node, UInt32 left_index,
                                idx_key_t key, rdma_bt_node *right) {
	RC rc;
	uint64_t i, j, split, k_prime;
    rdma_bt_node * new_node, * child;
	uint64_t part_id = params.part_id;
    rc = make_node(part_id, new_node);

    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places.
     * Then create a new node and copy half of the
     * keys and pointers to the old node and
     * the other half to the new.
     */

    idx_key_t temp_keys[BTREE_ORDER];
    idx_key_t old_keys[BTREE_ORDER];
    int old_key_nums = old_node->num_keys;
    // for(int i = 0;i < old_key_nums;i++){
    //     old_keys[i] = old_node->keys[i];
    // }
    uint64_t temp_child_offset[BTREE_ORDER];
    rdma_bt_node * temp_pointers[BTREE_ORDER + 1];
    for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = (rdma_bt_node *)old_node->pointers[i];
        //! need check
        temp_child_offset[j] = old_node->child_offsets[i];
    }

    for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->keys[i];
    }

    temp_pointers[left_index + 1] = right;
    temp_child_offset[left_index + 1] = (char *)right - (char*)rdma_global_buffer;
    temp_keys[left_index] = key;

	/* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    split = cut(order);
//	printf("will make_node(). part_id=%lld, key=%lld\n", part_id, key);
	if (rc != RCOK) return rc;

    old_node->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_node->pointers[i] = temp_pointers[i];
        old_node->child_offsets[i] = temp_child_offset[i];
        old_node->keys[i] = temp_keys[i];
        old_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
    }

	new_node->next_node_offset = old_node->next_node_offset;
	old_node->next_node_offset = (char *)new_node - (char*)rdma_global_buffer;

    old_node->pointers[i] = temp_pointers[i];
    old_node->child_offsets[i] = temp_child_offset[i];
    k_prime = temp_keys[split - 1];

    for (++i, j = 0; i < order; i++, j++) {
        new_node->pointers[j] = temp_pointers[i];
        new_node->child_offsets[j] = temp_child_offset[i];
        new_node->keys[j] = temp_keys[i];
        new_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
    }
    new_node->pointers[j] = temp_pointers[i];
    new_node->child_offsets[j] = temp_child_offset[i];

    new_node->parent_offset = old_node->parent_offset;
    for (i = 0; i <= new_node->num_keys; i++) {
        child = (rdma_bt_node *)(rdma_global_buffer + new_node->child_offsets[i]);
        child->parent_offset = (char *)new_node - (char *)rdma_global_buffer;
    }

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    // if(new_node->keys[0] > 16384)printf("[index_rdma_btree.cpp:768]warnning\n");
    // if(old_node->keys[0] > 16384){
    //     printf("[index_rdma_btree.cpp:769]warnning old_keys = ");
    //     for(int i = 0;i < old_key_nums;i++){
    //         printf(" %ld",old_keys[i]);
    //     }
    //     printf("\n");
    // }
    if(old_node->keys[0]==8)printf("[index_rdma_btree.cpp:985]parent node insert\n");
    return insert_into_parent(params, old_node, k_prime, new_node);
}

//locate
int IndexRdmaBtree::leaf_has_key(rdma_bt_node * leaf, idx_key_t key) {
	for (UInt32 i = 0; i < leaf->num_keys; i++)
        if (leaf->keys[i] == key) return i;
	return -1;
}

UInt32 IndexRdmaBtree::cut(UInt32 length) {
	if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

bool IndexRdmaBtree::index_exist(rdma_idx_key_t key) {
	assert(false); 
	glob_param params;
	params.part_id = key_to_part(key) % part_cnt;
	rdma_bt_node * leaf;
	find_leaf(params, key, INDEX_NONE, leaf);
	if (leaf == NULL) return false;
	for (UInt32 i = 0; i < leaf->num_keys; i++)
    	if (leaf->keys[i] == key) {
			return true;
        }
	return false;
}

RC IndexRdmaBtree::index_read(rdma_idx_key_t key, itemid_t *& item) {
	assert(false);
	return RCOK;
}

RC IndexRdmaBtree::index_node_read(rdma_idx_key_t key, rdma_bt_node *&leaf_node, int part_id, int thd_id){

    RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	rdma_bt_node * leaf= NULL;
	rc = find_leaf(params, key, INDEX_READ, leaf);
    if (leaf == NULL) {
        // M_ASSERT(false, "the leaf does not exist!");
        return Abort;
    }
    if(leaf->keys[leaf->num_keys - 1] < key){
        // printf("[index_rdma_btree.cpp:1046]key=%lf,firstkey=%lf,lastKey=%lf\n",key,leaf->keys[0],leaf->keys[leaf->num_keys - 1]);
        // while(leaf->next_node_offset != UINT64_MAX){
        //     leaf = (rdma_bt_node*)(rdma_global_buffer + leaf->next_node_offset);
        //     if(leaf->keys[leaf->num_keys - 1] >= key)break;
        // }
    }
	for (UInt32 i = 0; i < leaf->num_keys; i++){
		if (leaf->keys[i] == key) {
            leaf_node = leaf;
			release_latch(leaf);
			(*cur_leaf_per_thd[thd_id]) = leaf;
			*cur_idx_per_thd[thd_id] = i;
			return RCOK;
		}
    }
    printf("[index_rdma_btree.cpp:1061]fail to find key = %lf\n",key);
    rdma_bt_node *parent=(rdma_bt_node *)(rdma_global_buffer + leaf->parent_offset);
    for(int i=0;i<parent->num_keys;i++){
        printf("[index_rdma_btree.cpp:1066]parent->keys[%d]=%lf\n",i,parent->keys[i]);
    }
    for(int i=0;i<leaf->num_keys;i++){
        printf("[index_rdma_btree.cpp:1070]leaf->keys[%d]=%lf\n",i,leaf->keys[i]);
    }
	M_ASSERT(false, "the key does not exist!");
	return rc;
}

RC IndexRdmaBtree::find_index_node_to_insert(rdma_idx_key_t key, rdma_bt_node *&leaf_node, int part_id, int thd_id){

    RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	rdma_bt_node * leaf= NULL;
	rc = find_leaf(params, key, INDEX_INSERT, leaf);
    if (leaf == NULL) {
        // M_ASSERT(false, "the leaf does not exist!");
        return Abort;
    }
    release_latch(leaf);
    (*cur_leaf_per_thd[thd_id]) = leaf;
    *cur_idx_per_thd[thd_id] = 0;

    if(leaf->keys[leaf->num_keys-1]<key){
        while(leaf->next_node_offset!=UINT64_MAX){
            rdma_bt_node *next_node = (rdma_bt_node*)(rdma_global_buffer + leaf->next_node_offset);
            if(next_node->keys[0]<key)leaf=next_node;
            else break;
        }
    }

    leaf_node = leaf;
	

	return RCOK;
}


RC IndexRdmaBtree::insert_into_leaf(glob_param params, rdma_bt_node * leaf, rdma_idx_key_t key, itemid_t * item) {
	UInt32 i, insertion_point;
    insertion_point = 0;
	int idx = leaf_has_key(leaf, key);
	if (idx >= 0) {
	}
    while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < (double)key){
        insertion_point++;
    }     
	for (i = leaf->num_keys; i > insertion_point; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->child_offsets[i] = leaf->child_offsets[i - 1];
        leaf->pointers[i] = leaf->pointers[i - 1];
    }
    leaf->keys[insertion_point] = (double)key;
    leaf->child_offsets[insertion_point] = (char *)(item->location) - (char *)rdma_global_buffer;
    leaf->pointers[insertion_point] = (void *)(item->location);
    leaf->num_keys++;

    ((row_t *)(item->location))->parent_offset = (char *)leaf - rdma_global_buffer;
	M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    return RCOK;
}

RC IndexRdmaBtree::split_lf_insert(glob_param params, rdma_bt_node * leaf, rdma_idx_key_t key, itemid_t * item) {
    RC rc;
	UInt32 insertion_index, split, i, j;
	rdma_idx_key_t new_key;

	uint64_t part_id = params.part_id;
    rdma_bt_node * new_leaf;

	rc = make_lf(part_id, new_leaf);
	if (rc != RCOK) return rc;

	M_ASSERT(leaf->num_keys == order - 1, "trying to split non-full leaf!");

	rdma_idx_key_t temp_keys[BTREE_ORDER];
    uint64_t temp_child_odffsets[BTREE_ORDER];
	void * temp_pointers[BTREE_ORDER];
    insertion_index = 0;
    //locate
    while (insertion_index < order - 1 && leaf->keys[insertion_index] < key) insertion_index++;

    for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->keys[i];
        temp_child_odffsets[j] = leaf->child_offsets[i];
        temp_pointers[j] = (row_t *)leaf->pointers[i];
    }
    temp_keys[insertion_index] = key;
    temp_child_odffsets[insertion_index] = (char *)(item->location) - (char *)rdma_global_buffer;
    temp_pointers[insertion_index] = item->location;

    split = cut(order - 1);//half of origin leaf
    if(split < insertion_index){
        ((row_t *)(item->location))->parent_offset = (char *)leaf - rdma_global_buffer;
    }else{
        ((row_t *)(item->location))->parent_offset = (char *)new_leaf - rdma_global_buffer;
    }
    leaf->num_keys = 0;
    for (i = 0; i < split; i++) {
        leaf->pointers[i] = temp_pointers[i];
        leaf->keys[i] = temp_keys[i];
        leaf->child_offsets[i] = temp_child_odffsets[i];
        leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    }
	for (i = split, j = 0; i < order; i++, j++) {
        new_leaf->pointers[j] = temp_pointers[i];
        new_leaf->child_offsets[j] = temp_child_odffsets[i];
        new_leaf->keys[j] = temp_keys[i];
        new_leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
    }

	new_leaf->next_node_offset = leaf->next_node_offset;
	leaf->next_node_offset = (char *)new_leaf - (char*)rdma_global_buffer;

    for (i = leaf->num_keys; i < order - 1; i++) {
        leaf->keys[i] = 0;
        leaf->pointers[i] = NULL;
        leaf->child_offsets[i] = UINT64_MAX;
    }
    for (i = new_leaf->num_keys; i < order - 1; i++) {
        new_leaf->keys[i] = 0;
        new_leaf->child_offsets[i] = UINT64_MAX;
        new_leaf->pointers[i] = NULL;
    }

    new_leaf->parent_offset = leaf->parent_offset;
    new_key = new_leaf->keys[0];
    if(leaf->keys[0]==8)printf("[index_rdma_btree.cpp:1135]parent node insert\n");
    rc = insert_into_parent(params, leaf, new_key, new_leaf);
	return rc;
}

RC IndexRdmaBtree::insert_into_parent(glob_param params, rdma_bt_node *left, rdma_idx_key_t key,rdma_bt_node * right) {

    rdma_bt_node * parent = (rdma_bt_node *)(rdma_global_buffer + left->parent_offset);

    if(left->parent_offset == UINT64_MAX)return insert_into_new_root(params, left, key, right);

	UInt32 insert_idx = 0;
    while (parent->keys[insert_idx] < key && insert_idx < parent->num_keys) insert_idx++;
    if (parent->num_keys < order - 1) {
		for (UInt32 i = parent->num_keys-1; i >= insert_idx; i--) {
			parent->keys[i + 1] = parent->keys[i];
			parent->pointers[i+2] = parent->pointers[i+1];
            parent->child_offsets[i+2] =  parent->child_offsets[i+1];
		}
		parent->num_keys ++;
		parent->keys[insert_idx] = key;
		parent->pointers[insert_idx + 1] = right;
        parent->child_offsets[insert_idx + 1] = (char*)right - (char*)rdma_global_buffer;

		return RCOK;
	}

	return split_nl_insert(params, parent, insert_idx, key, right);
}

RC IndexRdmaBtree::insert_into_new_root(glob_param params, rdma_bt_node *left, rdma_idx_key_t key,rdma_bt_node *right) {
	RC rc;
	uint64_t part_id = params.part_id;
	rdma_bt_node * new_root;
	rc = make_nl(part_id, new_root);
	if (rc != RCOK) return rc;
    new_root->keys[0] = key;
    new_root->pointers[0] = left;
    new_root->child_offsets[0] = (char *)left - rdma_global_buffer;
    new_root->pointers[1] = right;
    new_root->child_offsets[1] = (char *)right - rdma_global_buffer;
    new_root->num_keys++;
	M_ASSERT( (new_root->num_keys < order), "too many keys in leaf" );
    new_root->parent_offset = UINT64_MAX;
    left->parent_offset = (char *)new_root - rdma_global_buffer;
    right->parent_offset = (char *)new_root - rdma_global_buffer;
    right->next_node_offset = left->next_node_offset;
	left->next_node_offset = (char *)right - rdma_global_buffer;

    if(new_root->keys[0] > 16384)printf("[index_rdma_btree.cpp:633]warnning\n");

    //! check
    //将新的root节点放到共享内存的开始处
	this->roots[part_id] = new_root;
    root_offset->root_offset = (char*)(this->roots[g_node_id]) - rdma_global_buffer;
    my_root_offset = root_offset->root_offset;
    
    return RCOK;
}

RC IndexRdmaBtree::split_nl_insert(glob_param params, rdma_bt_node *old_node, UInt32 left_index,rdma_idx_key_t key, rdma_bt_node *right) {
	RC rc;
	uint64_t i, j, split, k_prime;
    rdma_bt_node * new_node, * child;
	uint64_t part_id = params.part_id;
    rc = make_node(part_id, new_node);

    rdma_idx_key_t temp_keys[BTREE_ORDER];
    rdma_idx_key_t old_keys[BTREE_ORDER];
    int old_key_nums = old_node->num_keys;
   
    uint64_t temp_child_offset[BTREE_ORDER];
    rdma_bt_node * temp_pointers[BTREE_ORDER + 1];
    for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = (rdma_bt_node *)old_node->pointers[i];
        temp_child_offset[j] = old_node->child_offsets[i];
    }

    for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->keys[i];
    }

    temp_pointers[left_index + 1] = right;
    temp_child_offset[left_index + 1] = (char *)right - (char*)rdma_global_buffer;
    temp_keys[left_index] = key;

    split = cut(order);
	if (rc != RCOK) return rc;

    old_node->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_node->pointers[i] = temp_pointers[i];
        old_node->child_offsets[i] = temp_child_offset[i];
        old_node->keys[i] = temp_keys[i];
        old_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
    }

	new_node->next_node_offset = old_node->next_node_offset;
	old_node->next_node_offset = (char *)new_node - (char*)rdma_global_buffer;

    old_node->pointers[i] = temp_pointers[i];
    old_node->child_offsets[i] = temp_child_offset[i];
    k_prime = temp_keys[split - 1];

    for (++i, j = 0; i < order; i++, j++) {
        new_node->pointers[j] = temp_pointers[i];
        new_node->child_offsets[j] = temp_child_offset[i];
        new_node->keys[j] = temp_keys[i];
        new_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
    }
    new_node->pointers[j] = temp_pointers[i];
    new_node->child_offsets[j] = temp_child_offset[i];

    new_node->parent_offset = old_node->parent_offset;
    for (i = 0; i <= new_node->num_keys; i++) {
        child = (rdma_bt_node *)(rdma_global_buffer + new_node->child_offsets[i]);
        child->parent_offset = (char *)new_node - (char *)rdma_global_buffer;
    }
    if(old_node->keys[0]==8)printf("[index_rdma_btree.cpp:1257]parent node insert\n");
    return insert_into_parent(params, old_node, k_prime, new_node);
}

int IndexRdmaBtree::leaf_has_key(rdma_bt_node * leaf, rdma_idx_key_t key) {
	for (UInt32 i = 0; i < leaf->num_keys; i++)
        if (leaf->keys[i] == key) return i;
	return -1;
}
/*
void index_btree::print_btree(bt_node * start) {
	if (roots == NULL) {
		cout << "NULL" << endl;
		return;
	}
	bt_node * c;
	bt_node * p = start;
	bool last_iter = false;
	do {
		c = p;
		if (!c->is_leaf)
			p = (bt_node *)c->pointers[0];
		else
			last_iter = true;

		while (c != NULL) {
			for (int i = 0; i < c->num_keys; i++) {
				row_t * r = (row_t *)((itemid_t*)c->pointers[i])->location;
				if (c->is_leaf)
					printf("%lld(%lld,%d),",
						c->keys[i],
						r->get_uint_value(0),
						((itemid_t*)c->pointers[i])->valid);
				else
					printf("%lld,", c->keys[i]);
			}
			cout << "|";
			c = c->next;
		}
		cout << endl;
	} while (!last_iter);

}*/
