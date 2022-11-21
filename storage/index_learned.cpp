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
#include "index_learned.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include "src/allocator_master.hh"

#if WORKLOAD == TPCC
RC IndexLearned::init(uint64_t bucket_cnt) {
    uint64_t index_size;
   // if(this == i_item){
    if(bucket_cnt == 0){
        index_info = (IndexInfo*)rdma_global_buffer;
        index_size = item_idx_num/g_node_cnt; 
    }else if (bucket_cnt == 1){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size);
        index_size = wh_idx_num/g_node_cnt;
    }else if (bucket_cnt == 2){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size);
        index_size = dis_idx_num/g_node_cnt;
    }else if (bucket_cnt == 3){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size);
        index_size = cust_idx_num/g_node_cnt;
    }else if (bucket_cnt == 4){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size + cust_index_size);
        index_size = cust_idx_num/g_node_cnt;
    }else if (bucket_cnt == 5){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size + cust_index_size + cl_index_size);
        index_size = stock_idx_num/g_node_cnt;
    }else if (bucket_cnt == 6){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size + cust_index_size + cl_index_size + stock_index_size);
        index_size = order_idx_num/g_node_cnt;
    }else if (bucket_cnt == 7){
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size + cust_index_size + cl_index_size + stock_index_size + order_index_size);
        index_size = ol_idx_num/g_node_cnt;
    }else{
        index_info = (IndexInfo*)(rdma_global_buffer + item_index_size + wh_index_size + dis_index_size + cust_index_size + cl_index_size + stock_index_size + order_index_size + ol_index_size);
        index_size = 1000;
    }
	printf("%d",index_info[0].key);
	_index_size = index_size;
	uint64_t i = 0;
	for (i = 0; i < index_size; i ++) {
		index_info[i].init();
	}

 	printf("init %ld index\n",i);
	return RCOK;
}
#else
RC IndexLearned::init(uint64_t bucket_cnt) {
	leaf_index_info = (LeafIndexInfo*)(rdma_global_buffer + rdma_pgm_index_para_size);

	// printf("%d",index_info[0].key);

	uint64_t i = 0;
	for (i = 0; i < g_synth_table_size/g_node_cnt; i ++) {
		leaf_index_info[i].init();
	}
	_index_size = g_synth_table_size/g_node_cnt;
 	printf("init %ld index\n",i);
	return RCOK;
}
#endif

uint64_t IndexLearned::get_count() {
	#if WORKLOAD == TPCC
	return _index_size;
	#else
	return g_synth_table_size/g_node_cnt;
	#endif
}

RC IndexLearned::init(int part_cnt, table_t *table, uint64_t bucket_cnt) {
#if WORKLOAD == TPCC
    bucket_cnt = part_cnt;
#endif
    init(bucket_cnt);
	this->table = table;
	return RCOK;
}

void IndexLearned::index_delete() {
	for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
		_buckets[0][n].delete_bucket();
	}
	mem_allocator.free(_buckets[0],sizeof(BucketHeader) * _bucket_cnt_per_part);
	delete _buckets;
}

void IndexLearned::index_reset() {
	for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
		_buckets[0][n].delete_bucket();
	}
}

bool IndexLearned::index_exist(idx_key_t key) {
	assert(false);
}

void
IndexLearned::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void
IndexLearned::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}


RC IndexLearned::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
    uint64_t range = RANGE_SIZE;

	int index_key = (key/g_node_cnt)/range;
    int subscript = (key/g_node_cnt)%range;

	// uint64_t offset = (char*)(&leaf_index_info[index_key]) - rdma_global_buffer;
	// assert(offset < rdma_index_size);
	leaf_index_info[index_key].keys[subscript] = key;
	leaf_index_info[index_key].offsets[subscript] = (char*)(item->location) - rdma_global_buffer;
    leaf_index_info[index_key].key_cnt ++;
    assert(key >= index_key*range_size*g_node_cnt && key <= (index_key+1) *range_size*g_node_cnt);

	return rc;
}

// todo:之后可能要改
RC IndexLearned::index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);

	// 2. update the latch list
	cur_bkt->insert_item_nonunique(key, item, part_id);

	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexLearned::get_index_by_id(uint64_t index, itemid_t * &item, int part_id) {
	RC rc = RCOK;

	uint64_t index_key = index;

	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

	// item->location = leaf_index_info[index_key].address;
	// item->type = leaf_index_info[index_key].type;
	// item->valid = index_info[index_key].valid;

	return rc;
}

RC IndexLearned::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	RC rc = RCOK;

	uint64_t index_key = key/g_node_cnt;

	// assert(index_info[index_key].key == key);
	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	// item->location = index_info[index_key].address;
	// item->type = index_info[index_key].type;
	// item->valid = index_info[index_key].valid;

	return rc;

}
// todo:之后可能要改
RC IndexLearned::index_read(idx_key_t key, int count, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);

	cur_bkt->read_item(key, count, item);

	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

RC IndexLearned::learn_index_node_read(idx_key_t key, LeafIndexInfo *&leaf_node, int part_id, int thd_id){
    RC rc = RCOK;

    auto position = pgm_index[g_node_id]->search(key);
    int subscript = position.pos;
    int subscript1 = position.pos;
    // printf("[index_learned.cpp:215]key = %ld,position = %d\n",key,subscript);

    bool get_data = false;
    int high = subscript+64 < ((g_synth_table_size/g_node_cnt)/range_size)?subscript+64:(g_synth_table_size/g_node_cnt)/range_size;
    int low = subscript-64 > 0 ? subscript-64 : 0;
    while(get_data == false){
        // printf("[index_learned.cpp:242]low = %ld,high = %ld,subscript=%ld\n",low,high,subscript);
        for(int i = 0;i < leaf_index_info[subscript].key_cnt;i++){
            if(leaf_index_info[subscript].keys[i] == key){
	            // *leaf_node = *(leaf_index_info + subscript);
                leaf_node = &leaf_index_info[subscript];
                get_data = true;
                break;
            }
        }
        if(get_data == false){
            if(subscript == 0){
                low = 0;
                high = 64;
                subscript = (low + high)/2;
                continue;
            }
            else if(subscript == ((g_synth_table_size/g_node_cnt)/range_size)){
                high = ((g_synth_table_size/g_node_cnt)/range_size);
                low = high -64;
                subscript = (low + high)/2;
                continue;
            }

            uint64_t key_cnt = leaf_index_info[subscript].key_cnt;
            if(key > leaf_index_info[subscript].keys[ key_cnt- 1]){
                    low = subscript + 1;
            }
            else{
                   high = subscript - 1;
            }
            subscript = (low + high)/2;
            if(low > high)break;
        }else{
            break;
        }
    }

    assert(get_data == true);
    return rc;
}

//***********//TODO: 读本地learn index
RC IndexLearned::index_read(idx_key_t key, itemid_t * &item,int part_id, int thd_id) {
	RC rc = RCOK;
	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

    auto position = pgm_index[g_node_id]->search(key);
    int subscript = position.pos;
    int subscript1 = position.pos;
    // printf("[index_learned.cpp:215]key = %ld,position = %d\n",key,subscript);

    bool get_data = false;
    int high = subscript+64 < ((g_synth_table_size/g_node_cnt)/range_size)?subscript+64:(g_synth_table_size/g_node_cnt)/range_size;
    int low = subscript-64 > 0 ? subscript-64 : 0;
    while(get_data == false){
        // printf("[index_learned.cpp:276]low = %ld,high = %ld,key=%ld\n",low,high,key);
        for(int i = 0;i < leaf_index_info[subscript].key_cnt;i++){
            if(leaf_index_info[subscript].keys[i] == key){
	            // item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
                item->offset = leaf_index_info[subscript].offsets[i];
                item->location = (row_t *)(rdma_global_buffer + item->offset);
                item->leaf_node_offset = sizeof(LeafIndexInfo)*subscript + rdma_pgm_index_para_size;
                item->range_lock =  leaf_index_info[subscript].intent_lock;
                get_data = true;
                break;
            }
        }
        if(get_data == true)break;
        if(get_data == false){
            if(subscript == 0){
                low = 0;
                high = 64;
                subscript = (low + high)/2;
                continue;
            }
            else if(subscript == ((g_synth_table_size/g_node_cnt)/range_size)){
                high = ((g_synth_table_size/g_node_cnt)/range_size);
                low = high -64;
                subscript = (low + high)/2;
                continue;
            }

            uint64_t key_cnt = leaf_index_info[subscript].key_cnt;
            if(key > leaf_index_info[subscript].keys[ key_cnt- 1]){
                    low = subscript + 1;
            }
            else{
                   high = subscript - 1;
            }
            subscript = (low + high)/2;
            if(low > high)break;
        }else{
            break;
        }
    }

    // if(get_data == false){
    //     printf("[index_learned.cpp:244]cannot find data\n");
    // }
    assert(get_data == true);

	// item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	// assert(index_info[index_key].key == key);
	// item->location = index_info[index_key].address;
	// item->type = index_info[index_key].type;
	// item->valid = index_info[index_key].valid;                               

	return rc;
}
