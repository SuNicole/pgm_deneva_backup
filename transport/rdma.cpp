#include "global.h"
#include <pwd.h>
#include <unistd.h>
#include "config.h"
#include "rdma.h"
#include <stdio.h>
//#include "rdma_ctrl.hpp"
#include "lib.hh"
#include <assert.h>
#include <string>
#include "src/allocator_master.hh"
#include "index_rdma.h"
#include "index_rdma_btree.h"
#include "storage/row.h"
#include "storage/table.h"
#include "system/rdma_calvin.h"
char *Rdma::rdma_buffer; //= new char[RDMA_BUFFER_SIZE];
char ** Rdma::ifaddr = new char *[g_total_node_cnt+20];

uint64_t Rdma::get_socket_count() {
	uint64_t sock_cnt = 0;
	if(ISCLIENT)
		sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt * g_servers_per_client;
	else
		sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt;
	return sock_cnt;
}

void Rdma::read_ifconfig(const char * ifaddr_file) {
  	// ifaddr = new char *[g_total_node_cnt];
	uint64_t cnt = 0;
	//read IP from ifconfig.txt
	printf("Reading ifconfig file: %s\n",ifaddr_file);
	ifstream fin(ifaddr_file);
	string line;
	while (getline(fin, line)) {
		//memcpy(ifaddr[cnt],&line[0],12);
		//init
		ifaddr[cnt] = new char[line.length()+1];
		//assignment
		strcpy(ifaddr[cnt],&line[0]);
		//output
		printf("%ld: %s\n",cnt,ifaddr[cnt]);
		cnt++;
	}
  	assert(cnt == g_total_node_cnt);
}

string Rdma::get_path() {
	string path;
#if SHMEM_ENV
 	 path = "/dev/shm/";
#else
	char * cpath;
  	cpath = getenv("SCHEMA_PATH");
	if(cpath == NULL)
		path = "./";
	else
		path = string(cpath);
#endif
	path += "ifconfig.txt";
  	return path;

}

uint64_t Rdma::get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id) {
	uint64_t port_id = 0;
	DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
	// printf("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
	port_id += g_total_node_cnt * dest_node_id;
	DEBUG("%ld\n",port_id);
	port_id += src_node_id;
	DEBUG("%ld\n",port_id);
	port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
	DEBUG("%ld\n",port_id);
	port_id += 30000;
	port_id = port_id + 1;
	DEBUG("%ld\n",port_id);

	printf("Port ID:  %ld, %ld -> %ld : %ld\n",send_thread_id,src_node_id,dest_node_id,port_id);
	return port_id;
}

uint64_t get_rm_id(uint64_t node_id,uint64_t thread_id){
	uint64_t rm_id = 0;
	rm_id = node_id * 10 + thread_id;
	return rm_id;
}

uint64_t Rdma::get_port(uint64_t node_id){
  uint64_t port_id = 0;
  port_id = RDMA_TPORT + node_id;
//   printf("port id = %ld\n",port_id);
  //port_id = TPORT_PORT + 344 + node_id;
  return port_id ;
}

int get_msg_size(){
    int msg_size = 0;
    uint64_t segment_vector_size = pgm_index[g_node_id]->get_segment().size();
    uint64_t level_size = pgm_index[g_node_id]->get_level_offset().size();
    uint64_t segment_size = sizeof(uint64_t) + sizeof(float) + sizeof(int32_t);

    
    msg_size += sizeof(uint64_t)*2;
    msg_size += sizeof(uint64_t);
    msg_size += sizeof(size_t);
    msg_size +=segment_size * segment_vector_size + sizeof(size_t)*level_size;
}

void * Rdma::client_qp(void *arg){

	printf("\n====client====");

	rdmaParameter *arg_tmp;
	arg_tmp = (rdmaParameter*)arg;
	uint64_t node_id = arg_tmp->node_id;
	uint64_t thread_num = arg_tmp->thread_num;

	printf("\n node_id = %d \n",node_id);

	ConnectManager cm_(std::string(rdma_server_add[node_id]));

	printf("address = %s\n",rdma_server_add[node_id].c_str());

	if (cm_.wait_ready(10000000, 16) == IOCode::Timeout) RDMA_ASSERT(false) << "cm connect to server timeout";

	uint64_t reg_nic_name = node_id;
	uint64_t reg_mem_name = node_id;

	//   uint64_t reg_nic_name = 0;
	//   uint64_t reg_mem_name = 0;
	struct passwd *pwd = getpwuid(getuid());
	printf("login account：%s\n", pwd->pw_name);

	auto fetch_res = cm_.fetch_remote_mr(reg_mem_name);
	RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
	rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
	remote_mr_attr[node_id] = std::get<1>(fetch_res.desc);
	#if USE_COROUTINE
		for(int thread_id = 0;thread_id < g_total_thread_cnt * (COROUTINE_CNT + 1); thread_id ++){
		#if CC_ALG == RDMA_CALVIN
			if (thread_id > g_thread_cnt * (COROUTINE_CNT + 1) && thread_id <= (g_total_thread_cnt - 2)* (COROUTINE_CNT + 1)) continue;
		#else 
			// if (thread_id > g_thread_cnt * (COROUTINE_CNT + 1)) continue;
		#endif
	#else
		for(int thread_id = 0;thread_id < g_total_thread_cnt ; thread_id ++){
		#if CC_ALG == RDMA_CALVIN
			if (thread_id > g_thread_cnt && thread_id <= g_total_thread_cnt - 2) continue;
		#else 
			if (thread_id > g_thread_cnt) continue;
		#endif
	#endif
		
		// pthread_mutex_lock( RDMA_QP_LATCH );
		Option<Arc<RDMARC>> qp2 = RDMARC::create(nic,QPConfig());
		rc_qp[node_id][thread_id] = qp2.value();
		// rc_qp[node_id][thread_id] = RDMARC::create(nic, QPConfig()).value();

		string usename = pwd->pw_name;
		string name = "cl-qp" + std::to_string(g_node_id) + "b" +std::to_string(node_id) + "c" + std::to_string(thread_id);
		qp_name[node_id][thread_id] = name;

		// printf("qp_name = %s, rdma_server_port[node_id] = %d \n",qp_name[node_id][thread_id].c_str(),rdma_server_port[node_id]);
		auto qp_res = cm_.cc_rc(qp_name[node_id][thread_id], rc_qp[node_id][thread_id], reg_nic_name, QPConfig());
		RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
		auto key = std::get<1>(qp_res.desc);
		// RDMA_LOG(4) << "client fetch QP authentical key: " << key;

		rc_qp[node_id][thread_id]->bind_remote_mr(remote_attr);
		rc_qp[node_id][thread_id]->bind_local_mr(client_rm_handler->get_reg_attr().value());
		// pthread_mutex_unlock( RDMA_QP_LATCH );
	}
	// cm.push_back(cm_);

	printf("server %d QP connect to server %d\n",g_node_id,node_id);

	return NULL;
}

void * Rdma::server_qp(void *){
	printf("\n====server====\n");
	printf("rdma_server_port[g_node_id] = %d\n",rdma_server_port[g_node_id]);
	rm_ctrl = Arc<RCtrl>(new rdmaio::RCtrl(rdma_server_port[g_node_id]));

	uint64_t reg_nic_name = g_node_id;
	uint64_t reg_mem_name = g_node_id;

	// uint64_t reg_nic_name = 0;
	// uint64_t reg_mem_name = 0;

	RDMA_ASSERT(rm_ctrl->opened_nics.reg(reg_nic_name, nic));

	RDMA_ASSERT(rm_ctrl->registered_mrs.create_then_reg(
			reg_mem_name, rdma_rm,
			rm_ctrl->opened_nics.query(reg_nic_name).value()));

	rdma_global_buffer = (char*)(rm_ctrl->registered_mrs.query(reg_mem_name)
								.value()
								->get_reg_attr()
								.value()
								.buf);
	rdma_txntable_buffer = rdma_global_buffer + (rdma_buffer_size - rdma_txntable_size);
	rdma_calvin_buffer = rdma_global_buffer + (rdma_buffer_size - rdma_txntable_size - rdma_calvin_buffer_size);
	rm_ctrl->start_daemon();

	return NULL;
}

char* Rdma::get_index_client_memory(uint64_t thd_id, int num) { //num>=1
	char* temp = (char *)(client_rdma_rm->raw_ptr);
#if INDEX_STRUCT == IDX_RDMA_BTREE
    temp += sizeof(rdma_bt_node) * ((num-1) * g_total_thread_cnt * (COROUTINE_CNT + 1) + thd_id);
#elif INDEX_STRUCT == IDX_LEARNED
	temp += sizeof(LeafIndexInfo) * ((num-1) * g_total_thread_cnt * (COROUTINE_CNT + 1) + thd_id);
#else
    temp += sizeof(IndexInfo) * ((num-1) * g_total_thread_cnt * (COROUTINE_CNT + 1) + thd_id);
#endif
	return temp;
}

char* Rdma::get_row_client_memory(uint64_t thd_id, int num) { //num>=1
	//when num>1, get extra row for doorbell batched RDMA requests
	char* temp = (char *)(client_rdma_rm->raw_ptr);
#if INDEX_STRUCT == IDX_RDMA_BTREE
    temp +=  sizeof(rdma_bt_node) * ((max_batch_index+1) * g_total_thread_cnt * (COROUTINE_CNT + 1));
#elif INDEX_STRUCT == IDX_LEARNED
	temp +=  sizeof(LeafIndexInfo) * ((max_batch_index+1) * g_total_thread_cnt * (COROUTINE_CNT + 1));
#else
    temp +=  sizeof(IndexInfo) * (max_batch_index * g_total_thread_cnt * (COROUTINE_CNT + 1));
#endif
	temp += row_t::get_row_size(ROW_DEFAULT_SIZE) * ((num-1) * g_total_thread_cnt * (COROUTINE_CNT + 1) + thd_id);
	return temp;
}

char* Rdma::get_param_client_memory(int num) { //num>=1
	//when num>1, get extra row for doorbell batched RDMA requests
	char* temp = (char *)(client_rdma_rm->raw_ptr);

	// temp +=  sizeof(LeafIndexInfo) * ((max_batch_index+1) * g_total_thread_cnt * (COROUTINE_CNT + 1));
	// temp += row_t::get_row_size(ROW_DEFAULT_SIZE) * (g_total_thread_cnt * (COROUTINE_CNT + 1) + g_total_thread_cnt);
	return temp;
}

/*
char* Rdma::get_table_client_memory(uint64_t thd_id) {
	char* temp = (char *)(client_rdma_rm->raw_ptr + sizeof(IndexInfo) * g_total_thread_cnt);
 	temp += row_t::get_row_size(ROW_DEFAULT_SIZE) * g_total_thread_cnt;
  	temp += sizeof(table_t) * thd_id;
  	return temp;
}
*/

#if CC_ALG == RDMA_CALVIN
char* Rdma::get_queue_client_memory() {
	char * temp =  (char *)(client_rdma_rm->raw_ptr);
  	return temp;
}

char* Rdma::get_rear_client_memory() {
	char * temp =  (char *)(client_rdma_rm->raw_ptr) + g_msg_size;
  	return temp;
}
#endif

#if 0
void *malloc_huge_pages(size_t size,uint64_t huge_page_sz,bool flag)
{
  char *ptr; // the return value
#define ALIGN_TO_PAGE_SIZE(x)  (((x) + huge_page_sz - 1) / huge_page_sz * huge_page_sz)
  size_t real_size = ALIGN_TO_PAGE_SIZE(size + huge_page_sz);

  if(flag) {
	// Use 1 extra page to store allocation metadata
	// (libhugetlbfs is more efficient in this regard)
	char *ptr = (char *)mmap(NULL, real_size, PROT_READ | PROT_WRITE,
							 MAP_PRIVATE | MAP_ANONYMOUS |
							 MAP_POPULATE | MAP_HUGETLB, -1, 0);
	if (ptr == MAP_FAILED) {
	  // The mmap() call failed. Try to malloc instead
	  LOG(4) << "huge page alloc failed!";
	  goto ALLOC_FAILED;
	} else {
	  LOG(2) << "huge page real size " << (double)(get_memory_size_g(real_size)) << "G";
	  // Save real_size since mmunmap() requires a size parameter
	  *((size_t *)ptr) = real_size;
	  // Skip the page with metadata
	  return ptr + huge_page_sz;
	}
  }
ALLOC_FAILED:
  ptr = (char *)malloc(real_size);
  if (ptr == NULL) return NULL;
  real_size = 0;
  return ptr + huge_page_sz;
}

#endif

void Rdma::get_pgm_para(){
    int get_num = g_node_cnt - 1;
    printf("[rdma.cpp:288]get pgm para\n");
    // while(get_num > 0){
        for(int i = 0;i < g_node_cnt;i++){
            if(i == g_node_id)continue;

            int msg_size = 0;
            msg_size = get_msg_size();
            char *buf = (char *)malloc(msg_size);
            memcpy(buf,(char*)(rdma_global_buffer + i * 1024*1024L),msg_size);
            printf("[310]%d\n",*(size_t *)buf);

            uint64_t ptr = 0;

            size_t n;
            COPY_VAL(n,buf,ptr);
            pgm_index[i]->set_n(n);
           //todo - ok
            uint64_t first_key;
            COPY_VAL(first_key,buf,ptr);
            pgm_index[i]->set_first_key(first_key);

            uint64_t segments_size;
            COPY_VAL(segments_size,buf,ptr);

            // vector<pgm::PGMIndex<uint64_t,64>::Segment> segments;      
            // vector<size_t> levels_offsets; 
            printf("[rdma.cpp:327]segment_size = %ld\n",segments_size);

            vector<pgm::PGMIndex<uint64_t,64>::Segment> segments;      
            for(int j = 0;j < segments_size;j++){
                    pgm::PGMIndex<uint64_t,64>::Segment tmp_seg;
                    // printf("[rdma.cpp:330]segment_size = %ld\n",segments_size);
                    COPY_VAL(tmp_seg.key,buf,ptr);           
                    COPY_VAL(tmp_seg.slope,buf,ptr);           
                    COPY_VAL(tmp_seg.intercept,buf,ptr);
                    // pgm_index[i]->get_segment().emplace_back(tmp_seg);
                    segments.emplace_back(tmp_seg);
            }
            pgm_index[i]->set_segment(segments);                          

            uint64_t levels_offsets_size = 0;
            COPY_VAL(levels_offsets_size,buf,ptr);

            vector<size_t> levels_offsets; 
            for(int j = 0;j < levels_offsets_size;j++){
                size_t tmp_lev;
                // printf("[rdma.cpp:341]j = %d,ptr = %ld\n",j,ptr);
                COPY_VAL(tmp_lev,buf,ptr);
                levels_offsets.emplace_back(tmp_lev);
                printf("[rdma.cpp:350]tmp_level = %ld\n",tmp_lev);
                // pgm_index[i]->get_level_offset().emplace_back(tmp_lev);
                // pgm_index[i]->get_level_offset()[j] = tmp_lev;
            }
            pgm_index[i]->set_level_offset(levels_offsets);

            printf("[rdma.cpp:321]%d size = %ld\n",i,segments_size);
            get_num --;
        }
    // }
}



void Rdma::send_pgm_para(){
    int msg_size = 0;
    msg_size = get_msg_size();
    uint64_t segment_vector_size = pgm_index[g_node_id]->get_segment().size();
    uint64_t level_size = pgm_index[g_node_id]->get_level_offset().size();
    uint64_t segment_size = sizeof(uint64_t) + sizeof(float) + sizeof(int32_t);

    char *buf;
    buf = (char *)malloc(msg_size);
    printf("\nmsg_size = %d\n",msg_size);
    uint64_t ptr = 0;

    size_t n = pgm_index[g_node_id]->get_n();
    COPY_BUF(buf,n,ptr); 
    uint64_t first_key = pgm_index[g_node_id]->get_first_key();
    COPY_BUF(buf,first_key,ptr); 

    COPY_BUF(buf,segment_vector_size,ptr); 
   
    for(int i = 0;i < segment_vector_size;i++){
        COPY_BUF(buf,(pgm_index[g_node_id]->get_segment())[i].key,ptr);         
        COPY_BUF(buf,(pgm_index[g_node_id]->get_segment())[i].slope,ptr);           
        COPY_BUF(buf,(pgm_index[g_node_id]->get_segment())[i].intercept,ptr);    
    }

    COPY_BUF(buf,level_size,ptr); 

    uint64_t s = sizeof(size_t);
    printf("[rdma.cpp:381]sizeof(size_t) = %ld\n",s);
    for(int i = 0;i < level_size;i++){
        printf("[rdma.cpp:381]ptr = %ld,level_offset = %d\n",ptr,(pgm_index[g_node_id]->get_level_offset())[i]);
        COPY_BUF(buf,(pgm_index[g_node_id]->get_level_offset())[i],ptr);
    }

    char *local_buf = Rdma::get_param_client_memory(1);
    memcpy(local_buf, buf , msg_size);

    printf("[382]%d\n",*(size_t *)buf);

    printf("****381***%s\n",buf);
    for(int i = 0;i < g_node_cnt;i ++){
        if(i == g_node_id)continue;
        uint64_t remote_offset = g_node_id*1024*1024L;

        uint64_t starttime;
	    uint64_t endtime;
	    starttime = get_sys_clock();
        auto res_s = rc_qp[i][0]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = msg_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));

	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	do {
		res_p = rc_qp[i][0]->poll_send_comp();
		waitcomp_time = get_sys_clock();
	} while (res_p.first == 0);
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

    }

	sleep(3);

 

}
void Rdma::init(){
	_sock_cnt = get_socket_count();
	printf("rdma Init %d: %ld\n",g_node_id,_sock_cnt);
	string path = get_path();
	read_ifconfig(path.c_str());

	nic =  RNic::create(RNicInfo::query_dev_names().at(0)).value();

	//as server
	rdma_rm = Arc<RMem>(new RMem(rdma_buffer_size));
	rm_handler = RegHandler::create(rdma_rm, nic).value();

	//as client
	client_rdma_rm = Arc<RMem>(new RMem(client_rdma_buffer_size));
	// client_rdma_rm = Arc<RMem>(new RMem(RDMA_LOCAL_BUFFER_SIZE));
	client_rm_handler = RegHandler::create(client_rdma_rm, nic).value();

	uint64_t thread_num = 0;
	uint64_t node_id = 0;
#if USE_COROUTINE
	pthread_t *client_thread = new pthread_t[g_total_node_cnt * g_total_thread_cnt * (COROUTINE_CNT + 1)];
#else
	pthread_t *client_thread = new pthread_t[g_total_node_cnt * g_total_thread_cnt];
#endif
	pthread_t server_thread;
	printf("g_total_node_cnt = %d",g_total_node_cnt);

	for(int i = 0; i < NODE_CNT ; i++){
		rdma_server_port[i] = get_port(i);
  	}

	server_qp(NULL);
	printf("start wait\n");
	sleep(3);

	for(node_id = 0; node_id < g_total_node_cnt; node_id++) {

		if(ISCLIENTN(node_id)) continue;  //for every client

		rdma_server_add[node_id] = ifaddr[node_id] + std::string(":") + std::to_string(rdma_server_port[node_id]);
		//rdma_server_add[node_id] = ifaddr[node_id] + std::string(":") + std::to_string(server_port);

		rdmaParameter *arg = (rdmaParameter*)malloc(sizeof(rdmaParameter));
		arg->node_id = node_id;
		arg->thread_num = thread_num;

		pthread_create(&client_thread[thread_num],NULL,client_qp,(void *)arg);

		thread_num ++;
	}

	for(int i = 0;i<thread_num;i++){
		pthread_join(client_thread[i],NULL);
	}

  	char* rheader = rdma_global_buffer + rdma_index_size;
    last_index_node_order = (uint64_t*)(rdma_global_buffer+rdma_index_size-sizeof(uint64_t));
    last_row_order = (uint64_t*)(rdma_global_buffer+rdma_buffer_size-sizeof(uint64_t));
    *last_index_node_order=0;
    *last_row_order=0;
	r2::AllocatorMaster<>::init(rheader,rdma_buffer_size-rdma_index_size);

}
