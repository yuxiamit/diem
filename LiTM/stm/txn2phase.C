#include "txn.h"
#include "txn2phase.h"
#include "parallel.h"
#include "utils.h"
#include <algorithm>
#include <iterator>
#include <iostream>
#include <stdio.h>
#include <mm_malloc.h>
#include <math.h>
using namespace std;

#if DEBUG
intT readCounter = 0;
intT writeCounter = 0;
#endif

#define WAR_OPT true 
uint32_t * rs_ptrs;
uint32_t * ws_ptrs;
uint32_t maxRS = 0;
__thread int thread_id;
__thread bool initialized;
__thread TxnMan2Phase * txn_man;

uint32_t totalHoles = 0;

TxnMan2Phase::TxnMan2Phase(int thread_id)
{
	assert(global_man);
	//_max_buffer_size = 655360;
	secondRound = checkFailure = false;
	_rs_size = 0;
	_ws_size = 0;
	assert(thread_id < (1 << 8));
	rs_records = (uint32_t *)((uint64_t)rs_records_base +  thread_id * (1<<24)); //7200016); //(1<<23));
	ws_records = ws_records_base +  thread_id * (1<<24); //7200016; // (1<<23);
	next_rs_record = rs_records;
	next_ws_record = ws_records;
	curr_ws_end = next_ws_record + 4;
}

bool 
TxnMan2Phase::process_phase2( uint32_t id ) {
	_priority = id + base_pri;
  #if REPEATEXEC
	// we don't have read set here.
  #else
	// check read set.
	uint32_t * rs = (uint32_t*)((uint64_t)rs_ptrs[id] + (uint64_t)rs_records_base);
	_rs_size = rs[0]; 
	for (int i=0;i<_rs_size;i++) {
		uint32_t id = rs[i+1]; 
		uint32_t en = lock_table[id];
		if (_priority > en)	
			return true;
	}
  #endif

	// check write set.
	char * ws = (char *)((uint64_t)ws_ptrs[id] + (uint64_t)ws_records_base);
	_ws_size = *(uint32_t *)ws;
	ws += sizeof(uint32_t);
	char * end = ws;
	for (int i=0;i<_ws_size;i++) {
		uint64_t key = *(uint64_t*)end;
		// check the validity of reads
		uint32_t en = lock_table[mhash(key)];
		if (_priority > en)		
			return true;
		end += 8;
		uint32_t size = *(uint32_t*)end;
		end += size + 4; 
	}
	// copy write set. 
	end = ws;
	for (int i=0;i<_ws_size;i++) {
		uint64_t key = *(uint64_t*)end;
		end += 8;
		uint32_t size = *(uint32_t*)end;
//		assert(size == 1);
		end += 4;
		memcpy((char *)key, end, size); 
		end += size; 
	}
	return false;
}



#if REPEATEXEC
bool 
TxnMan2Phase::process_phase3( uint32_t id ) {
	char * ws = (char *)((uint64_t)ws_ptrs[id] + (uint64_t)ws_records_base);
	_ws_size = *(uint32_t *)ws;

	ws += sizeof(uint32_t);
	char * end = ws;
	for (int i=0;i<_ws_size;i++) {
		uint64_t key = *(uint64_t*)end;
		//std::cout << "key " << key << std::endl;
		end += 8;
		uint32_t size = *(uint32_t*)end;
//		assert(size == 1);
		end += 4;
		memcpy((char *)key, end, size); 
		end += size; 
	}
	return true;
}
#endif


uint32_t parseKey(uint8_t * ptr) {
  // every address is a [u8; 16], two addresses per read
        uint64_t key = *(uint64_t*)(ptr);
        key ^= *(uint64_t*)(ptr + 8);
        key ^= *(uint64_t*)(ptr + 16);
        key ^= *(uint64_t*)(ptr + 24);
        return (uint32_t)(key % MAX_ADDRESS);
}


intT speculative_for_vm(DVector<char> &f, intT s, intT e, int roundsize, 
		     bool hasState, int maxTries) {
	int granularity = (e - s) / roundsize;
	if (maxTries < 0) maxTries = 100 + 200*granularity;
	intT maxRoundSize = roundsize;

	intT *I = newA(intT,maxRoundSize);
	intT *Ihold = newA(intT,maxRoundSize);
	rs_ptrs = newA(uint32_t, maxRoundSize);
	ws_ptrs = newA(uint32_t, maxRoundSize);

	bool *keep = newA(bool,maxRoundSize);
	parallel_for(intT i=0;i<maxRoundSize;i++)
		keep[i]=false;
	
	base_pri = UINT_T_MAX / 2;
	upper_pri = UINT_T_MAX / 2;

	int round = 0; 
	intT numberDone = s; // number of iterations done
	intT numberKeep = 0; // number of iterations to carry to next round
	intT totalProcessed = 0;
	intT curRoundSize = maxRoundSize; //40000; //maxRoundSize; //10000;
        intT totalKeep = 0;
	
	char * states = init_vm();

	uint64_t allmem = 0;
	while (numberDone < e) {
		if (round++ > maxTries) {
			std::cout << "speculativeLoop: too many iterations, increase maxTries parameter" << endl;
			abort();
		}
		intT size = min(curRoundSize, e - numberDone);
		assert(base_pri > size);
		base_pri -= size;
		totalProcessed += size;
		uint64_t t1 = get_sys_clock();  // TODO: change to INTEL on-chip clock
		global_man->_phase = 0;
		
		parallel_for (intT i1 =0; i1 < size; i1++) {
			if (__builtin_expect(!initialized, 0)) {
				initialized = true;
				thread_id = omp_get_thread_num(); // __cilkrts_get_worker_number();
			}
			txn_man = local_txn_man[thread_id];
			if (i1 >= numberKeep)
				I[i1] = numberDone + i1;
			TXN_START(i1 + base_pri, i1);
			// run txn
			Buffer b = entry_vm(states, 1, 0, 100); // init_entry_vm(NULL, 1, 0, 100);
			
			uint32_t read_len = *(uint32_t*)b.data;
			uint32_t write_len = *(uint32_t*)(b.data + 4);
			
			assert(b.len >= 8 + (read_len + write_len) * 32);

			char ans = 0;
			for(uint32_t i =0; i< read_len; i++)
			{
				uint32_t key = parseKey(b.data + 8 + 32 * i);
				ans += f[key]; // simulate read
			}
			for(uint32_t i =0; i< write_len; i++)
			{
				uint32_t key = parseKey(b.data + 8 + 32 * i);
				f[key] = (char)ans; // simulate write
			}
			// txn end
			keep[i1] = TXN_END
		}
		global_man->_phase = 1;
		uint64_t tt = get_sys_clock();
		*global_man->stats1[0] += tt - t1;  // execution phase
		parallel_for(intT i2 = 0; i2<size; i2++) {

		bool isHole = false;
		txn_man = local_txn_man[thread_id];
		if (keep[i2]) {

			// didn't finish in phase 1. should check dependency in phase 2.
			keep[i2] = txn_man->process_phase2( i2 );
			isHole = keep[i2];
		}
	}

	uint64_t t3 = get_sys_clock();
	*global_man->stats2[0] += t3 - tt;

	// apply writes for deterministic without healing
	#if REPEATEXEC
	parallel_for(intT i=0; i<size; i++) {
		txn_man = local_txn_man[thread_id];
		if(!keep[i])
			txn_man ->process_phase3(i); 
	}
	#endif
	uint64_t currentMem = 0;

        for(int i=0; i<size; i++)
	{
		//int i = I[ki];
		uint32_t *rs = (uint32_t *)((uint64_t)rs_ptrs[i] + (uint64_t)rs_records_base);
		currentMem += 4 + rs[0] * sizeof(uint32_t); // rs_size
		//cout << "rs size " << rs[0] * sizeof(uint32_t) << endl;
	        char * ws = (char *)((uint64_t)ws_ptrs[i] + (uint64_t)ws_records_base);
	        currentMem += 4 + *(uint32_t*)ws;	
		//cout << "ws size" << *(uint32_t*)ws << endl;
	}

	if(currentMem > allmem) allmem = currentMem;

	numberKeep = sequence::pack(I, Ihold, (bool *)keep, size);
	swap(I, Ihold);

	totalKeep += numberKeep;
	numberDone += size - numberKeep;

	parallel_for (intT i=0; i<getWorkers(); i++)
	local_txn_man[i]->cleanup();
	*global_man->stats3[0] += get_sys_clock() - t3;

	assert(numberKeep < size);

	}

	cout << endl << "allmem " << allmem << endl << "maxRS " << maxRS << endl;
	cout << endl << totalHoles << endl;
	cout << "total keep " << totalKeep << endl;

	free(I); free(Ihold); 

	#if PRINT_TIME
	cout << "Time " << *(global_man->stats1[0]) << "," << *(global_man->stats2[0]) << "," << *(global_man->stats3[0]) << endl;
	#endif

	free(keep); 
	//if(hasState) free(state);

	return totalProcessed;
}

