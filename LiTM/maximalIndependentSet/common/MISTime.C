// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2011 Guy Blelloch and the PBBS team
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <iostream>
#include <algorithm>
#include <cstdlib>
#include <string.h>
#include "gettime.h"
#include "utils.h"
#include "graph.h"
#include "parallel.h"
#include "IO.h"
#include "graphIO.h"
#include "parseCommandLine.h"
#include "MIS.h"
#include "../stm/dsm.h"
#include "../../../stm_entry.h"
#include "../stm/txn2phase.h"

using namespace std;
using namespace benchIO;
#define MAX_ADDRESS (0x7ffff)
int batchSize;

void timeMIS(graph<intT> G, int rounds, char* outFile) {
  graph<intT> H = G.copy(); //because MIS might modify graph
  char* flags = maximalIndependentSet(H);
  for (int i=0; i < rounds; i++) {
    free(flags);
    H.del();
    H = G.copy();
    startTime();
    flags = maximalIndependentSet(H);
    nextTimeN();
  }
  cout << endl;

  if (outFile != NULL) {
    int* F = newA(int, G.n);
    for (int i=0; i < G.n; i++) 
    {
      F[i] = flags[i];
    }
    writeIntArrayToFile(F, G.n, outFile);
    free(F);
  }

  free(flags);
  G.del();
  H.del();
}

void test_stm_entry() {
    //const char * p = init_vm();

    // Buffer b = init_entry_vm(NULL, 1, 0, 100);
    // printf("Buffer len: %d\n", b.len);

    int num_txns = 10;
    int batch_size = 10;
    int step = 1;
    int address_space = MAX_ADDRESS;
    DVector<char> Flags(address_space, 0);
    global_man = new GlobalMan(num_txns);
    //global_man = new GlobalMan(65536 * 16);
    local_txn_man = new TxnMan2Phase * [getWorkers()];
    for (int i=0; i<getWorkers();i++) { 
      local_txn_man[i] = (TxnMan2Phase *) _mm_malloc(sizeof(TxnMan2Phase), 64); 
      new(local_txn_man[i]) TxnMan2Phase(i);
    }

    speculative_for_vm(Flags, 0, num_txns/step, batch_size/step); // passing granularity
}

int parallel_main(int argc, char* argv[]) {
  commandLine P(argc, argv, "[-o <outFile>] [-r <rounds>] [-b <batchSize>] [-c <threadCount>] <inFile>");
  char* iFile = P.getArgument(0);
  char* oFile = P.getOptionValue("-o");
  int rounds = P.getOptionIntValue("-r",1);
  batchSize = P.getOptionIntValue("-b", 10000);
  /*
  int cilkThreadCount = P.getOptionIntValue("-c", -1);
  if(cilkThreadCount > 0)
	{
		//std::string s = std::to_string(cilkThreadCount);
		char num[3];
		sprintf(num,"%d",cilkThreadCount);
		__cilkrts_end_cilk();
		__cilkrts_set_param("nworkers", num);
		__cilkrts_init();
		std::cout << "The number of threads " << cilkThreadCount << std::endl;
	}
  */
  test_stm_entry();
  //graph<intT> G = readGraphFromFile<intT>(iFile);
  //timeMIS(G, rounds, oFile);
}

