# STM Bench Interface

## How to use this lib:

Step 1: navigate to *stm_entry* folder

        cargo build

Step 2: navigate to *LiTM/maximalIndependentSet/txn2Phase* folder

        make

Then you should have a MIS executable. Run it.

        ./MIS

## Change the parameter

The function test_stm_entry in LiTM/maximalIndependentSet/common/MISTime.C has parameters.

        int num_txns = 10000;
        int batch_size = 50;
        int step = 1;
        int address_space = MAX_ADDRESS;