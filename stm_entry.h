#pragma once
#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

struct Buffer {
    uint8_t * data;
    size_t len;
};

struct AccessSet {
    uint64_t * items;
    size_t rsize;
    size_t wsize;
};

char * init_vm();

AccessSet entry_vm(const char * genesis, uint64_t balance, uint64_t seq_num, uint64_t transfer_amount);

AccessSet init_entry_vm(const char * _genesis, uint64_t balance, uint64_t seq_num, uint64_t transfer_amount);

} // extern "C"
