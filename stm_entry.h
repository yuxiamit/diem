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

char * init_vm();

Buffer entry_vm(const char * genesis, uint64_t balance, uint64_t seq_num, uint64_t transfer_amount);

Buffer init_entry_vm(const char * _genesis, uint64_t balance, uint64_t seq_num, uint64_t transfer_amount);

} // extern "C"
