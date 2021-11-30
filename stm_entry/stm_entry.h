#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

const char *init_vm();

void entry_vm(const char *genesis, uint64_t balance, uint64_t seq_num, uint64_t transfer_amount);

} // extern "C"
