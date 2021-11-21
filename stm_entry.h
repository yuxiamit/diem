#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void entry_vm(uintptr_t idx,
              uint64_t from,
              uint64_t to,
              uint64_t seq_num,
              uint64_t transfer_amount);

} // extern "C"
