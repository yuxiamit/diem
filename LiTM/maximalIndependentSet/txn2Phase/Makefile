STM_PATH=../../stm/
include parallelDefs

# required files
GLOBAL_REQUIRE = utils.h parallel.h sequence.h graph.h
OBJS = MIS.o $(STM_PATH)/txn.o $(STM_PATH)/txn2phase.o
LIBS = -lstm_entry -ldl

include ../common/timeRequiredFiles
include MakeBench
include $(STM_PATH)/MakeSTM
