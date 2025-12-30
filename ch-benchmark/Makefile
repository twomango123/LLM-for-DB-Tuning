#Copyright 2014 Florian Wolf, SAP AG
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

CC=g++
CXXFLAGS=-c -std=c++11 -O2 -Wall -g
LDFLAGS=-lodbc -pthread

# 调试编译标志
DEBUG_CXXFLAGS=-c -std=c++11 -O0 -Wall -g3 -ggdb -DDEBUG -fno-omit-frame-pointer
DEBUG_LDFLAGS=-lodbc -pthread -g3 -ggdb

SOURCES=src/AnalyticalStatistic.cpp src/TransactionalStatistic.cpp src/dialect/DialectStrategy.cpp src/Config.cpp src/Log.cpp src/DbcTools.cpp src/DataSource.cpp src/TupleGen.cpp src/Schema.cpp src/Queries.cpp src/RewriteDML.cpp src/Transactions.cpp chBenchmark.cpp
OBJECTS=$(SOURCES:.cpp=.o)
DEBUG_OBJECTS=$(SOURCES:.cpp=_debug.o)
EXECUTABLE=chBenchmark
DEBUG_EXECUTABLE=chBenchmark_debug
TEST_EXECUTABLE=rewrite_test
PLAN_TEST_EXECUTABLE=rewrite_plan
TRI_TEST_EXECUTABLE=rewrite_tri
MERGE_TEST_EXECUTABLE=rewrite_merge
SPLIT_TEST_EXECUTABLE=rewrite_split
DELAYED_VISUAL_EXECUTABLE=delayed_insert_test

all: $(SOURCES) $(EXECUTABLE)
	
# 调试版本目标
debug: $(DEBUG_EXECUTABLE)


$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)

# 调试版本
$(DEBUG_EXECUTABLE): $(DEBUG_OBJECTS) 
	$(CC) $(DEBUG_OBJECTS) -o $@ $(DEBUG_LDFLAGS)

.cpp.o:
	$(CC) $(CXXFLAGS) $< -o $@ -I/usr/local/unixODBC/include 

# 调试版本对象文件
%_debug.o: %.cpp
	$(CC) $(DEBUG_CXXFLAGS) $< -o $@ -I/usr/local/unixODBC/include

# 运行调试版本
run_debug: debug
	./$(DEBUG_EXECUTABLE)

# 使用 gdb 运行调试版本
gdb: debug
	gdb ./$(DEBUG_EXECUTABLE)

# 使用 valgrind 检查内存
valgrind: debug
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose ./$(DEBUG_EXECUTABLE)

# 使用 AddressSanitizer 版本
asan: CXXFLAGS += -fsanitize=address -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address
asan: clean $(EXECUTABLE)
	@echo "编译完成，使用 ASAN_OPTIONS=detect_leaks=1 ./$(EXECUTABLE) 运行"


# 清理
clean:
	rm -f *.o
	rm -f src/*.o
	rm -f src/*_debug.o
	rm -f src/dialect/*.o
	rm -f src/dialect/*_debug.o
	rm -f $(EXECUTABLE) $(DEBUG_EXECUTABLE)
	rm -f $(TEST_EXECUTABLE)
	rm -f $(PLAN_TEST_EXECUTABLE)
	rm -f $(TRI_TEST_EXECUTABLE)
	rm -f $(MERGE_TEST_EXECUTABLE)
	rm -f $(SPLIT_TEST_EXECUTABLE)

# 另外
.PHONY: all debug run_debug gdb valgrind asan clean install help

# Lightweight test to print rewrite plan without ODBC link
test_rewrite:
	$(CC) -std=c++11 -O2 -Wall -g -DREWRITE_NO_ODBC \
	  src/RewriteDML.cpp src/RewriteDMLTest.cpp \
	  -o $(TEST_EXECUTABLE)
	@echo "Run ./$(TEST_EXECUTABLE) to see rewrite plans."

plan_only:
	$(CC) -std=c++11 -O2 -Wall -g -DREWRITE_NO_ODBC \
	  src/RewriteDML.cpp src/RewritePlanTest.cpp \
	  -o $(PLAN_TEST_EXECUTABLE)
	@echo "Run ./$(PLAN_TEST_EXECUTABLE) to see generated plans."

tri_only:
	$(CC) -std=c++11 -O2 -Wall -g -DREWRITE_NO_ODBC \
	  src/RewriteDML.cpp src/RewriteTriStateTest.cpp \
	  -o $(TRI_TEST_EXECUTABLE)
	@echo "Run ./$(TRI_TEST_EXECUTABLE) to see tri-state style results."

merge_only:
	$(CC) -std=c++11 -O2 -Wall -g \
	  src/RewriteMergeBufferTest.cpp \
	  -o $(MERGE_TEST_EXECUTABLE)
	@echo "Run ./$(MERGE_TEST_EXECUTABLE) to see delayed merge + dedup demo."

split_only:
	$(CC) -std=c++11 -O2 -Wall -g -DREWRITE_NO_ODBC \
	  src/RewriteDML.cpp src/RewriteSplitDedupTest.cpp \
	  -o $(SPLIT_TEST_EXECUTABLE)
	@echo "Run ./$(SPLIT_TEST_EXECUTABLE) to see split dedup demo."

# Visual: delayed insert demos (no ODBC)
delayed_only:
	$(CC) -std=c++11 -O2 -Wall -g -DREWRITE_NO_ODBC \
	  src/RewriteDML.cpp src/DelayedInsertVisualTest.cpp \
	  -o $(DELAYED_VISUAL_EXECUTABLE)
	@echo "Run ./$(DELAYED_VISUAL_EXECUTABLE) to see delayed insert scenarios."
