# 编译器
CXX = g++
# 编译选项
CXXFLAGS = -std=c++17 -Wall -O2

# 目标文件
OBJS = server.o worker.o test.o

# 可执行文件名
TARGETS = server worker test

# 默认目标
all: $(TARGETS)

server: server.o
	$(CXX) $(CXXFLAGS) -o $@ $^

# app2 依赖的对象文件
worker: worker.o
	$(CXX) $(CXXFLAGS) -o $@ $^

# 通用规则：编译 .cpp 成 .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理目标
clean:
	rm -f *.o $(TARGETS)
	rm -rf *.dSYM

.PHONY: all clean