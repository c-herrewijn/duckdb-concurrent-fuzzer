NAME = concurrent-fuzzer
CXXFLAGS = -Wall -Wno-unused-command-line-argument -O3 -flto -std=c++11 -g

OBJ_DIR = obj
SRC_DIR = src

SRCS =	main.cpp
OBJS = $(addprefix $(OBJ_DIR)/, $(SRCS:.cpp=.o))

# local duckdb build
DUCKDB_DIR=/$(HOME)/git/duckdb121

INC =-I$(DUCKDB_DIR)/src/include
DUCKDBLIB=$(DUCKDB_DIR)/build/release/src/libduckdb_static.a

DUCKDB_DEPS=$(DUCKDB_DIR)/build/release/third_party/fmt/libduckdb_fmt.a \
	$(DUCKDB_DIR)/build/release/third_party/libpg_query/libduckdb_pg_query.a \
	$(DUCKDB_DIR)/build/release/third_party/re2/libduckdb_re2.a \
	$(DUCKDB_DIR)/build/release/third_party/miniz/libduckdb_miniz.a \
	$(DUCKDB_DIR)/build/release/third_party/utf8proc/libduckdb_utf8proc.a \
	$(DUCKDB_DIR)/build/release/third_party/hyperloglog/libduckdb_hyperloglog.a \
	$(DUCKDB_DIR)/build/release/third_party/skiplist/libduckdb_skiplistlib.a \
	$(DUCKDB_DIR)/build/release/third_party/fastpforlib/libduckdb_fastpforlib.a \
	$(DUCKDB_DIR)/build/release/third_party/mbedtls/libduckdb_mbedtls.a \
	$(DUCKDB_DIR)/build/release/third_party/fsst/libduckdb_fsst.a \
	$(DUCKDB_DIR)/build/release/third_party/yyjson/libduckdb_yyjson.a \
	$(DUCKDB_DIR)/build/release/third_party/zstd/libduckdb_zstd.a

DUCKDB_EXT= $(DUCKDB_DIR)/build/release/extension/core_functions/libcore_functions_extension.a \
	$(DUCKDB_DIR)/build/release/extension/parquet/libparquet_extension.a \
	$(DUCKDB_DIR)/build/release/extension/json/libjson_extension.a

all: $(NAME)

$(NAME): $(OBJS)
	@$(CXX) $(CXXFLAGS) $^ $(INC) $(DUCKDBLIB) $(DUCKDB_EXT) $(DUCKDB_DEPS) -o $(NAME)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	@$(CXX) -c $(CXXFLAGS) $< $(INC) $(DUCKDBLIB) $(DUCKDB_EXT) $(DUCKDB_DEPS) -o $@

clean:
	rm -f $(NAME)
	rm -rf $(OBJ_DIR)

re: clean all

.PHONY: all clean re
