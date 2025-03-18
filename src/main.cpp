#include "duckdb.hpp"

#include <iostream>

using namespace duckdb;

unique_ptr<DuckDB> create_db_file(std::string db_file_name) {
	return unique_ptr<DuckDB>(new DuckDB(db_file_name));
}

bool run_query(Connection &con, std::string query_str, bool print_query_result = false) {
	ErrorData error;
	unique_ptr<DataChunk> data_result;
	auto query_result = con.Query(query_str);
	if (!query_result->TryFetch(data_result, error)) {
		std::cout << error.Message() << std::endl;
		return false;
	}
	if (print_query_result) {
		std::cout << query_result->ToString() << std::endl;
		// loop over consecutive sql statments
		auto next_query_result = std::move(query_result->next);
		while (next_query_result != nullptr) {
			std::cout << next_query_result->ToString() << std::endl;
			next_query_result = std::move(next_query_result->next);
		}
	}
	return true;
}

bool create_table(Connection &con, std::string table_name) {
	std::string query_str = "CREATE TABLE " + table_name + " (c1 VARCHAR, c2 VARCHAR);";
	return run_query(con, query_str);
}

int main() {
	DuckDB in_memory_db(nullptr);
	Connection in_memory_con(in_memory_db);

	create_table(in_memory_con, "t1");
	create_table(in_memory_con, "t2");
	run_query(in_memory_con, "show tables;", true);
	run_query(in_memory_con, "SELECT 42 as my_col; SELECT 43 as my_other_col; SELECT 44 as col_44;", true);

	auto db1 = create_db_file("db1.duckdb");
	Connection db1_con(*db1);
	create_table(db1_con, "t1");
	create_table(db1_con, "t2");
	run_query(db1_con, "show tables;", true);
	std::remove("db1.duckdb"); // delete file

	auto db2 = create_db_file("db2.duckdb");
	Connection db2_con(*db2);
	create_table(db2_con, "t1");
	create_table(db2_con, "t2");
	run_query(db2_con, "show tables;", true);
	std::remove("db2.duckdb"); // delete file
}
