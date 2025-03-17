#include "main.hpp"
#include "duckdb.hpp"

#include <iostream>

int main() {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	std::string query = "SELECT 42;";
	duckdb::unique_ptr<duckdb::MaterializedQueryResult> q_result = con.Query(query);

	std::cout << q_result->ToString() << std::endl;
}
