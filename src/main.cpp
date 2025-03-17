#include "main.hpp"
#include "duckdb.hpp"

#include <iostream>

using namespace duckdb;

int main() {

	DuckDB db(nullptr);
	Connection con(db);
	std::string query = "SELECT 42 as my_col; SELECT 43 as my_other_col;";

	unique_ptr<MaterializedQueryResult> q_result = con.Query(query);

	// query 1
	std::cout << q_result->names[0] << std::endl;
	std::cout << q_result->GetValue(0, 0) << std::endl;
	std::cout << q_result->types[0].ToString() << std::endl;

	// query 2
	auto q2 = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(q_result->next));
	std::cout << q2->names[0] << std::endl;
	std::cout << q2->GetValue(0, 0) << std::endl;
	std::cout << q2->types[0].ToString() << std::endl;
}
