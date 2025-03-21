#include "../global.h"
#include <algorithm>

/**
 * @brief
 * SYNTAX:
 *   <newTable> <- ORDER BY <columnName> ASC|DESC ON <existingTable>
 *
 * We read all rows from <existingTable>, sort them by <columnName>
 * in ascending or descending order, then write them into <newTable>.
 */

//-------------------------
// Syntactic parse is in syntacticParser.cpp
// Semantic parse is in semanticParser.cpp
// So here we only do `executeORDERBY()`
//-------------------------

void executeORDERBY()
{
	logger.log("executeORDERBY");

	// 1) Grab input table
	Table sourceTable = *(tableCatalogue.getTable(parsedQuery.orderByRelationName));

	// 2) Create a new table with same columns, but different name
	Table *resultTable =
		new Table(parsedQuery.orderByResultRelationName, sourceTable.columns);

	// 3) Read all rows from source
	Cursor cursor = sourceTable.getCursor();
	vector<int> row = cursor.getNext();
	vector<vector<int>> allRows;
	while (!row.empty())
	{
		allRows.push_back(row);
		row = cursor.getNext();
	}

	// 4) Figure out which column to sort on
	int colIndex = sourceTable.getColumnIndex(parsedQuery.orderByColumnName);

	// 5) Sort ascending or descending
	if (parsedQuery.orderBySortingStrategy == ASC)
	{
		sort(allRows.begin(), allRows.end(),
			 [colIndex](auto &A, auto &B)
			 {
				 return A[colIndex] < B[colIndex];
			 });
	}
	else
	{
		// DESC
		sort(allRows.begin(), allRows.end(),
			 [colIndex](auto &A, auto &B)
			 {
				 return A[colIndex] > B[colIndex];
			 });
	}

	// 6) Write rows into result table’s CSV
	for (auto &sortedRow : allRows)
		resultTable->writeRow<int>(sortedRow);

	// 7) Blockify the new table (break it into pages)
	resultTable->blockify();

	// 8) Insert into catalogue
	tableCatalogue.insertTable(resultTable);
}

bool syntacticParseORDERBY()
{
	logger.log("syntacticParseORDERBY");

	// 8 tokens:
	// 0: <newTableName>
	// 1: "<-"
	// 2: "ORDER"
	// 3: "BY"
	// 4: <columnName>
	// 5: "ASC" or "DESC"
	// 6: "ON"
	// 7: <oldTableName>
	if (tokenizedQuery.size() != 8)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	// Fill up parsedQuery
	parsedQuery.queryType = ORDERBY;
	parsedQuery.orderByResultRelationName = tokenizedQuery[0];
	// token[1] == "<-" (we assume that’s verified above)

	// token[2] == "ORDER", token[3] == "BY"
	parsedQuery.orderByColumnName = tokenizedQuery[4];

	// token[5] == "ASC" or "DESC"
	string sortDirection = tokenizedQuery[5];
	if (sortDirection == "ASC")
		parsedQuery.orderBySortingStrategy = ASC;
	else if (sortDirection == "DESC")
		parsedQuery.orderBySortingStrategy = DESC;
	else
	{
		cout << "SYNTAX ERROR: expected ASC or DESC" << endl;
		return false;
	}

	// token[6] == "ON"
	parsedQuery.orderByRelationName = tokenizedQuery[7];

	return true;
}

bool semanticParseORDERBY()
{
	logger.log("semanticParseORDERBY");

	// 1) The result table must NOT already exist
	if (tableCatalogue.isTable(parsedQuery.orderByResultRelationName))
	{
		cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
		return false;
	}

	// 2) The input table must exist
	if (!tableCatalogue.isTable(parsedQuery.orderByRelationName))
	{
		cout << "SEMANTIC ERROR: Input relation does not exist" << endl;
		return false;
	}

	// 3) The column must exist in that table
	if (!tableCatalogue.isColumnFromTable(parsedQuery.orderByColumnName,
										  parsedQuery.orderByRelationName))
	{
		cout << "SEMANTIC ERROR: Column doesn't exist in given relation" << endl;
		return false;
	}

	return true;
}
