#include "../global.h"
#include <algorithm>
#include <string>
#include <sstream>
#include <fstream>

/**
 * @brief
 * SYNTAX:
 *   <newTable> <- ORDER BY <columnName> ASC|DESC ON <existingTable>
 *
 * We read all rows from <existingTable>, sort them by <columnName>
 * in ascending or descending order, then write them into <newTable>.
 */

extern void executeSORT();

void executeORDERBY()
{
	logger.log("executeORDERBY");

	// 1) Source table
	Table *sourceTable = tableCatalogue.getTable(parsedQuery.orderByRelationName);

	// 2) Make sure the new table doesn't already exist (semanticParseORDERBY checks this)
	//    We'll build it at the end (after sorting).

	// 3) Create a temporary table name. Something guaranteed unique:
	std::string tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName;
	int counter = 0;
	while (tableCatalogue.isTable(tempTableName))
	{
		// in case of collisions
		tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName + "_" + std::to_string(++counter);
	}

	// 4) Build a new Table object with the same columns, under the temporary name
	Table *tempTable = new Table(tempTableName, sourceTable->columns);

	// 5) Copy all rows from sourceTable into tempTable (just like how CROSS or PROJECTION might do)
	{
		Cursor cursor = sourceTable->getCursor();
		vector<int> row = cursor.getNext();
		while (!row.empty())
		{
			tempTable->writeRow<int>(row);
			row = cursor.getNext();
		}
		// Now blockify so the temp table is physically laid out in pages
		tempTable->blockify();
	}

	// 6) Insert this temp table into the catalogue so that "executeSORT()" can find it
	tableCatalogue.insertTable(tempTable);

	// 7) We want to sort by "parsedQuery.orderByColumnName" in either ASC or DESC.
	//    But our existing external‐sort logic uses parsedQuery.sortColumns (vector of (colName, "ASC"/"DESC")).
	//    So temporarily stash and restore any prior sort info, and set the fields as if user typed "SORT tempTable ..."

	// Save old queryType / data
	auto oldQuery = parsedQuery;

	// Overwrite just enough to call executeSORT()
	parsedQuery.queryType = SORT;
	parsedQuery.sortRelationName = tempTableName; // table to sort in place
	parsedQuery.sortColumns.clear();			  // e.g. [("colName","ASC")]
	{
		// Convert enum ASC/DESC to string "ASC" or "DESC"
		std::string dir = (oldQuery.orderBySortingStrategy == ASC) ? "ASC" : "DESC";
		parsedQuery.sortColumns.push_back({oldQuery.orderByColumnName, dir});
	}

	// 8) Call the external mergesort logic
	executeSORT();

	// (At this point, the temp table named 'tempTableName' is sorted on the desired column.)

	// Restore the old parsedQuery
	parsedQuery = oldQuery;

	// 9) Now read from the sorted temp table, and write into the final "result" table
	Table *resultTable = new Table(parsedQuery.orderByResultRelationName,
								   sourceTable->columns);

	{
		Cursor sortedCursor(tempTableName, 0);
		vector<int> sortedRow = sortedCursor.getNext();
		while (!sortedRow.empty())
		{
			resultTable->writeRow<int>(sortedRow);
			sortedRow = sortedCursor.getNext();
		}
		resultTable->blockify();
	}

	// 10) Insert the final table into the catalogue
	tableCatalogue.insertTable(resultTable);

	// 11) Clean up the temp table
	tableCatalogue.deleteTable(tempTableName);

	cout << "ORDER BY on table \"" << parsedQuery.orderByRelationName << "\" complete."
		 << "\nNew table \"" << parsedQuery.orderByResultRelationName << "\" created, sorted by "
		 << parsedQuery.orderByColumnName << "." << endl;
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
