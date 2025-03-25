#include "../global.h"
#include <string>
#include <fstream>

/**
 * @brief
 * SYNTAX:
 *   <newTable> <- ORDER BY <columnName> ASC|DESC ON <existingTable>
 *
 * We read all rows from <existingTable>, sort them by <columnName>
 * in ascending or descending order, then write them into <newTable>.
 */

extern void executeSORT(); // We'll re-use the code in sort.cpp

void executeORDERBY()
{
	logger.log("executeORDERBY");

	// 1) Get the source table
	Table *sourceTable = tableCatalogue.getTable(parsedQuery.orderByRelationName);

	// 2) Create a unique temporary table name
	std::string tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName;
	int counter = 0;
	while (tableCatalogue.isTable(tempTableName))
	{
		tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName + "_" + std::to_string(++counter);
	}

	// 3) Build the temp table with the same columns as source
	Table *tempTable = new Table(tempTableName, sourceTable->columns);

	// 4) Copy rows from source into temp, row by row (so we never store them all in memory)
	{
		Cursor srcCursor = sourceTable->getCursor();
		vector<int> row = srcCursor.getNext();
		while (!row.empty())
		{
			tempTable->writeRow<int>(row); // appends row to CSV
			row = srcCursor.getNext();
		}
		// "blockify()" will read the CSV data for tempTable in chunks of "maxRowsPerBlock"
		// and write them into pages. That uses far less than 10 blocks in memory at once.
		tempTable->blockify();
	}

	// 5) Insert the temp table into the catalogue so "executeSORT()" can find it
	tableCatalogue.insertTable(tempTable);

	// 6) We want to sort the temp table by the chosen column & direction (ASC|DESC).
	//    Our external mergesort code in `executeSORT()` uses fields in `parsedQuery`
	//    named .sortRelationName and .sortColumns. So we must temporarily set them up.
	ParsedQuery oldQuery = parsedQuery; // stash current parse info

	// Overwrite parse info so that "executeSORT()" thinks user typed:
	//   SORT <tempTable> BY <theColumn> IN <ASC|DESC>
	parsedQuery.queryType = SORT;
	parsedQuery.sortRelationName = tempTableName;
	parsedQuery.sortColumns.clear();
	{
		// Convert enum ASC/DESC to the correct string
		std::string dir = (oldQuery.orderBySortingStrategy == ASC) ? "ASC" : "DESC";
		parsedQuery.sortColumns.emplace_back(oldQuery.orderByColumnName, dir);
	}

	// 7) Perform external mergesort on the temp table
	executeSORT();

	// (At this point, tempTable is fully sorted on the desired column.)

	// Restore the old query
	parsedQuery = oldQuery;

	// 8) Create the final table with the same schema
	Table *resultTable = new Table(parsedQuery.orderByResultRelationName, sourceTable->columns);

	// 9) Read sorted rows from the temp table *row by row*,
	//    and write them into the final table
	{
		Cursor sortedCursor(tempTableName, 0);
		vector<int> sortedRow = sortedCursor.getNext();
		while (!sortedRow.empty())
		{
			resultTable->writeRow<int>(sortedRow);
			sortedRow = sortedCursor.getNext();
		}
		// Then physically layout the new table
		resultTable->blockify();
	}

	// 10) Insert the final result table into the catalogue
	tableCatalogue.insertTable(resultTable);

	// 11) Remove the temp table from the system
	tableCatalogue.deleteTable(tempTableName);

	cout << "ORDER BY on table \"" << parsedQuery.orderByRelationName << "\" complete.\n"
		 << "New table \"" << parsedQuery.orderByResultRelationName
		 << "\" is sorted by column \"" << parsedQuery.orderByColumnName << "\"."
		 << endl;
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
