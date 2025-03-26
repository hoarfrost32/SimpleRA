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

	// Get source table
	Table *sourceTable = tableCatalogue.getTable(parsedQuery.orderByRelationName);

	// Create unique temp table name
	std::string tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName;
	int counter = 0;
	while (tableCatalogue.isTable(tempTableName))
	{
		tempTableName = "_ORDERBY_TEMP_" + parsedQuery.orderByRelationName + "_" + std::to_string(++counter);
	}

	// Build temp table with same columns as source
	Table *tempTable = new Table(tempTableName, sourceTable->columns);

	// Copy rows from source to temp
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

	// Insert temp table into catalogue for executeSORT()
	tableCatalogue.insertTable(tempTable);

	// Set up parsedQuery for executeSORT() call
	ParsedQuery oldQuery = parsedQuery; // stash current parse info

	// Overwrite parse info so that "executeSORT()" thinks user typed:
	//   SORT <tempTable> BY <theColumn> IN <ASC|DESC>
	parsedQuery.queryType = SORT;
	parsedQuery.sortRelationName = tempTableName;
	parsedQuery.sortColumns.clear();
	{
		// Convert ASC/DESC enum to string
		std::string dir = (oldQuery.orderBySortingStrategy == ASC) ? "ASC" : "DESC";
		parsedQuery.sortColumns.emplace_back(oldQuery.orderByColumnName, dir);
	}

	// Sort the temp table
	executeSORT();

	// Restore original query state
	parsedQuery = oldQuery;

	// Create final table with same schema
	Table *resultTable = new Table(parsedQuery.orderByResultRelationName, sourceTable->columns);

	// Copy sorted rows to final table
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

	// Add final table to catalogue
	tableCatalogue.insertTable(resultTable);

	// Cleanup temp table
	tableCatalogue.deleteTable(tempTableName);

	cout << "ORDER BY on table \"" << parsedQuery.orderByRelationName << "\" complete.\n"
		 << "New table \"" << parsedQuery.orderByResultRelationName
		 << "\" is sorted by column \"" << parsedQuery.orderByColumnName << "\"."
		 << endl;
}

bool syntacticParseORDERBY()
{
	logger.log("syntacticParseORDERBY");

	// Parse ORDER BY syntax: <newTable> <- ORDER BY <columnName> ASC|DESC ON <oldTable>
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

	// Verify result table doesn't exist
	if (tableCatalogue.isTable(parsedQuery.orderByResultRelationName))
	{
		cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
		return false;
	}

	// Verify input table exists
	if (!tableCatalogue.isTable(parsedQuery.orderByRelationName))
	{
		cout << "SEMANTIC ERROR: Input relation does not exist" << endl;
		return false;
	}

	// Verify column exists in input table
	if (!tableCatalogue.isColumnFromTable(parsedQuery.orderByColumnName,
										  parsedQuery.orderByRelationName))
	{
		cout << "SEMANTIC ERROR: Column doesn't exist in given relation" << endl;
		return false;
	}

	return true;
}
