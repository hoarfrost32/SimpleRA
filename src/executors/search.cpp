#include "../global.h"
#include "../table.h"      // Include table definition
#include "../index.h"      // Include index definition (Assuming this exists as per guide.md)
#include <regex>

/**
 * @brief Executes the SEARCH command.
 *
 * SYNTAX: R <- SEARCH FROM T WHERE col bin_op literal
 *
 * Selects rows from T where the condition (col bin_op literal) is met.
 * If T is indexed on 'col' and bin_op is '==', uses the index.
 * Otherwise, performs a full table scan.
 * Stores the result in table R.
 */

 bool syntacticParseSEARCH() {
	logger.log("syntacticParseSEARCH");
	// Expected Syntax: res_table <- SEARCH FROM table_name WHERE col bin_op int_literal
	// Token indices:      0       1    2     3       4       5    6     7      8
	if (tokenizedQuery.size() != 9 || tokenizedQuery[3] != "FROM" ||
		tokenizedQuery[5] != "WHERE") {
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
 
	parsedQuery.queryType = SEARCH;
	parsedQuery.searchResultRelationName = tokenizedQuery[0];
	parsedQuery.searchRelationName = tokenizedQuery[4];
	parsedQuery.searchColumnName = tokenizedQuery[6];
 
	// Parse binary operator
	string binaryOperator = tokenizedQuery[7];
	if (binaryOperator == "<")
		parsedQuery.searchOperator = LESS_THAN;
	else if (binaryOperator == ">")
		parsedQuery.searchOperator = GREATER_THAN;
	else if (binaryOperator == ">=" || binaryOperator == "=>")
		parsedQuery.searchOperator = GEQ;
	else if (binaryOperator == "<=" || binaryOperator == "=<")
		parsedQuery.searchOperator = LEQ;
	else if (binaryOperator == "==")
		parsedQuery.searchOperator = EQUAL;
	else if (binaryOperator == "!=")
		parsedQuery.searchOperator = NOT_EQUAL;
	else {
		cout << "SYNTAX ERROR: Invalid binary operator" << endl;
		return false;
	}
	
	// Parse integer literal
	regex numeric("[-]?[0-9]+");
	string literalValue = tokenizedQuery[8];
	if (!regex_match(literalValue, numeric)) {
		cout << "SYNTAX ERROR: Condition requires an integer literal" << endl;
		return false;
	}
	parsedQuery.searchLiteralValue = stoi(literalValue);

	return true;
}

bool semanticParseSEARCH()
{
	logger.log("semanticParseSEARCH");

	// Result table must not exist
	if (tableCatalogue.isTable(parsedQuery.searchResultRelationName)) {
		cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
		return false;
	}

	// Source table must exist
	if (!tableCatalogue.isTable(parsedQuery.searchRelationName)) {
		cout << "SEMANTIC ERROR: Source relation doesn't exist" << endl;
		return false;
	}

	// Search column must exist in the source table
	if (!tableCatalogue.isColumnFromTable(parsedQuery.searchColumnName, parsedQuery.searchRelationName)) {
		cout << "SEMANTIC ERROR: Column '" << parsedQuery.searchColumnName << "' doesn't exist in relation '" << parsedQuery.searchRelationName << "'" << endl;
		return false;
	}

	// Potentially add checks here if the index type (BTREE/HASH) restricts operators,
	// but for now, we only check for '==' in the executor logic.

	return true;
}
 
void executeSEARCH()
{
    logger.log("executeSEARCH");

    Table *sourceTable = tableCatalogue.getTable(parsedQuery.searchRelationName);
    Table *resultTable = new Table(parsedQuery.searchResultRelationName, sourceTable->columns);

    int searchColIndex = sourceTable->getColumnIndex(parsedQuery.searchColumnName);
    // bool indexUsed = false;

    // Check conditions for using the index (as per guide.md)
    // ASSUMPTION: Person A provides an `Index` class instance attached to the Table,
    //             and `Index::search` returns vector<RecordPointer>.
    // ASSUMPTION: Table class has `Index* index` member. Initialize to nullptr.
    // ASSUMPTION: RecordPointer struct { int pageIndex; int rowIndex; }; exists.
    // if (sourceTable->indexed &&
    //     sourceTable->indexedColumn == parsedQuery.searchColumnName &&
    //     parsedQuery.searchOperator == EQUAL &&
    //     sourceTable->index != nullptr) // Check if index object actually exists
    // {
    //     logger.log("executeSEARCH: Using index.");
    //     indexUsed = true;

    //     // Use the index to find matching RecordPointers
    //     vector<RecordPointer> pointers = sourceTable->index->search(parsedQuery.searchLiteralValue);

    //     // Fetch rows corresponding to the pointers
    //     for (const auto& ptr : pointers)
    //     {
    //         // Fetch the page containing the row
    //         // Note: Getting page directly. A helper table->getRow(ptr) might be cleaner.
    //         Page page = bufferManager.getPage(sourceTable->tableName, ptr.pageIndex);
    //         vector<int> row = page.getRow(ptr.rowIndex);
    //         if (!row.empty())
    //         {
    //             resultTable->writeRow<int>(row);
    //         }
    //         else
    //         {
    //              logger.log("executeSEARCH: Warning - Index pointer pointed to an empty/invalid row.");
    //         }
    //     }
    //      cout << "Index search used. Found " << pointers.size() << " matching rows." << endl;
    // }
    // 
    // 
    // 
    bool indexUsed = false;

    // Check conditions for using the index (as per guide.md)
    // Now UNCOMMENTED and assumed to work with Person A's BTree/Index implementation.
    if (sourceTable->indexed &&
        sourceTable->indexedColumn == parsedQuery.searchColumnName &&
        parsedQuery.searchOperator == EQUAL &&
        sourceTable->index != nullptr) // Check if index object actually exists
    {
        logger.log("executeSEARCH: Using index on column '" + sourceTable->indexedColumn + "' for key " + std::to_string(parsedQuery.searchLiteralValue));
        indexUsed = true;

        // Use the index to find matching RecordPointers
        // ASSUMPTION: searchKey exists and returns vector<RecordPointer>
        vector<RecordPointer> pointers = sourceTable->index->searchKey(parsedQuery.searchLiteralValue);

        // Fetch rows corresponding to the pointers
        long long rowsAdded = 0;
        for (const auto& ptr : pointers)
        {
            // Basic validation of the pointer
            if (ptr.first < 0 || ptr.first >= sourceTable->blockCount || ptr.second < 0 ) {
                 logger.log("executeSEARCH: Warning - Index returned an invalid pointer: {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + "}. Skipping.");
                 continue;
            }
             // Check if rowIndex is within the bounds for that specific page
            if (ptr.second >= sourceTable->rowsPerBlockCount[ptr.first]) {
                 logger.log("executeSEARCH: Warning - Index returned pointer with row index out of bounds for page " + std::to_string(ptr.first) + ": {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + ", rowsInPage=" + std::to_string(sourceTable->rowsPerBlockCount[ptr.first]) + "}. Skipping.");
                 continue;
            }


            // Fetch the page containing the row
            Page page = bufferManager.getPage(sourceTable->tableName, ptr.first);
            vector<int> row = page.getRow(ptr.second); // 0-based index

            if (!row.empty())
            {
                resultTable->writeRow<int>(row);
                rowsAdded++;
            }
            else
            {
                 // This might happen if the row was deleted but index wasn't updated, or other inconsistency.
                 logger.log("executeSEARCH: Warning - Index pointer {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + "} pointed to an empty row within the page file. Skipping.");
            }
        }
        // Provide user feedback
        cout << "Index search used. Found " << pointers.size() << " pointer(s), added " << rowsAdded << " row(s) to result." << endl;
    }
    else
    {
        // Fallback to table scan
        logger.log("executeSEARCH: Using table scan (index not applicable or not found).");
        Cursor cursor = sourceTable->getCursor();
        vector<int> row = cursor.getNext();
        long long rowsFound = 0;

        while (!row.empty())
        {
            bool conditionMet = evaluateBinOp(row[searchColIndex],
                                              parsedQuery.searchLiteralValue,
                                              parsedQuery.searchOperator);

            if (conditionMet)
            {
                resultTable->writeRow<int>(row);
                rowsFound++;
            }
            row = cursor.getNext();
        }
         cout << "Table scan used. Found " << rowsFound << " matching rows." << endl;
    }

    // Finalize the result table
    if (resultTable->blockify()) {
        tableCatalogue.insertTable(resultTable);
        cout << "SEARCH successful. Result stored in table: " << resultTable->tableName << endl;
    }
    else {
        // No rows matched, or an error occurred during blockify
        cout << "SEARCH completed. No matching rows found or result table is empty." << endl;
        resultTable->unload(); // Clean up the empty temp file
        delete resultTable;
    }
}