#include "../global.h"
#include "../table.h"      // Include table definition (now presumably supports multiple indexes)
#include "../index.h"      // Include index definition
#include <regex>
#include <limits> // Required for INT_MIN, INT_MAX
#include <vector>
#include <algorithm> // For sort, unique (if needed)
#include <unordered_map> // Assuming Table now uses this for indexes

/**
 * @brief Executes the SEARCH command with multi-index support.
 *
 * SYNTAX: R <- SEARCH FROM T WHERE col bin_op literal
 *
 * Selects rows from T where the condition (col bin_op literal) is met.
 * - Always attempts to use or create an index for the specific 'col' for
 *   operators ==, <, >, <=, >=, !=.
 * - If an index exists on 'col', uses it.
 * - If an index does NOT exist on 'col', implicitly creates a BTREE index for 'col' and uses it.
 * - If implicit index creation fails, the search aborts.
 * - Table scan is NOT used.
 * Stores the result in table R.
 */

 bool syntacticParseSEARCH() {
	logger.log("syntacticParseSEARCH");
	// Expected Syntax: res_table <- SEARCH FROM table_name WHERE col bin_op int_literal
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

	return true;
}

// Forward declaration for executeINDEX if it's not in executor.h
extern void executeINDEX();

void executeSEARCH()
{
    logger.log("executeSEARCH");

    Table *sourceTable = tableCatalogue.getTable(parsedQuery.searchRelationName);
    Table *resultTable = new Table(parsedQuery.searchResultRelationName, sourceTable->columns);

    int searchColIndex = sourceTable->getColumnIndex(parsedQuery.searchColumnName); // Should be valid due to semantic check
    bool useIndex = false;
    bool indexImplicitlyCreated = false;
    BTree* indexToUse = nullptr; // Pointer to the specific index we'll use

    // --- Determine if index exists or needs creation ---

    // Check if an index *specifically for this column* exists
    // ASSUMPTION: Table class now has a way to check/get index for a column, e.g., getIndex(columnName)
    indexToUse = sourceTable->getIndex(parsedQuery.searchColumnName); // Assumes getIndex returns nullptr if not found

    if (indexToUse != nullptr) {
        logger.log("executeSEARCH: Found existing index for column '" + parsedQuery.searchColumnName + "'. Planning to use it.");
        useIndex = true;
    } else {
        // Index doesn't exist for this column, create it implicitly
        logger.log("executeSEARCH: Index not found for column '" + parsedQuery.searchColumnName + "'. Implicitly creating B+ Tree index...");
        indexImplicitlyCreated = true;

        // Temporarily modify parsedQuery to call executeINDEX
        ParsedQuery currentQuery = parsedQuery; // Save current state
        parsedQuery.queryType = INDEX;
        parsedQuery.indexRelationName = currentQuery.searchRelationName;
        parsedQuery.indexColumnName = currentQuery.searchColumnName; // Create for the specific column
        parsedQuery.indexingStrategy = BTREE;

        // Execute the index creation
        executeINDEX(); // This should modify the table object, adding the index to its collection

        printf("Done creating index. Now on to SEARCH\n");
        
        // Restore original query type
        parsedQuery.queryType = SEARCH;
        
        printf("Restored original query type \n");

        // Refresh the table pointer and try to get the newly created index
        sourceTable = tableCatalogue.getTable(currentQuery.searchRelationName); // Refresh table pointer
        indexToUse = sourceTable->getIndex(currentQuery.searchColumnName); // Try getting the index again

        // cout << "Index to use: " << indexToUse << endl;
        
        // Check if index creation was successful
        if (indexToUse != nullptr)
        {
            logger.log("executeSEARCH: Successfully created index for column '" + currentQuery.searchColumnName + "'. Now planning to use it.");
            useIndex = true;
        }
        else
        {
            logger.log("executeSEARCH: ERROR - Failed to create or retrieve implicitly created index for column '" + currentQuery.searchColumnName + "'. Aborting search operation.");
            useIndex = false; // Ensure we don't proceed
        }
    }

    // --- Perform Index Search (if index is available) ---

    if (useIndex && indexToUse != nullptr) {
        vector<RecordPointer> pointers;
        int searchLiteral = parsedQuery.searchLiteralValue;

        // Use the appropriate index search method based on the operator
        switch (parsedQuery.searchOperator) {
            case EQUAL:
                logger.log("executeSEARCH: Using index->searchKey(" + std::to_string(searchLiteral) + ")");
                pointers = indexToUse->searchKey(searchLiteral);
                break;
            case LESS_THAN:
                 logger.log("executeSEARCH: Using index->searchRange(MIN, " + std::to_string(searchLiteral - 1) + ")");
                 pointers = indexToUse->searchRange(std::numeric_limits<int>::min(), (searchLiteral == std::numeric_limits<int>::min()) ? std::numeric_limits<int>::min() : searchLiteral - 1);
                 if (searchLiteral == std::numeric_limits<int>::min()) pointers.clear();
                break;
            case GREATER_THAN:
                 logger.log("executeSEARCH: Using index->searchRange(" + std::to_string(searchLiteral + 1) + ", MAX)");
                 pointers = indexToUse->searchRange((searchLiteral == std::numeric_limits<int>::max()) ? std::numeric_limits<int>::max() : searchLiteral + 1, std::numeric_limits<int>::max());
                 if (searchLiteral == std::numeric_limits<int>::max()) pointers.clear();
                break;
            case LEQ:
                logger.log("executeSEARCH: Using index->searchRange(MIN, " + std::to_string(searchLiteral) + ")");
                pointers = indexToUse->searchRange(std::numeric_limits<int>::min(), searchLiteral);
                break;
            case GEQ:
                 logger.log("executeSEARCH: Using index->searchRange(" + std::to_string(searchLiteral) + ", MAX)");
                pointers = indexToUse->searchRange(searchLiteral, std::numeric_limits<int>::max());
                break;
            case NOT_EQUAL:
                {
                    logger.log("executeSEARCH: Using index for != by combining two range scans.");
                    vector<RecordPointer> pointers_less;
                    vector<RecordPointer> pointers_greater;

                    pointers_less = indexToUse->searchRange(std::numeric_limits<int>::min(), (searchLiteral == std::numeric_limits<int>::min()) ? std::numeric_limits<int>::min() : searchLiteral - 1);
                    if (searchLiteral == std::numeric_limits<int>::min()) pointers_less.clear();

                    pointers_greater = indexToUse->searchRange((searchLiteral == std::numeric_limits<int>::max()) ? std::numeric_limits<int>::max() : searchLiteral + 1, std::numeric_limits<int>::max());
                     if (searchLiteral == std::numeric_limits<int>::max()) pointers_greater.clear();

                    pointers = pointers_less;
                    pointers.insert(pointers.end(), pointers_greater.begin(), pointers_greater.end());
                     logger.log("executeSEARCH: Combined " + std::to_string(pointers_less.size()) + " (<) and " + std::to_string(pointers_greater.size()) + " (>) pointers for != operator.");
                }
                break;
            default:
                logger.log("executeSEARCH: Error - Unknown operator in index search switch. Aborting.");
                 useIndex = false; // Should not happen
                break;
        }

        // --- Fetch rows using the pointers obtained from the index ---
        if(useIndex) { // Check again in case default case was hit
            long long rowsAdded = 0;
            for (const auto& ptr : pointers)
            {
                // Basic validation of the pointer
                 if (ptr.first < 0 || ptr.first >= sourceTable->blockCount || ptr.second < 0 ) {
                    logger.log("executeSEARCH: Warning - Index returned an invalid pointer: {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + "}. Skipping.");
                    continue;
                }
                // Check if rowIndex is within the bounds for that specific page
                if (ptr.first >= sourceTable->rowsPerBlockCount.size() || ptr.second >= sourceTable->rowsPerBlockCount[ptr.first]) {
                    logger.log("executeSEARCH: Warning - Index returned pointer with row index out of bounds for page " + std::to_string(ptr.first) + ": {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + ", rowsInPage=" + (ptr.first < sourceTable->rowsPerBlockCount.size() ? std::to_string(sourceTable->rowsPerBlockCount[ptr.first]) : "N/A") + "}. Skipping.");
                    continue;
                }

                // Fetch the page containing the row
                Page page = bufferManager.getPage(sourceTable->tableName, ptr.first);
                vector<int> row = page.getRow(ptr.second);

                if (!row.empty())
                {
                    resultTable->writeRow<int>(row);
                    rowsAdded++;
                }
                else
                {
                    logger.log("executeSEARCH: Warning - Index pointer {page=" + std::to_string(ptr.first) + ", row=" + std::to_string(ptr.second) + "} pointed to an empty row within the page file. Skipping.");
                }
            }
            // Provide user feedback
            cout << (indexImplicitlyCreated ? "Implicit index created for '" + parsedQuery.searchColumnName + "'. " : "") << "Index search used. ";
            cout << "Found " << pointers.size() << " pointer(s), added " << rowsAdded << " row(s) to result." << endl;
        }
    } else { // Index not used (likely because implicit creation failed)
         logger.log("executeSEARCH: No search performed as index could not be used or created. Result table will be empty.");
    }


    // --- Finalize the result table ---
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
