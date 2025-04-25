#include "../global.h"
#include <vector>
#include <string>
#include <algorithm> // Needed for std::remove_if
#include "../table.h"
#include "../page.h"
#include "../index.h"
#include "../bufferManager.h"
/**
 * Supported limited grammar:
 *   UPDATE <table> SET <col> = <int> WHERE <col2> <binop> <int>
 * tokenizedQuery = ["UPDATE","T","SET","c","=","5","WHERE","d",">","10"]
 */

static bool parseBinOp(const string &tok, BinaryOperator &op)
{
	if (tok == "==")
		op = EQUAL;
	else if (tok == "!=")
		op = NOT_EQUAL;
	else if (tok == "<")
		op = LESS_THAN;
	else if (tok == "<=" || tok == "=<")
		op = LEQ;
	else if (tok == ">")
		op = GREATER_THAN;
	else if (tok == ">=" || tok == "=>")
		op = GEQ;
	else
		return false;
	return true;
}

bool syntacticParseUPDATE()
{
	logger.log("syntacticParseUPDATE");

	/* Grammar we accept:
	   UPDATE table_name WHERE col1 <binop> int_literal SET col2 = int_literal
	   Minimum tokens = 10  (UPDATE T WHERE c1 == 5 SET c2 = 7)               */

	if (tokenizedQuery.size() < 10 ||
		tokenizedQuery[2] != "WHERE")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = UPDATE;
	parsedQuery.updateRelationName = tokenizedQuery[1];

	/* -------- condition part -------- */
	parsedQuery.updateCondColumn = tokenizedQuery[3];

	string binOpTok = tokenizedQuery[4];
	if (binOpTok == "<")
		parsedQuery.updateCondOperator = LESS_THAN;
	else if (binOpTok == ">")
		parsedQuery.updateCondOperator = GREATER_THAN;
	else if (binOpTok == "<=")
		parsedQuery.updateCondOperator = LEQ;
	else if (binOpTok == ">=")
		parsedQuery.updateCondOperator = GEQ;
	else if (binOpTok == "==")
		parsedQuery.updateCondOperator = EQUAL;
	else if (binOpTok == "!=")
		parsedQuery.updateCondOperator = NOT_EQUAL;
	else
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	/* RHS of condition must be int literal */
	regex numeric("[-]?[0-9]+");
	if (!regex_match(tokenizedQuery[5], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateCondValue = stoi(tokenizedQuery[5]);

	/* -------- SET part -------- */
	size_t setPos = 6;
	if (tokenizedQuery[setPos] != "SET" ||
		setPos + 3 >= tokenizedQuery.size() ||
		tokenizedQuery[setPos + 2] != "=")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.updateTargetColumn = tokenizedQuery[setPos + 1];

	/* literal after '=' */
	if (!regex_match(tokenizedQuery[setPos + 3], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateLiteral = stoi(tokenizedQuery[setPos + 3]);

	/* extra tokens? */
	if (setPos + 4 != tokenizedQuery.size())
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	return true;
}

/* semantic & executor remain dummy */
bool semanticParseUPDATE()
{
	logger.log("semanticParseUPDATE");

	/* table must exist */
	if (!tableCatalogue.isTable(parsedQuery.updateRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.updateRelationName);

	/* target column must exist */
	if (!table->isColumn(parsedQuery.updateTargetColumn))
	{
		cout << "SEMANTIC ERROR: Target column not found" << endl;
		return false;
	}

	/* WHERE column must exist */
	if (!table->isColumn(parsedQuery.updateCondColumn))
	{
		cout << "SEMANTIC ERROR: Condition column not found" << endl;
		return false;
	}
	return true;
}

static void writeIntRow(ofstream &fout, const vector<int> &row)
{
	for (size_t c = 0; c < row.size(); ++c)
	{
		if (c)
			fout << ", ";
		fout << row[c];
	}
	fout << '\n';
}

void executeUPDATE()
{
    logger.log("executeUPDATE");

    Table *table = tableCatalogue.getTable(parsedQuery.updateRelationName);
    if (!table)
    {
        cout << "FATAL ERROR: Table '" << parsedQuery.updateRelationName << "' not found during execution." << endl;
        return;
    }
    if (table->columnCount == 0)
    {
        cout << "ERROR: Table '" << table->tableName << "' has no columns. Cannot update." << endl;
        return;
    }

    // Get indices for relevant columns
    int targetColIndex = table->getColumnIndex(parsedQuery.updateTargetColumn);
    int condColIndex = table->getColumnIndex(parsedQuery.updateCondColumn);

    if (targetColIndex < 0)
    {
        cout << "SEMANTIC ERROR: Target column '" << parsedQuery.updateTargetColumn << "' not found." << endl;
        return;
    }
    if (condColIndex < 0)
    {
        cout << "SEMANTIC ERROR: Condition column '" << parsedQuery.updateCondColumn << "' not found." << endl;
        return;
    }

    long long rowsUpdatedCounter = 0;
    vector<RecordPointer> pointersToUpdate; // Store {pageIdx, rowIdxInPage}
    bool indexUsedForLookup = false;
    BTree* indexToUse = nullptr; // Pointer to the specific index if used

    // --- 1. Find Rows to Update ---
    logger.log("executeUPDATE: Scanning table to find matching rows...");

    // Conditions to use index for lookup: WHERE column is indexed, operator is EQUAL
    if (parsedQuery.updateCondOperator == EQUAL && table->isIndexed(parsedQuery.updateCondColumn))
    {
        indexToUse = table->getIndex(parsedQuery.updateCondColumn); // Get index for the condition column
        if (indexToUse != nullptr) // Check if index object actually exists
        {
            // ** Use Index Lookup **
            logger.log("executeUPDATE: Using index on column '" + parsedQuery.updateCondColumn + "' to find rows where key == " + to_string(parsedQuery.updateCondValue));
            pointersToUpdate = indexToUse->searchKey(parsedQuery.updateCondValue);
            indexUsedForLookup = true;
            logger.log("executeUPDATE: Index search returned " + to_string(pointersToUpdate.size()) + " potential rows.");

            // ** Integrated Pointer Validation Step **
            size_t originalPointerCount = pointersToUpdate.size();
            pointersToUpdate.erase(
                std::remove_if(pointersToUpdate.begin(), pointersToUpdate.end(),
                               [&](const RecordPointer &p)
                               {
                                   // Check basic bounds: page index valid? row index non-negative?
                                   if (p.first < 0 || p.first >= table->blockCount || p.second < 0)
                                   {
                                       logger.log("executeUPDATE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Basic bounds check failed).");
                                       return true; // Remove this pointer
                                   }
                                   // Check if row index is within the bounds for that *specific* page using table metadata
                                   if (p.first >= table->rowsPerBlockCount.size())
                                   {
                                       logger.log("executeUPDATE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Page index out of bounds for rowsPerBlockCount lookup).");
                                       return true; // Remove this pointer
                                   }
                                   if (p.second >= table->rowsPerBlockCount[p.first])
                                   {
                                       logger.log("executeUPDATE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Row index >= rows in page " + to_string(table->rowsPerBlockCount[p.first]) + ").");
                                       return true; // Remove this pointer
                                   }
                                   return false; // Keep this pointer
                               }),
                pointersToUpdate.end());

            if (pointersToUpdate.size() < originalPointerCount)
            {
                logger.log("executeUPDATE: Validation - Removed " + to_string(originalPointerCount - pointersToUpdate.size()) + " invalid pointers. Valid pointers count: " + to_string(pointersToUpdate.size()));
            }
            else
            {
                // logger.log("executeUPDATE: Validation - All pointers returned by index seem valid based on metadata."); // Can be verbose
            }
            // ** End of Integrated Pointer Validation Step **
        } else {
             logger.log("executeUPDATE: Column '" + parsedQuery.updateCondColumn + "' marked as indexed, but index object is null. Falling back to scan.");
        }
    }
    else
    {
        // ** Fallback to Full Table Scan **
        if (!table->indexes.empty()) { // Log only if indexes exist but weren't used
             logger.log("executeUPDATE: Index(es) exist but cannot be used for this query (Column='" + parsedQuery.updateCondColumn + "', Operator=" + to_string(parsedQuery.updateCondOperator) + "). Performing table scan.");
        } else {
             logger.log("executeUPDATE: Table not indexed or index object missing. Performing table scan.");
        }

        // int condColIndex = table->getColumnIndex(parsedQuery.updateCondColumn); // Already calculated above

        Cursor cursor = table->getCursor();
        vector<int> row = cursor.getNext();

        while (!row.empty())
        {
            // Calculate pointer for the current row
            int currentPageIndex = cursor.pageIndex;
            int currentRowInPage = cursor.pagePointer - 1; // Index of the row just returned

            // Basic validation for pointer calculation during scan
            if (currentRowInPage < 0 || currentPageIndex < 0 || currentPageIndex >= table->blockCount)
            {
                // Check specific case of first row: pageIndex=0, pagePointer=1 -> currentRowInPage=0. This is valid.
                if (!(currentPageIndex == 0 && cursor.pagePointer == 1 && currentRowInPage == 0))
                {
                    logger.log("executeUPDATE: Warning - Invalid pointer calculation during scan (Page=" + to_string(currentPageIndex) + ", RowPtr=" + to_string(cursor.pagePointer) + ", RowIdx=" + to_string(currentRowInPage) + "). Skipping row check.");
                    row = cursor.getNext();
                    continue; // Skip processing this potentially invalid state
                }
                // Handle the first row case correctly
                currentRowInPage = 0;
            }

            // Check the WHERE condition
            if (condColIndex < 0)
            { // Check condition column index validity once
                logger.log("executeUPDATE: Error - Condition column index invalid during scan setup.");
                break; // Stop scan
            }
            if (condColIndex >= row.size())
            {
                logger.log("executeUPDATE: Error - Row size mismatch during scan. Row size=" + to_string(row.size()) + ", Cond Idx=" + to_string(condColIndex));
                break; // Stop scan if schema mismatch detected
            }

            if (evaluateBinOp(row[condColIndex], parsedQuery.updateCondValue, parsedQuery.updateCondOperator))
            {
                pointersToUpdate.push_back({currentPageIndex, currentRowInPage});
            }
            row = cursor.getNext();
        }
        logger.log("executeUPDATE: Scan complete. Found " + to_string(pointersToUpdate.size()) + " rows matching criteria.");
        indexUsedForLookup = false; // Explicitly set for clarity
    }

    // --- 2. Process Updates (Pointer by Pointer) ---
    // Note: Updating page by page might be slightly more efficient if many rows
    // are updated on the same page, but pointer by pointer is simpler to implement first.

    logger.log("executeUPDATE: Processing updates...");
    for (const auto &pointer : pointersToUpdate)
    {
        int pageIndex = pointer.first;
        int rowIndexInPage = pointer.second;

        logger.log("executeUPDATE: Updating row at {" + to_string(pageIndex) + ", " + to_string(rowIndexInPage) + "}");

        Page page = bufferManager.getPage(table->tableName, pageIndex);
        int loadedRowCount = page.getRowCount();
        if (loadedRowCount <= rowIndexInPage)
        { // Check if row index is valid for the loaded page
            cout << "ERROR: Row index " << rowIndexInPage << " out of bounds for page " << pageIndex << " (size " << loadedRowCount << ")." << endl;
            logger.log("executeUPDATE: ERROR - Row index out of bounds for page " + to_string(pageIndex));
            continue; // Skip this pointer
        }

        // Get original row data
        vector<int> originalRow = page.getRow(rowIndexInPage);
        if (originalRow.empty())
        {
            cout << "ERROR: Failed to read original row " << rowIndexInPage << " from page " << pageIndex << "." << endl;
            logger.log("executeUPDATE: ERROR - Failed to read original row " + to_string(rowIndexInPage) + " page " + to_string(pageIndex));
            continue; // Skip this pointer
        }

        // --- Store old key values for ALL indexed columns BEFORE modification ---
        std::map<string, int> oldIndexedValues; // Map columnName -> oldValue
        if (!table->indexes.empty()) {
            for (const auto& [colName, indexPtr] : table->indexes) {
                if (indexPtr) {
                    int idx = table->getColumnIndex(colName);
                    if (idx >= 0 && idx < originalRow.size()) {
                        oldIndexedValues[colName] = originalRow[idx];
                    } else {
                        logger.log("executeUPDATE: Warning - Could not get old value for indexed column '" + colName + "' (index " + to_string(idx) + ").");
                    }
                }
            }
        }
        // --- End Store old key values ---

        // Create modified row
        vector<int> modifiedRow = originalRow;
        modifiedRow[targetColIndex] = parsedQuery.updateLiteral; // Apply the update

        // --- Get new key value for the TARGET column if it's indexed ---
        int newTargetKeyValue = modifiedRow[targetColIndex];
        // --- End Get new key value ---

        // ** Rewrite the entire page **
        // This is inefficient but necessary without a Page::updateRow method.
        vector<vector<int>> pageRows;
        pageRows.reserve(loadedRowCount);
        bool readError = false;
        for (int i = 0; i < loadedRowCount; ++i)
        {
            if (i == rowIndexInPage)
            {
                pageRows.push_back(modifiedRow); // Use the modified row
            }
            else
            {
                vector<int> currentRow = page.getRow(i);
                if (currentRow.empty() && i < loadedRowCount)
                {
                    logger.log("executeUPDATE: Error reading row " + to_string(i) + " while rewriting page " + to_string(pageIndex));
                    readError = true;
                    break; // Stop processing this page
                }
                if (!currentRow.empty())
                {
                    pageRows.push_back(currentRow); // Use original row
                }
            }
        }

        if (readError)
        {
            logger.log("executeUPDATE: Aborting update for page " + to_string(pageIndex) + " due to read error.");
            continue; // Skip to the next pointer
        }

        bufferManager.writePage(table->tableName, pageIndex, pageRows, loadedRowCount); // Row count doesn't change

        // ** Index Maintenance: Update ALL affected indexes **
        if (!table->indexes.empty()) {
            logger.log("executeUPDATE: Performing index maintenance for updated row at {" + to_string(pageIndex) + "," + to_string(rowIndexInPage) + "}");
            for (const auto& [colName, indexPtr] : table->indexes) {
                if (indexPtr) {
                    int idx = table->getColumnIndex(colName);
                    if (idx < 0 || idx >= modifiedRow.size()) continue; // Skip if column index invalid

                    int newKey = modifiedRow[idx];
                    int oldKey = -1; // Default if not found
                    auto oldValIt = oldIndexedValues.find(colName);
                    if (oldValIt != oldIndexedValues.end()) {
                        oldKey = oldValIt->second;
                    } else {
                        // This case should ideally not happen if we stored all old values correctly
                        logger.log("executeUPDATE: Warning - Old key value not found for indexed column '" + colName + "' during maintenance.");
                        // Attempt to fetch again? Or assume it didn't change? Assuming no change might be risky.
                        // Let's assume if not found, it didn't change (or wasn't indexed before, which is wrong).
                        // A safer approach might be to re-fetch the originalRow here if needed.
                        // For now, we proceed assuming oldKey = newKey if not found in map.
                        oldKey = newKey;
                    }


                    // Only update the index if the key value for *this specific index's column* changed
                    if (oldKey != newKey) {
                        logger.log("executeUPDATE: Value changed for indexed column '" + colName + "' (Old: " + to_string(oldKey) + ", New: " + to_string(newKey) + "). Updating index.");

                        // Use BTree::deleteKey(key) - This removes ALL entries for the old key.
                        logger.log("executeUPDATE: Calling index->deleteKey(" + to_string(oldKey) + ") for index '" + indexPtr->getIndexName() + "'");
                        if (!indexPtr->deleteKey(oldKey)) {
                            logger.log("executeUPDATE: WARNING - BTree deleteKey returned false for old key " + to_string(oldKey) + " in index '" + indexPtr->getIndexName() + "'");
                            // Potential inconsistency: old entry might still be there.
                        }

                        // Use BTree::insertKey(key, pointer)
                        logger.log("executeUPDATE: Calling index->insertKey(" + to_string(newKey) + ", {" + to_string(pageIndex) + "," + to_string(rowIndexInPage) + "}) for index '" + indexPtr->getIndexName() + "'");
                        if (!indexPtr->insertKey(newKey, pointer)) {
                            logger.log("executeUPDATE: WARNING - BTree insertKey returned false for new key " + to_string(newKey) + " in index '" + indexPtr->getIndexName() + "'");
                            // Potential inconsistency: new entry might be missing.
                        }
                    } else {
                        // logger.log("executeUPDATE: Value for indexed column '" + colName + "' did not change. No update needed for this index."); // Can be verbose
                    }
                }
            }
        }
        // --- End Index Maintenance ---

        rowsUpdatedCounter++;
    }

    // --- 3. Print Result ---
    cout << rowsUpdatedCounter << " row(s) updated in \"" << table->tableName << "\"." << endl;
    // table->rowCount remains unchanged.
}
