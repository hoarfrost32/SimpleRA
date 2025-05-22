#include "../global.h"
#include <vector>
#include <map>
#include <algorithm> // Needed for std::remove_if
#include <set>
#include <string>
#include "../table.h"
#include "../page.h"
#include "../index.h"
#include "../bufferManager.h"
#include <regex>

/**
 * Grammar:
 *   DELETE FROM <table> WHERE <col> <binop> <int>
 * tokenizedQuery = ["DELETE","FROM","T","WHERE","col","<", "10"]
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

bool syntacticParseDELETE()
{
	logger.log("syntacticParseDELETE");
	if (tokenizedQuery.size() != 7 ||
		tokenizedQuery[1] != "FROM" ||
		tokenizedQuery[3] != "WHERE")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = DELETE;
	parsedQuery.deleteRelationName = tokenizedQuery[2];
	parsedQuery.deleteCondColumn = tokenizedQuery[4];

	if (!parseBinOp(tokenizedQuery[5], parsedQuery.deleteCondOperator))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	regex numeric("[-]?[0-9]+");
	if (!regex_match(tokenizedQuery[6], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.deleteCondValue = stoi(tokenizedQuery[6]);
	return true;
}

/* keep semantic & executor dummy */
bool semanticParseDELETE()
{
	logger.log("semanticParseDELETE");

	if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);

	if (!table->isColumn(parsedQuery.deleteCondColumn))
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

void executeDELETE()
{
    logger.log("executeDELETE");
    Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
    if (!table)
    {
        cout << "FATAL ERROR: Table '" << parsedQuery.deleteRelationName << "' not found during execution." << endl;
        return;
    }

    vector<RecordPointer> pointersToDelete; // Store {pageIdx, rowIdxInPage}
    map<RecordPointer, vector<int>> deletedRowData; // Store actual data for index maintenance {ptr -> rowData}
    bool indexUsed = false;
    BTree *indexToUse = nullptr; // Pointer to the specific index if used

    // --- 1. Find Rows to Delete ---
	// Conditions to use index: WHERE column is indexed, operator is EQUAL
	if (parsedQuery.deleteCondOperator == EQUAL && table->isIndexed(parsedQuery.deleteCondColumn))
	{
		indexToUse = table->getIndex(parsedQuery.deleteCondColumn); // Get index for the condition column
		if (indexToUse != nullptr) // Check if index object actually exists
		{
			// ** Use Index Lookup **
			logger.log("executeDELETE: Using index on column '" + parsedQuery.deleteCondColumn + "' to find rows where key == " + to_string(parsedQuery.deleteCondValue));
			pointersToDelete = indexToUse->searchKey(parsedQuery.deleteCondValue);
			indexUsed = true;
			logger.log("executeDELETE: Index search returned " + to_string(pointersToDelete.size()) + " potential rows.");

			// ** Integrated Pointer Validation Step **
			size_t originalPointerCount = pointersToDelete.size();
			pointersToDelete.erase(
				std::remove_if(pointersToDelete.begin(), pointersToDelete.end(),
								[&](const RecordPointer &p)
								{
									// Check basic bounds: page index valid? row index non-negative?
									if (p.first < 0 || p.first >= table->blockCount || p.second < 0)
									{
										logger.log("executeDELETE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Basic bounds check failed).");
										return true; // Remove this pointer
									}
									// Check if row index is within the bounds for that *specific* page using table metadata
									if (p.first >= table->rowsPerBlockCount.size())
									{
										logger.log("executeDELETE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Page index out of bounds for rowsPerBlockCount lookup).");
										return true; // Remove this pointer
									}
									if (p.second >= table->rowsPerBlockCount[p.first])
									{
										logger.log("executeDELETE: Validation - Removing invalid pointer {page=" + to_string(p.first) + ", row=" + to_string(p.second) + "} (Row index >= rows in page " + to_string(table->rowsPerBlockCount[p.first]) + ").");
										return true; // Remove this pointer
									}
									return false; // Keep this pointer
								}),
				pointersToDelete.end());

			if (pointersToDelete.size() < originalPointerCount)
			{
				logger.log("executeDELETE: Validation - Removed " + to_string(originalPointerCount - pointersToDelete.size()) + " invalid pointers. Valid pointers count: " + to_string(pointersToDelete.size()));
			}
			// ** End of Integrated Pointer Validation Step **

			// Fetch row data for valid pointers found via index
			for (const auto &ptr : pointersToDelete)
			{
				Page page = bufferManager.getPage(table->tableName, ptr.first);
				vector<int> row = page.getRow(ptr.second);
				if (!row.empty())
				{
					deletedRowData[ptr] = row;
				}
				else
				{
					logger.log("executeDELETE: Warning - Could not fetch row data for pointer {" + to_string(ptr.first) + "," + to_string(ptr.second) + "} found via index. Index might be stale.");
					// Keep the pointer in pointersToDelete, but it won't have data in deletedRowData for index maintenance.
				}
			}
		}
		else
		{
			logger.log("executeDELETE: Column '" + parsedQuery.deleteCondColumn + "' marked as indexed, but index object is null. Falling back to scan.");
		}
	}

	// Fallback to Full Table Scan if index not used or not applicable
	if (!indexUsed)
	{
		if (!table->indexes.empty())
		{ // Log only if indexes exist but weren't used
			logger.log("executeDELETE: Index(es) exist but cannot be used for this query (Column='" + parsedQuery.deleteCondColumn + "', Operator=" + to_string(parsedQuery.deleteCondOperator) + "). Performing table scan.");
		}
		else
		{
			logger.log("executeDELETE: Table not indexed or index not usable. Performing table scan.");
		}

		int condColIndex = table->getColumnIndex(parsedQuery.deleteCondColumn);
		if (condColIndex < 0)
		{
			cout << "SEMANTIC ERROR: Column '" << parsedQuery.deleteCondColumn << "' not found for WHERE clause." << endl;
			return; // Abort if condition column doesn't exist
		}

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
				if (!(currentPageIndex == 0 && cursor.pagePointer == 1 && currentRowInPage == 0))
				{
					logger.log("executeDELETE: Warning - Invalid pointer calculation during scan (Page=" + to_string(currentPageIndex) + ", RowPtr=" + to_string(cursor.pagePointer) + ", RowIdx=" + to_string(currentRowInPage) + "). Skipping row check.");
					row = cursor.getNext();
					continue;
				}
				currentRowInPage = 0;
			}
			// Check row size before accessing column
			if (condColIndex >= row.size())
			{
				logger.log("executeDELETE: Error - Row size mismatch during scan. Row size=" + to_string(row.size()) + ", Cond Idx=" + to_string(condColIndex) + ". Skipping row.");
				row = cursor.getNext();
				continue;
			}

			if (evaluateBinOp(row[condColIndex], parsedQuery.deleteCondValue, parsedQuery.deleteCondOperator))
			{
				RecordPointer ptr = {currentPageIndex, currentRowInPage};
				pointersToDelete.push_back(ptr);
				deletedRowData[ptr] = row; // Store row data for index maintenance
			}
			row = cursor.getNext();
		}
		logger.log("executeDELETE: Scan complete. Found " + to_string(pointersToDelete.size()) + " rows matching criteria.");
	}

    // --- If no rows to delete, exit early ---
    if (pointersToDelete.empty())
    {
        cout << "No rows matched the criteria. 0 rows deleted from table '" << table->tableName << "'." << endl;
        logger.log("executeDELETE: No rows to delete.");
        return;
    }

    // --- 2. Group Deletions by Page ---
    map<int, vector<int>> rowsToDeleteByPage;
    for (const auto &pointer : pointersToDelete)
    {
        rowsToDeleteByPage[pointer.first].push_back(pointer.second);
    }

    // Sort row indices within each page's list for efficient processing (ascending order)
    for (auto &pair : rowsToDeleteByPage)
    {
        sort(pair.second.begin(), pair.second.end()); // Sort ascending
    }

    // --- 3. Process Deletions Page by Page ---
    logger.log("executeDELETE: Processing deletions page by page...");
    long long totalRowsDeleted = 0;
    vector<uint> newRowsPerBlockCount = table->rowsPerBlockCount; // Copy to update safely
    bool pageRewriteErrorOccurred = false; // Flag to track if any page failed

    for (auto const &[pageIndex, rowIndicesToDelete] : rowsToDeleteByPage)
    {
        Page page = bufferManager.getPage(table->tableName, pageIndex);
        int originalRowCount = page.getRowCount(); // Use getter
        if (originalRowCount < 0)
        { // Basic check if getRowCount failed
            logger.log("executeDELETE: Error - Failed to get row count for page " + to_string(pageIndex) + ". Skipping page.");
            pageRewriteErrorOccurred = true; // Mark error and skip this page
            continue;
        }

        vector<vector<int>> keptRows;
        keptRows.reserve(originalRowCount); // Reserve based on original count
        bool readErrorOnPage = false;

        int deletePtr = 0; // Pointer into the sorted rowIndicesToDelete vector
        for (int i = 0; i < originalRowCount; ++i)
        { // Iterate using row count
            bool deleteThisRow = false;
            if (deletePtr < rowIndicesToDelete.size() && i == rowIndicesToDelete[deletePtr])
            {
                // This row index matches the next one in the ascending list to delete
                deleteThisRow = true;
                deletePtr++;
            }

            if (!deleteThisRow)
            {
                vector<int> currentRow = page.getRow(i); // Use getter
                if (currentRow.empty() && i < originalRowCount)
                { // Check if getRow failed unexpectedly
                    logger.log("executeDELETE: Error - Failed to get row " + to_string(i) + " from page " + to_string(pageIndex) + " while rebuilding. Skipping page.");
                    readErrorOnPage = true; // Mark error for this page
                    break;					// Stop processing this page
                }
                if (!currentRow.empty())
                { // Only add if row was successfully retrieved
                    keptRows.push_back(currentRow);
                }
            }
        }

        // If a read error occurred while processing rows for this page, skip writing it back
        if (readErrorOnPage)
        {
            logger.log("executeDELETE: Aborting rewrite for page " + to_string(pageIndex) + " due to previous getRow error.");
            pageRewriteErrorOccurred = true; // Mark that an error occurred
            continue;						 // Skip writing this page and updating metadata for it
        }

        // Write the modified page back (only if no error occurred for this page)
        bufferManager.writePage(table->tableName, pageIndex, keptRows, keptRows.size());
        logger.log("executeDELETE: Rewrote page " + to_string(pageIndex) + " with " + to_string(keptRows.size()) + " rows (deleted " + to_string(rowIndicesToDelete.size()) + ").");

        // Update the count for this block in our temporary vector
        if (pageIndex < newRowsPerBlockCount.size())
        {
            newRowsPerBlockCount[pageIndex] = keptRows.size();
        }
        else
        {
            logger.log("executeDELETE: Error - pageIndex " + to_string(pageIndex) + " out of bounds for newRowsPerBlockCount during update.");
            pageRewriteErrorOccurred = true; // Mark error
                                             // This indicates a serious issue if it happens.
        }
        // Accumulate deleted count only if page processing was successful
        totalRowsDeleted += rowIndicesToDelete.size();
    }

    // --- 4. Update Table Metadata (Only if no page rewrite errors occurred) ---
    if (!pageRewriteErrorOccurred)
    {
        table->rowsPerBlockCount = newRowsPerBlockCount; // Assign the updated counts
        table->rowCount -= totalRowsDeleted;
        // Note: blockCount remains the same, pages are rewritten, not removed.
        //       Distinct value counts are not updated here, would require re-scan.
        cout << "Deleted " << totalRowsDeleted << " rows from table '" << table->tableName << "'. New Row Count: " << table->rowCount << endl;
    }
    else
    {
        cout << "ERROR: One or more pages could not be processed correctly during delete. Table metadata may be inconsistent." << endl;
        logger.log("executeDELETE: Errors occurred during page processing. Table metadata update skipped.");
        // Do not update table->rowCount or table->rowsPerBlockCount if errors occurred
        // Index maintenance should also be skipped or handled carefully
    }

    // --- 5. Index Maintenance: Delete entries from ALL indexes (Only if no page rewrite errors) ---
    if (totalRowsDeleted > 0 && !table->indexes.empty() && !pageRewriteErrorOccurred)
    {
        logger.log("executeDELETE: Performing index maintenance for " + to_string(totalRowsDeleted) + " deleted rows...");

        // Iterate through each row that was successfully marked for deletion
        for (const auto &pointer : pointersToDelete)
        {
            auto dataIt = deletedRowData.find(pointer);
            if (dataIt == deletedRowData.end())
            {
                logger.log("executeDELETE: Warning - Row data not found for deleted pointer {" + to_string(pointer.first) + "," + to_string(pointer.second) + "}. Skipping index maintenance for this row.");
                continue; // Skip if we couldn't store the row data earlier
            }
            const vector<int> &deletedRow = dataIt->second;

            // Iterate through all indexes on the table
            for (const auto &[colName, indexPtr] : table->indexes)
            {
                if (indexPtr)
                {
                    int idx = table->getColumnIndex(colName);
                    if (idx >= 0 && idx < deletedRow.size())
                    {
                        int key = deletedRow[idx];

                        // Use BTree::deleteKey(key) - This removes ALL entries for this key.
                        // IMPORTANT: BTree::deleteKey needs to handle the case where the key
                        // might have multiple pointers and only remove the specific one if possible,
                        // or remove the key entirely if that's the intended behavior.
                        // Assuming BTree::deleteKey(key) removes the key and all associated pointers.
                        logger.log("executeDELETE: Calling index->deleteKey(" + to_string(key) + ") for index '" + indexPtr->getIndexName() + "' due to deletion of row at {" + to_string(pointer.first) + "," + to_string(pointer.second) + "}");
                        if (!indexPtr->deleteKey(key))
                        {
                            // This might just mean the key wasn't found (e.g., if index was already inconsistent)
                            // logger.log("executeDELETE: Info - BTree deleteKey returned false for key " + to_string(key) + " in index '" + indexPtr->getIndexName() + "' (Key might not have been present)."); // Can be verbose
                        }
                    }
                    else
                    {
                        logger.log("executeDELETE: Warning - Could not get key for indexed column '" + colName + "' (index " + to_string(idx) + ") from deleted row data.");
                    }
                }
            }
        }
        logger.log("executeDELETE: Finished index maintenance.");
    }
    else if (totalRowsDeleted > 0 && !pageRewriteErrorOccurred)
    {
        logger.log("executeDELETE: No indexes found on table '" + table->tableName + "'. Skipping index maintenance.");
    }
    else if (pageRewriteErrorOccurred)
    {
        logger.log("executeDELETE: Skipping index maintenance due to errors during page processing.");
    }
    // --- End Index Maintenance ---

    return;
}
