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
	if (table->columnCount == 0)
	{
		cout << "ERROR: Table '" << table->tableName << "' has no columns. Cannot delete." << endl;
		return;
	}

	int condColIndex = table->getColumnIndex(parsedQuery.deleteCondColumn);
	if (condColIndex < 0)
	{
		cout << "SEMANTIC ERROR: Condition column '" << parsedQuery.deleteCondColumn << "' not found." << endl;
		// Error message might have already been printed by getColumnIndex
		return;
	}

	long long initialRowCount = table->rowCount; // Store for final count calculation
	long long rowsDeletedCounter = 0;

	// --- 1. Find Rows to Delete (using Full Table Scan for Phase 1) ---
	vector<RecordPointer> pointersToDelete; // Store {pageIdx, rowIdxInPage}

	bool indexUsed = false;
	// Conditions to use index: table indexed, index object exists, WHERE column is the indexed column, operator is EQUAL
	if (table->indexed && table->index != nullptr &&
		parsedQuery.deleteCondColumn == table->indexedColumn &&
		parsedQuery.deleteCondOperator == EQUAL)
	{
		// ** Use Index Lookup **
		logger.log("executeDELETE: Using index on column '" + table->indexedColumn + "' to find rows where key == " + to_string(parsedQuery.deleteCondValue));
		pointersToDelete = table->index->searchKey(parsedQuery.deleteCondValue);
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
							   // Need to ensure page index itself is valid for rowsPerBlockCount lookup first
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
		else
		{
			// logger.log("executeDELETE: Validation - All pointers returned by index seem valid based on metadata."); // Can be verbose
		}
		// ** End of Integrated Pointer Validation Step **
	}
	else
	{
		// ** Fallback to Full Table Scan **
		if (table->indexed && table->index != nullptr)
		{ // Check index != nullptr here too
			logger.log("executeDELETE: Index exists but cannot be used for this query (Column='" + parsedQuery.deleteCondColumn + "', Operator=" + to_string(parsedQuery.deleteCondOperator) + "). Performing table scan.");
		}
		else
		{
			logger.log("executeDELETE: Table not indexed or index object missing. Performing table scan.");
		}

		int condColIndex = table->getColumnIndex(parsedQuery.deleteCondColumn); // Moved calculation here, only needed for scan

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
					logger.log("executeDELETE: Warning - Invalid pointer calculation during scan (Page=" + to_string(currentPageIndex) + ", RowPtr=" + to_string(cursor.pagePointer) + ", RowIdx=" + to_string(currentRowInPage) + "). Skipping row check.");
					row = cursor.getNext();
					continue; // Skip processing this potentially invalid state
				}
				// Handle the first row case correctly
				currentRowInPage = 0;
			}

			// Check the WHERE condition
			if (condColIndex < 0)
			{ // Check condition column index validity once
				logger.log("executeDELETE: Error - Condition column index invalid during scan setup.");
				break; // Stop scan
			}
			if (condColIndex >= row.size())
			{
				logger.log("executeDELETE: Error - Row size mismatch during scan. Row size=" + to_string(row.size()) + ", Cond Idx=" + to_string(condColIndex));
				// Should not happen if CSV is consistent, but safety first
				break; // Stop scan if schema mismatch detected
			}

			if (evaluateBinOp(row[condColIndex], parsedQuery.deleteCondValue, parsedQuery.deleteCondOperator))
			{
				pointersToDelete.push_back({currentPageIndex, currentRowInPage});
			}
			row = cursor.getNext();
		}
		logger.log("executeDELETE: Scan complete. Found " + to_string(pointersToDelete.size()) + " rows matching criteria.");
		indexUsed = false; // Explicitly set for clarity
	}
	// --- 2. Group Deletions by Page ---
	map<int, vector<int>> rowsToDeleteByPage;
	for (const auto &pointer : pointersToDelete)
	{
		rowsToDeleteByPage[pointer.first].push_back(pointer.second);
	}

	// Sort row indices within each page's list for efficient processing
	for (auto &pair : rowsToDeleteByPage)
	{
		sort(pair.second.begin(), pair.second.end());
	}

	int indexedColIdx = -1; // Get this only once if needed
	if (table->indexed && table->index != nullptr)
	{
		indexedColIdx = table->getColumnIndex(table->indexedColumn);
		if (indexedColIdx < 0)
		{
			cout << "INTERNAL ERROR: Indexed column '" << table->indexedColumn << "' not found during DELETE execution." << endl;
			logger.log("executeDELETE: ERROR - Indexed column not found for index update.");
			// Proceed without index update? Or abort? Abort seems safer.
			return;
		}
	}

	// --- 3. Process Deletions Page by Page ---
	logger.log("executeDELETE: Processing deletions page by page...");
	for (auto const &page_pair : rowsToDeleteByPage)
	{															  // Iterate through key-value pairs
		int pageIndex = page_pair.first;						  // Extract the page index (key)
		const vector<int> &rowIndicesToDelete = page_pair.second; // Extract the vector (value)

		logger.log("executeDELETE: Processing page " + to_string(pageIndex));
		Page page = bufferManager.getPage(table->tableName, pageIndex);
		int loadedRowCount = page.getRowCount();
		if (loadedRowCount < 0)
		{
			cout << "ERROR: Failed to load page " << pageIndex << " for deletion." << endl;
			logger.log("executeDELETE: ERROR - Failed loading page " + to_string(pageIndex));
			// Problem: some rows might already be deleted from index. State is inconsistent.
			// For now, continue and hope for the best, or abort? Let's continue but log error.
			continue;
		}

		vector<vector<int>> keptRows;
		keptRows.reserve(loadedRowCount); // Reserve approximate space
		// Use a set for O(1) average lookup of rows to delete on this page
		set<int> deletionSet(rowIndicesToDelete.begin(), rowIndicesToDelete.end());
		int deletedInThisPage = 0;

		for (int i = 0; i < loadedRowCount; ++i)
		{
			if (deletionSet.count(i))
			{
				// This row index 'i' needs to be deleted
				deletedInThisPage++;

				// ** Index Maintenance **
				if (table->indexed && table->index != nullptr && indexedColIdx >= 0)
				{
					vector<int> originalRow = page.getRow(i); // Get data before deleting
					if (originalRow.empty())
					{
						logger.log("executeDELETE: Warning - Tried to get data for index delete from empty row " + to_string(i) + " in page " + to_string(pageIndex));
						continue; // Skip index update if row data isn't readable
					}
					if (indexedColIdx >= originalRow.size())
					{
						logger.log("executeDELETE: Warning - Row size mismatch when getting key for index delete from row " + to_string(i) + " in page " + to_string(pageIndex));
						continue; // Skip index update
					}

					int key = originalRow[indexedColIdx];
					RecordPointer pointerToDeleteFromIndex = {pageIndex, i};					   // The exact pointer
					logger.log("executeDELETE: Calling index->deleteKey(" + to_string(key) + ")"); // Add pointer if needed: ", {" + to_string(pointerToDeleteFromIndex.first) + "," + to_string(pointerToDeleteFromIndex.second) + "})" );

					// **** IMPORTANT: Adapt this call based on how BTree::deleteKey is implemented ****
					// If deleteKey needs the pointer: table->index->deleteKey(key, pointerToDeleteFromIndex);
					// If deleteKey(key) deletes ALL occurrences of key:
					if (!table->index->deleteKey(key))
					{
						logger.log("executeDELETE: WARNING - BTree deleteKey returned false for key " + to_string(key));
					}
					// If deleteKey(key) only deletes ONE arbitrary occurrence, this logic is insufficient for duplicates.
				}
			}
			else
			{
				// Keep this row
				vector<int> rowToKeep = page.getRow(i);
				if (rowToKeep.empty() && i < loadedRowCount)
				{
					logger.log("executeDELETE: Warning - Got empty row " + to_string(i) + " from page " + to_string(pageIndex) + " when trying to keep it.");
					// Skip adding this potentially corrupt row?
					continue;
				}
				if (!rowToKeep.empty())
				{
					keptRows.push_back(rowToKeep);
				}
			}
		}

		// Write the modified (shorter) page back
		bufferManager.writePage(table->tableName, pageIndex, keptRows, keptRows.size());
		logger.log("executeDELETE: Rewrote page " + to_string(pageIndex) + " with " + to_string(keptRows.size()) + " rows (deleted " + to_string(deletedInThisPage) + ").");

		// Update table metadata for this page
		if (pageIndex >= table->rowsPerBlockCount.size())
		{
			cout << "INTERNAL ERROR: Metadata inconsistency - trying to update rowsPerBlockCount for out-of-bounds index " << pageIndex << endl;
			logger.log("executeDELETE: Error updating metadata - index out of bounds for page " + to_string(pageIndex));
			// Abort or continue with potentially corrupt metadata? Continue for now.
		}
		else
		{
			table->rowsPerBlockCount[pageIndex] = keptRows.size();
		}
		rowsDeletedCounter += deletedInThisPage; // Add to total count
	}

	// --- 4. Update Final Table Metadata ---
	table->rowCount = initialRowCount - rowsDeletedCounter; // Update total row count
	// Note: blockCount might decrease if pages become empty, but we are not implementing
	// page deletion/compaction in this phase. So blockCount remains unchanged.

	// --- 5. Print Result ---
	cout << rowsDeletedCounter << " row(s) deleted from \"" << table->tableName << "\". New Row Count = " << table->rowCount << endl;
}
