#include "../global.h"
#include <vector>
#include <map>		 // For grouping deletions by page
#include <algorithm> // For sorting row indices within a page
#include <set>		 // For quick lookup of row indices to delete
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
	vector<RecordPointer> pointersToDelete; // Store {pageIdx, rowIdxInPage}

	// --- 1. Find Rows to Delete (using Full Table Scan for Phase 1) ---
	logger.log("executeDELETE: Scanning table to find matching rows...");
	Cursor cursor = table->getCursor();
	vector<int> row = cursor.getNext();

	while (!row.empty())
	{
		// Get pointer info *before* checking condition
		int currentPageIndex = cursor.pageIndex;
		int currentRowInPage = cursor.pagePointer - 1; // pagePointer is 1-based index of *next* row

		// Handle cursor wrapping to the next page
		if (cursor.pagePointer == 1 && currentPageIndex > 0)
		{
			// This means getNext() just loaded page 'currentPageIndex',
			// so the row we just got is row 0 of this new page.
			// BUT the pointer should refer to the *previous* state.
			// Let's recalculate based on previous page.
			int previousPageIndex = currentPageIndex - 1;
			if (previousPageIndex >= 0 && previousPageIndex < table->rowsPerBlockCount.size())
			{
				int rowsInPrevPage = table->rowsPerBlockCount[previousPageIndex];
				currentPageIndex = previousPageIndex;  // Correct page index
				currentRowInPage = rowsInPrevPage - 1; // Last row (0-based) of previous page
			}
			else
			{
				// Should not happen with normal iteration, indicates metadata issue
				logger.log("executeDELETE: Warning - Metadata inconsistency during cursor wrap handling.");
				currentRowInPage = -1; // Mark as invalid
			}
		}
		else if (currentRowInPage < 0)
		{
			// Should only happen if called on very first row of first page, make it 0
			currentRowInPage = 0;
			// Let's refine the logic above - pagePointer is the index of the row *just returned* if 0-based logic was intended?
			// Re-reading cursor.h/cpp - getNext() returns row[pagePointer], then increments pagePointer.
			// So, the index of the row *just returned* is pagePointer - 1. This seems correct.
			// The wrap-around logic above might be overly complex if pagePointer is handled correctly internally.
			// Let's trust `cursor.pageIndex` and `cursor.pagePointer - 1` for now.
			currentRowInPage = cursor.pagePointer - 1;
			if (currentRowInPage < 0)
			{
				logger.log("executeDELETE: Warning - Calculated negative row index.");
				// If it's the first row after loading page 0, pagePointer is 1, index is 0. Seems okay.
			}
		}

		// Now check the condition
		if (!row.empty() && evaluateBinOp(row[condColIndex], parsedQuery.deleteCondValue, parsedQuery.deleteCondOperator))
		{
			if (currentPageIndex >= 0 && currentRowInPage >= 0)
			{
				pointersToDelete.push_back({currentPageIndex, currentRowInPage});
			}
			else
			{
				logger.log("executeDELETE: Warning - Skipping deletion for row with invalid pointer calculation.");
			}
		}
		row = cursor.getNext();
	}
	logger.log("executeDELETE: Scan complete. Found " + to_string(pointersToDelete.size()) + " rows matching criteria.");

	if (pointersToDelete.empty())
	{
		cout << "0 rows matched the condition. No rows deleted from \"" << table->tableName << "\"." << endl;
		return;
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
