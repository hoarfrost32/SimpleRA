#include "../global.h"
#include <vector>
#include <string>
#include "../table.h"
#include "../page.h"
#include "../index.h"
#include "../bufferManager.h"
#include <regex>

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

	// --- 1. Find Rows to Update (using Full Table Scan for Phase 1) ---
	logger.log("executeUPDATE: Scanning table to find matching rows...");
	Cursor cursor = table->getCursor();
	vector<int> row = cursor.getNext();

	while (!row.empty())
	{
		// Calculate pointer for the current row (handle page wrap)
		int currentPageIndex = cursor.pageIndex;
		int currentRowInPage = cursor.pagePointer - 1; // Index of the row just returned

		// Validate pointer calculation - Reuse refined logic if necessary
		if (currentRowInPage < 0)
		{
			if (currentPageIndex == 0 && cursor.pagePointer == 1)
				currentRowInPage = 0; // First row case
			else
			{
				logger.log("executeUPDATE: Warning - Invalid pointer calculation during scan.");
				// Attempt to continue or skip? Skipping seems safer.
				row = cursor.getNext();
				continue;
			}
		}

		// Check the WHERE condition
		if (evaluateBinOp(row[condColIndex], parsedQuery.updateCondValue, parsedQuery.updateCondOperator))
		{
			if (currentPageIndex >= 0)
			{ // Ensure page index is valid
				pointersToUpdate.push_back({currentPageIndex, currentRowInPage});
			}
			else
			{
				logger.log("executeUPDATE: Warning - Skipping update for row with invalid page index.");
			}
		}
		row = cursor.getNext();
	}
	logger.log("executeUPDATE: Scan complete. Found " + to_string(pointersToUpdate.size()) + " rows matching criteria.");

	if (pointersToUpdate.empty())
	{
		cout << "0 rows matched the condition. No rows updated in \"" << table->tableName << "\"." << endl;
		return;
	}

	// --- 2. Process Updates (Pointer by Pointer) ---
	// Note: Updating page by page might be slightly more efficient if many rows
	// are updated on the same page, but pointer by pointer is simpler to implement first.

	int indexedColIdx = -1; // Get this once if table is indexed
	bool isIndexed = table->indexed && table->index != nullptr;
	if (isIndexed)
	{
		indexedColIdx = table->getColumnIndex(table->indexedColumn);
		if (indexedColIdx < 0)
		{
			cout << "INTERNAL ERROR: Indexed column '" << table->indexedColumn << "' not found during UPDATE execution." << endl;
			logger.log("executeUPDATE: ERROR - Indexed column not found for index update.");
			// Abort because index maintenance will fail.
			return;
		}
	}

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

		// Store old value if indexed column might be affected
		int oldIndexedValue = -1;
		if (isIndexed && indexedColIdx == targetColIndex)
		{ // Only store if target IS the indexed column
			if (indexedColIdx >= originalRow.size())
			{
				logger.log("executeUPDATE: Warning - Row size mismatch getting old indexed value.");
			}
			else
			{
				oldIndexedValue = originalRow[indexedColIdx];
			}
		}

		// Create modified row
		vector<int> modifiedRow = originalRow;
		modifiedRow[targetColIndex] = parsedQuery.updateLiteral;

		// Get new value if indexed column was affected
		int newIndexedValue = -1;
		if (isIndexed && indexedColIdx == targetColIndex)
		{ // Only get if target IS the indexed column
			if (indexedColIdx >= modifiedRow.size())
			{
				logger.log("executeUPDATE: Warning - Row size mismatch getting new indexed value.");
			}
			else
			{
				newIndexedValue = modifiedRow[indexedColIdx];
			}
		}

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

		// ** Index Maintenance **
		if (isIndexed && indexedColIdx == targetColIndex && oldIndexedValue != newIndexedValue)
		{
			// Condition: Table indexed, the target *was* the indexed column, AND the value changed.
			logger.log("executeUPDATE: Index maintenance required for key change from " + to_string(oldIndexedValue) + " to " + to_string(newIndexedValue));

			// **** Adapt deleteKey call based on BTree implementation ****
			logger.log("executeUPDATE: Calling index->deleteKey(" + to_string(oldIndexedValue) + ")");
			if (!table->index->deleteKey(oldIndexedValue))
			{
				logger.log("executeUPDATE: WARNING - BTree deleteKey returned false for old key " + to_string(oldIndexedValue));
				// Potential inconsistency: old entry might still be there.
			}

			logger.log("executeUPDATE: Calling index->insertKey(" + to_string(newIndexedValue) + ", {" + to_string(pageIndex) + "," + to_string(rowIndexInPage) + "})");
			if (!table->index->insertKey(newIndexedValue, pointer))
			{
				logger.log("executeUPDATE: WARNING - BTree insertKey returned false for new key " + to_string(newIndexedValue));
				// Potential inconsistency: new entry might be missing.
			}
		}
		rowsUpdatedCounter++;
	}

	// --- 3. Print Result ---
	cout << rowsUpdatedCounter << " row(s) updated in \"" << table->tableName << "\"." << endl;
	// table->rowCount remains unchanged.
}
