#include "../global.h"
#include <vector>
#include <unordered_map>
#include <string>
#include "../table.h"
#include "../page.h"
#include "../index.h"
#include "../bufferManager.h"

/**
 * Grammar (comma already stripped by tokenizer):
 *   INSERT INTO <table> VALUES <v1> <v2> ... <vn>
 * tokenizedQuery = ["INSERT","INTO","T","VALUES","1","2",...]
 */

bool syntacticParseINSERT()
{
	logger.log("syntacticParseINSERT");

	/* Expected token pattern:
	   0 1     2           3 4 ... n‑2 n‑1
	   INSERT INTO <table> ( col = val , col = val … )                */

	if (tokenizedQuery.size() < 8 || // minimal length
		tokenizedQuery[1] != "INTO" ||
		tokenizedQuery[3] != "(" ||
		tokenizedQuery.back() != ")")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = INSERT;
	parsedQuery.insertRelationName = tokenizedQuery[2];
	parsedQuery.insertColumnValueMap.clear();

	/* Scan tokens between '(' and ')' */
	size_t i = 4;
	while (i < tokenizedQuery.size() - 1) // stop before ')'
	{
		string col = tokenizedQuery[i];
		if (col == ",")
		{
			i++;
			continue;
		}

		/* expect: col = val */
		if (i + 2 >= tokenizedQuery.size() ||
			tokenizedQuery[i + 1] != "=")
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}

		/* numeric literal? */
		regex numeric("[-]?[0-9]+");
		string valTok = tokenizedQuery[i + 2];
		if (!regex_match(valTok, numeric))
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}

		int value = stoi(valTok);
		parsedQuery.insertColumnValueMap[col] = value;

		i += 3;							   // jump past "col = val"
		if (i < tokenizedQuery.size() - 1) // if not at ')'
		{
			if (tokenizedQuery[i] == ",")
				i++; // consume comma
		}
	}

	if (parsedQuery.insertColumnValueMap.empty())
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	return true;
}

bool semanticParseINSERT()
{
	logger.log("semanticParseINSERT");

	if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);

	/* Each referenced column must exist */
	for (auto &kv : parsedQuery.insertColumnValueMap)
	{
		if (!table->isColumn(kv.first))
		{
			cout << "SEMANTIC ERROR: Column " << kv.first
				 << " doesn't exist in relation" << endl;
			return false;
		}
	}
	return true;
}

static void buildRow(const Table *table,
					 const std::unordered_map<string, int> &colVal,
					 std::vector<int> &outRow)
{
	outRow.assign(table->columnCount, 0); // default 0

	for (size_t c = 0; c < table->columnCount; ++c)
	{
		const string &colName = table->columns[c];
		auto it = colVal.find(colName);
		if (it != colVal.end())
			outRow[c] = it->second;
	}
}

void executeINSERT()
{
	logger.log("executeINSERT");

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
	if (!table)
	{
		cout << "FATAL ERROR: Table '" << parsedQuery.insertRelationName << "' not found during execution." << endl;
		return;
	}
	if (table->columnCount == 0)
	{
		cout << "FATAL ERROR: Table '" << table->tableName << "' has no columns defined." << endl;
		return; // Cannot insert into a table without columns
	}
	if (table->maxRowsPerBlock == 0)
	{
		cout << "FATAL ERROR: Table '" << table->tableName << "' maxRowsPerBlock is zero." << endl;
		return; // Avoid division by zero or incorrect logic
	}

	// 1. Build the full row vector based on user input and table schema
	vector<int> newRow;
	buildRow(table, parsedQuery.insertColumnValueMap, newRow);

	// --- Core Logic: Direct Page Insertion ---

	int targetPageIndex = -1;
	int rowIndexInPage = -1; // 0-based index within the target page
	bool newPageCreated = false;

	// 2. Determine target page and check if a new page is needed
	if (table->blockCount == 0)
	{
		// Table is currently empty, need to create the first page
		logger.log("executeINSERT: Table empty, creating first page.");
		targetPageIndex = 0;
		newPageCreated = true;
	}
	else
	{
		// Table has pages, check the row count of the last page using table metadata
		int lastPageIndex = table->blockCount - 1;
		// Ensure rowsPerBlockCount has an entry for the last page index
		if (lastPageIndex >= table->rowsPerBlockCount.size())
		{
			cout << "FATAL ERROR: Table metadata mismatch - blockCount inconsistent with rowsPerBlockCount size." << endl;
			logger.log("executeINSERT: ERROR - Metadata mismatch blockCount=" + to_string(table->blockCount) + " rowsPerBlockCount.size=" + to_string(table->rowsPerBlockCount.size()));
			return;
		}

		if (table->rowsPerBlockCount[lastPageIndex] >= table->maxRowsPerBlock)
		{
			// Last page is full according to metadata, need a new page
			logger.log("executeINSERT: Last page full (metadata count " + to_string(table->rowsPerBlockCount[lastPageIndex]) + "), creating new page.");
			targetPageIndex = table->blockCount; // Index for the new page will be current blockCount
			newPageCreated = true;
		}
		else
		{
			// Last page has space according to metadata
			targetPageIndex = lastPageIndex;
			logger.log("executeINSERT: Appending to existing page " + to_string(targetPageIndex));
			newPageCreated = false;
		}
	}

	// 3. Perform the Page Write
	if (newPageCreated)
	{
		// Prepare data for a new page with only the new row
		vector<vector<int>> newPageData;
		newPageData.push_back(newRow); // Start with just the new row

		bufferManager.writePage(table->tableName, targetPageIndex, newPageData, 1); // Write 1 row
		rowIndexInPage = 0;															// It's the first row (index 0) in the new page

		// Update table metadata for the new page
		table->blockCount++;
		// Ensure rowsPerBlockCount has space if needed, then update/add
		if (targetPageIndex >= table->rowsPerBlockCount.size())
		{
			table->rowsPerBlockCount.push_back(1); // Add row count for the new block
		}
		else
		{
			// This case shouldn't happen if newPageCreated is true based on prior logic
			logger.log("executeINSERT: Warning - newPageCreated true but targetPageIndex was within bounds. Overwriting rowsPerBlockCount.");
			table->rowsPerBlockCount[targetPageIndex] = 1;
		}
	}
	else
	{
		// Append to existing page (targetPageIndex is already set)
		Page page = bufferManager.getPage(table->tableName, targetPageIndex);

		// *** USE GETTER HERE ***
		int loadedRowCount = page.getRowCount();
		if (loadedRowCount < 0)
		{ // Basic check if getRowCount failed or page invalid
			cout << "FATAL ERROR: Failed to load or get row count for page " << targetPageIndex << "." << endl;
			logger.log("executeINSERT: Error loading page or getting row count for page " + to_string(targetPageIndex));
			return;
		}
		// Check consistency again (optional, but good practice)
		if (loadedRowCount >= table->maxRowsPerBlock)
		{
			cout << "INTERNAL ERROR: Metadata indicated space, but loaded page reports full." << endl;
			logger.log("executeINSERT: ERROR - Metadata/Page inconsistency for page " + to_string(targetPageIndex));
			// Maybe try creating a new page instead? For now, error out.
			return;
		}

		// Need to read all existing rows, append the new one, and write back
		vector<vector<int>> currentPageData;
		currentPageData.reserve(loadedRowCount + 1); // Reserve space

		// *** USE GETTER IN LOOP CONDITION (via loadedRowCount) ***
		for (int i = 0; i < loadedRowCount; ++i)
		{
			vector<int> currentRow = page.getRow(i);
			if (currentRow.empty() && i < loadedRowCount)
			{ // Handle potential issue where getRow returns empty unexpectedly
				cout << "FATAL ERROR: Failed to read row " << i << " from page " << targetPageIndex << "." << endl;
				logger.log("executeINSERT: Error reading row " + to_string(i) + " from page " + to_string(targetPageIndex));
				return; // Abort if page data seems corrupt
			}
			if (!currentRow.empty())
			{ // Append only if getRow returned something valid
				currentPageData.push_back(currentRow);
			}
		}

		// *** USE GETTER TO DETERMINE INDEX ***
		rowIndexInPage = loadedRowCount; // The index where the new row goes (0-based)
		currentPageData.push_back(newRow);

		bufferManager.writePage(table->tableName, targetPageIndex, currentPageData, currentPageData.size());

		// Update table metadata for the modified page
		// Ensure the index exists before accessing
		if (targetPageIndex >= table->rowsPerBlockCount.size())
		{
			cout << "FATAL ERROR: Metadata inconsistency - trying to update rowsPerBlockCount for out-of-bounds index " << targetPageIndex << endl;
			logger.log("executeINSERT: Error updating metadata for existing page - index out of bounds.");
			return;
		}
		table->rowsPerBlockCount[targetPageIndex] = currentPageData.size();
	}

	// 4. Update total row count for the table
	table->rowCount++;

	// 5. Index Maintenance
	if (table->indexed && table->index != nullptr)
	{
		logger.log("executeINSERT: Updating index for column '" + table->indexedColumn + "'");
		int indexedColIdx = table->getColumnIndex(table->indexedColumn);
		if (indexedColIdx < 0)
		{
			cout << "INTERNAL ERROR: Indexed column '" << table->indexedColumn << "' not found during INSERT execution." << endl;
			logger.log("executeINSERT: ERROR - Indexed column not found.");
		}
		else
		{
			// Ensure the row has enough columns before accessing
			if (indexedColIdx >= newRow.size())
			{
				cout << "INTERNAL ERROR: Row size mismatch when accessing indexed column." << endl;
				logger.log("executeINSERT: ERROR - Row size (" + to_string(newRow.size()) + ") too small for indexed column index (" + to_string(indexedColIdx) + ")");
			}
			else
			{
				int key = newRow[indexedColIdx];
				RecordPointer recordPointer = {targetPageIndex, rowIndexInPage};
				logger.log("executeINSERT: Calling index->insertKey(" + to_string(key) + ", {" + to_string(recordPointer.first) + "," + to_string(recordPointer.second) + "})");

				// Call the B+ Tree insert function
				if (!table->index->insertKey(key, recordPointer))
				{
					// Optional: Handle insertion failure if BTree::insertKey returns bool
					logger.log("executeINSERT: WARNING - BTree insertKey returned false.");
					// Consider if you need error recovery here. Maybe reverse the data insertion? Complex.
				}
			}
		}
	}
	else
	{
		// logger.log("executeINSERT: No index update needed (table not indexed or index object missing).");
	}

	// 6. Print Success Message
	cout << "1 row inserted into \"" << table->tableName << "\". Row Count = " << table->rowCount << endl;
}
