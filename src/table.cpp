#include "global.h"
#include "table.h" // Ensure table.h is included
#include "index.h" // Ensure index.h is included for BTree definition

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
	logger.log("Table::Table");
    // Ensure index pointer is null initially
    this->index = nullptr;
    this->indexed = false;
    this->indexingStrategy = NOTHING;
    this->indexedColumn = "";
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName
 */
Table::Table(string tableName)
{
	logger.log("Table::Table");
	this->sourceFileName = "../data/" + tableName + ".csv";
	this->tableName = tableName;
    // Ensure index pointer is null initially
    this->index = nullptr;
    this->indexed = false;
    this->indexingStrategy = NOTHING;
    this->indexedColumn = "";
    // TODO: Add logic here or after load() to check if a persisted index
    // for this table exists and load it. For now, assumes no persistence.
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName
 * @param columns
 */
Table::Table(string tableName, vector<string> columns)
{
	logger.log("Table::Table");
	this->sourceFileName = "../data/temp/" + tableName + ".csv";
	this->tableName = tableName;
	this->columns = columns;
	this->columnCount = columns.size();
    // Calculate maxRowsPerBlock *before* writing header row if columnCount is known
	if (this->columnCount > 0) {
	    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
        if (this->maxRowsPerBlock == 0) this->maxRowsPerBlock = 1; // Ensure at least 1 row per block
    } else {
        this->maxRowsPerBlock = 0; // Or some default?
    }
	this->writeRow<string>(columns); // Write header row to the temp file
    // Ensure index pointer is null initially
    this->index = nullptr;
    this->indexed = false;
    this->indexingStrategy = NOTHING;
    this->indexedColumn = "";
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded
 * @return false if an error occurred
 */
bool Table::load()
{
	logger.log("Table::load");
	fstream fin(this->sourceFileName, ios::in);
	string line;
	if (getline(fin, line))
	{
		fin.close();
		if (this->extractColumnNames(line)) // This sets columnCount and maxRowsPerBlock
			if (this->blockify()) // This reads data and creates pages
            {
                 // TODO: After successful load, check for and load persisted index if necessary.
                return true;
            }

	}
	fin.close();
	return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file.
 *
 * @param line
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
	logger.log("Table::extractColumnNames");
	unordered_set<string> columnNames;
	string word;
	stringstream s(firstLine);
	this->columns.clear(); // Clear previous columns if any
	this->columnCount = 0; // Reset count

	while (getline(s, word, ','))
	{
        // Trim leading/trailing whitespace
		word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        // Remove potential quotes if CSV uses them (basic handling)
        if (!word.empty() && word.front() == '"' && word.back() == '"') {
            word = word.substr(1, word.length() - 2);
        }

		if (columnNames.count(word)) {
            cout << "SEMANTIC ERROR: Duplicate column name '" << word << "' found in header." << endl;
			return false;
        }
		columnNames.insert(word);
		this->columns.emplace_back(word);
	}
	this->columnCount = this->columns.size();
	if (this->columnCount == 0) {
        cout << "SEMANTIC ERROR: No columns found in header." << endl;
        return false; // No columns is an error
    }
	this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
	if (this->maxRowsPerBlock == 0) this->maxRowsPerBlock = 1; // Ensure at least 1 row per block
	return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
	logger.log("Table::blockify");
	ifstream fin(this->sourceFileName, ios::in);
    if (!fin.is_open()) {
        logger.log("Table::blockify - ERROR: Could not open source file: " + this->sourceFileName);
        return false;
    }

    // Reset state before blockifying
    this->blockCount = 0;
    this->rowCount = 0;
    this->rowsPerBlockCount.clear();
    // Keep distinct value stats or clear them? Let's clear and rebuild.
	if (this->columnCount > 0) {
        this->distinctValuesInColumns.assign(this->columnCount, unordered_set<int>());
	    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    } else {
         logger.log("Table::blockify - ERROR: Column count is zero.");
         fin.close();
         return false;
    }


	string line, word;
	vector<int> row(this->columnCount, 0);
	vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row); // Pre-allocate page buffer
	int pageCounter = 0;

	getline(fin, line); // Skip header line

	while (getline(fin, line))
	{
        if (line.empty() || std::all_of(line.begin(), line.end(), ::isspace)) {
             continue; // Skip empty lines
        }

		stringstream s(line);
		for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
		{
			if (!getline(s, word, ',')) {
                 logger.log("Table::blockify - ERROR: Row has fewer columns than expected. Line: " + line);
                 // Decide how to handle: return false, skip row, pad with 0?
                 // Returning false seems safest for now.
                 fin.close();
				 return false;
            }
            try {
                // Trim whitespace before converting
                word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
			    row[columnCounter] = stoi(word);
			    rowsInPage[pageCounter][columnCounter] = row[columnCounter];
            } catch (const std::invalid_argument& ia) {
                 logger.log("Table::blockify - ERROR: Invalid integer value '" + word + "' in line: " + line);
                 fin.close();
                 return false;
            } catch (const std::out_of_range& oor) {
                 logger.log("Table::blockify - ERROR: Integer value out of range '" + word + "' in line: " + line);
                 fin.close();
                 return false;
            }
		}
        // Check if there are extra columns in the line
        if (getline(s, word, ',')) {
             logger.log("Table::blockify - ERROR: Row has more columns than expected. Line: " + line);
             fin.close();
             return false;
        }


		pageCounter++;
		this->updateStatistics(row); // Updates rowCount and distinct counts
		if (pageCounter == this->maxRowsPerBlock)
		{
			bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
			this->blockCount++;
			this->rowsPerBlockCount.emplace_back(pageCounter);
			pageCounter = 0;
            // Optionally clear rowsInPage here if concerned about leftover data, but overwriting should be fine.
            // rowsInPage.assign(this->maxRowsPerBlock, vector<int>(this->columnCount, 0));
		}
	}
	fin.close(); // Close the file stream

	if (pageCounter > 0) // Write the last partially filled page
	{
		bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
		this->blockCount++;
		this->rowsPerBlockCount.emplace_back(pageCounter);
	}

	if (this->rowCount == 0) {
        logger.log("Table::blockify - Warning: Table is empty after blockifying.");
		// It's not necessarily an error for a table to be empty, so return true.
    }

    // Clear the large distinct value sets now that counts are computed
	this->distinctValuesInColumns.clear();
    this->distinctValuesInColumns.shrink_to_fit(); // Release memory
	return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row
 */
void Table::updateStatistics(vector<int> row)
{
    // This check should ideally happen before calling updateStatistics
	if (row.size() != this->columnCount) {
        logger.log("Table::updateStatistics - ERROR: Row size mismatch.");
        return; // Avoid processing mismatched rows
    }

	this->rowCount++;
    // Only update distinct counts if the structure is still allocated (it's cleared after blockify)
    if (!this->distinctValuesInColumns.empty() && this->distinctValuesInColumns.size() == this->columnCount) {
	    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
	    {
		    if (this->distinctValuesInColumns[columnCounter].find(row[columnCounter]) == this->distinctValuesInColumns[columnCounter].end())
		    {
			    this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
			    this->distinctValuesPerColumnCount[columnCounter]++;
		    }
	    }
    } else if (this->distinctValuesPerColumnCount.size() != this->columnCount) {
         // If counts vector isn't ready, maybe initialize or log error
         // Let's assume distinctValuesPerColumnCount was initialized in blockify/constructor
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName
 * @return true
 * @return false
 */
bool Table::isColumn(string columnName)
{
	// logger.log("Table::isColumn"); // Can be very verbose
	for (auto const& col : this->columns) // Use const& for efficiency
	{
		if (col == columnName)
		{
			return true;
		}
	}
	return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName
 * @param toColumnName
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
	logger.log("Table::renameColumn");
	bool found = false;
	for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
	{
		if (columns[columnCounter] == fromColumnName)
		{
			columns[columnCounter] = toColumnName;
            found = true;
			break; // Assuming column names are unique
		}
	}
    if (found && this->indexed && this->indexedColumn == fromColumnName) {
        this->indexedColumn = toColumnName;
        // Note: Index object itself doesn't store column name, just uses it for file naming.
        // If file naming depends on the column name stored here, this could be an issue.
        // Current BTree uses tableName + columnName for indexName, so renaming column
        // *after* index creation might break index loading/finding logic if not handled.
        logger.log("Table::renameColumn - Warning: Renamed an indexed column. Index file names might be based on the old name.");
    }
	return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
	logger.log("Table::print");
	if (this->columnCount == 0 || this->blockCount == 0) {
        cout << "Table " << this->tableName << " is empty or has no columns/blocks." << endl;
        return;
    }
	uint count = min((long long)PRINT_COUNT, this->rowCount);

	// print headings
	this->writeRow(this->columns, cout);

	Cursor cursor(this->tableName, 0);
	vector<int> row;
	for (uint rowCounter = 0; rowCounter < count; rowCounter++) // Use uint for consistency
	{
		row = cursor.getNext();
        if (row.empty()) break; // Stop if we run out of rows unexpectedly
		this->writeRow(row, cout);
	}
	printRowCount(this->rowCount);
}

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 * NOTE: This function was misnamed/misused. It's intended to advance the
 * cursor to the next page, not get a row. The actual row getting is in
 * Cursor::getNext(). Let's fix the implementation to match the name's intent.
 *
 * @param cursor
 * @return vector<int>
 */
void Table::getNextPage(Cursor *cursor)
{
	logger.log("Table::getNextPage"); // Corrected log message

	if (!cursor) {
        logger.log("Table::getNextPage - Error: Null cursor provided.");
        return;
    }

    // Check if the current page index is valid and not the last page
	if (cursor->pageIndex >= 0 && cursor->pageIndex < this->blockCount - 1)
	{
        // Tell the cursor to load the next page
		cursor->nextPage(cursor->pageIndex + 1);
	} else {
        // No next page exists or invalid state
        // Cursor::nextPage handles loading, maybe just log here or do nothing.
         logger.log("Table::getNextPage - Cursor is already at the last page or in an invalid state.");
         // We could invalidate the cursor here, but nextPage handles loading.
         // The cursor's getNext() method will naturally return empty if no more rows.
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
	logger.log("Table::makePermanent");
    string currentSource = this->sourceFileName; // Keep track of the current source
    bool wasInMemoryOnly = !this->isPermanent(); // Check if it was temporary

	// Define the target permanent file path
    string newSourceFile = "../data/" + this->tableName + ".csv";

    // If the file already exists at the target location, we might just update the sourceFileName.
    // However, the current implementation reads from temp pages, so we always rewrite.
    // Let's ensure we don't try to delete the target if it's the same as source.
    // if (wasInMemoryOnly && currentSource != newSourceFile) {
	//     bufferManager.deleteFile(currentSource); // Delete old temp CSV if different
    // } else if (wasInMemoryOnly) {
    //      logger.log("Table::makePermanent - Temp source file seems to be the same as permanent? " + currentSource);
    // } // If it was already permanent, we just overwrite it below.


	ofstream fout(newSourceFile, ios::trunc); // Open in trunc mode to overwrite
    if (!fout.is_open()) {
         logger.log("Table::makePermanent - ERROR: Could not open permanent file for writing: " + newSourceFile);
         return; // Exit if cannot open file
    }


	// print headings
    if (!this->columns.empty()) {
	    this->writeRow(this->columns, fout);
    } else {
         logger.log("Table::makePermanent - Warning: Table has no columns defined.");
    }

	// Read from pages and write to the new file
    if (this->blockCount > 0) {
	    Cursor cursor(this->tableName, 0);
	    vector<int> row;
	    // Iterate exactly rowCount times if rowCount is accurate
	    for (long long rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
	    {
		    row = cursor.getNext();
            if (row.empty()) {
                logger.log("Table::makePermanent - Warning: Cursor returned empty row before reaching rowCount. Actual rows: " + std::to_string(rowCounter));
                break; // Stop if we run out of data
            }
		    this->writeRow(row, fout);
	    }
    } else {
        logger.log("Table::makePermanent - Warning: Table has no blocks to write.");
    }

	fout.close();

    // Update the source file name to the permanent location
    this->sourceFileName = newSourceFile;
    logger.log("Table::makePermanent - Table data written to permanent file: " + this->sourceFileName);

    // Now, delete the temporary page files if they existed
    if (wasInMemoryOnly) {
         logger.log("Table::makePermanent - Deleting temporary page files for: " + this->tableName);
         for (uint i = 0; i < this->blockCount; ++i) {
              bufferManager.deleteFile(this->tableName, i); // Deletes ../data/temp/<tableName>_Page<i>
         }
         // Also delete the old temp CSV file if it wasn't the same as the target
         if (currentSource != this->sourceFileName && currentSource.find("../data/temp/") != string::npos) {
             bufferManager.deleteFile(currentSource);
         }
    }
     // TODO: Decide if the index associated with this table should also be made permanent.
     // This would involve saving the BTree structure (root page index, node count, order etc.)
     // and potentially moving the BTree node pages from temp to data.
}

/**
 * @brief Function to check if table is already exported (i.e., its source points to ../data/)
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
	// logger.log("Table::isPermanent"); // Can be verbose
    // Check if the sourceFileName starts with "../data/" and not "../data/temp/"
	if (this->sourceFileName.rfind("../data/", 0) == 0 &&
        this->sourceFileName.rfind("../data/temp/", 0) != 0) {
		return true;
    }
	return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table, including page files
 * and the temporary source CSV (if not permanent). It also deletes the
 * associated index object if one exists.
 *
 */
void Table::unload()
{
	logger.log("Table::unload - Unloading table: " + this->tableName);

    // Delete the B+ Tree index object and its files if it exists
    if (this->index != nullptr) {
        logger.log("Table::unload - Dropping associated index for column: " + this->indexedColumn);
        this->index->dropIndex(); // Deletes index node pages
        delete this->index;       // Deletes the BTree object itself
        this->index = nullptr;
        this->indexed = false;
        this->indexedColumn = "";
        this->indexingStrategy = NOTHING;
    }

    // Delete the table's data page files from the temp directory
	for (uint pageCounter = 0; pageCounter < this->blockCount; pageCounter++) { // Use uint for consistency
		bufferManager.deleteFile(this->tableName, pageCounter); // Deletes ../data/temp/<tableName>_Page<i>
    }

    // Delete the source CSV file ONLY if it's temporary (in ../data/temp/)
	if (!isPermanent() && !this->sourceFileName.empty()) {
         logger.log("Table::unload - Deleting temporary source file: " + this->sourceFileName);
		 bufferManager.deleteFile(this->sourceFileName);
    } else {
         logger.log("Table::unload - Keeping permanent source file: " + this->sourceFileName);
    }

    // Reset table state (optional, as object might be deleted soon after)
    // this->columns.clear();
    // this->rowsPerBlockCount.clear();
    // this->distinctValuesPerColumnCount.clear();
    // this->rowCount = 0;
    // this->blockCount = 0;
    // this->columnCount = 0;

}

/**
 * @brief Function that returns a cursor that reads rows from this table
 *
 * @return Cursor
 */
Cursor Table::getCursor()
{
	logger.log("Table::getCursor");
    // Check if table has blocks before creating cursor
    if (this->blockCount == 0) {
         logger.log("Table::getCursor - Warning: Table has no blocks. Returning cursor starting at page 0 (will likely return empty).");
         // Allow creating cursor, but getNext() will likely fail gracefully
    }
	Cursor cursor(this->tableName, 0); // Always start cursor at page 0
	return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 *
 * @param columnName
 * @return int column index, or -1 if not found
 */
int Table::getColumnIndex(string columnName)
{
	// logger.log("Table::getColumnIndex"); // Verbose
	for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
	{
		if (this->columns[columnCounter] == columnName)
			return columnCounter;
	}
    // Return -1 or throw an exception if column not found?
    // Returning -1 allows callers to check.
     logger.log("Table::getColumnIndex - Warning: Column '" + columnName + "' not found in table '" + this->tableName + "'.");
    return -1;
}

/**
 * @brief Re‑reads the CSV (sourceFileName) and rebuilds all page files +
 *        statistics. Assumes the CSV already contains the latest data.
 *        Also assumes the index (if any) will be rebuilt separately if needed.
 */
bool Table::reload()
{
	logger.log("Table::reload - Reloading table from: " + this->sourceFileName);

	/* Remove old page files */
    // Don't call unload() as that deletes the index object and potentially the source CSV.
    // Just delete the old pages.
	for (uint i = 0; i < this->blockCount; ++i) {
        bufferManager.deleteFile(this->tableName, i); // Deletes ../data/temp/<name>_Page<i>
    }

	/* Reset in‑memory statistics related to pages and rows */
	this->rowCount = 0;
	this->blockCount = 0;
	this->rowsPerBlockCount.clear();
	this->distinctValuesPerColumnCount.clear(); // Will be rebuilt by blockify
	this->distinctValuesInColumns.clear();    // Will be rebuilt by blockify

    // Keep column definitions (columns, columnCount, maxRowsPerBlock) as they shouldn't change.
    // Keep index information (indexed, indexedColumn, indexingStrategy, index pointer)
    // NOTE: The pointers stored in the index *might* become invalid after reload.
    // This is the core problem if using physical pointers with reload().

	/* blockify() will re-read the sourceFileName and create new pages */
	bool success = this->blockify();
    if (success) {
        logger.log("Table::reload - Successfully reloaded and blockified.");
        // WARNING: If an index exists, its pointers are now likely invalid!
        if (this->indexed) {
             logger.log("Table::reload - WARNING: Table was reloaded, but an index exists. Index pointers are likely invalid and need rebuilding.");
             // Optionally, automatically drop/invalidate the index here?
             // Or rely on the user/system to rebuild it?
             // Dropping seems safer if reload is used carelessly.
             // if (this->index) {
             //     this->index->dropIndex();
             //     delete this->index;
             //     this->index = nullptr;
             // }
             // this->indexed = false;
             // this->indexedColumn = "";
             // this->indexingStrategy = NOTHING;
             // logger.log("Table::reload - Index for column '" + this->indexedColumn + "' has been invalidated due to reload.");
        }
    } else {
         logger.log("Table::reload - Failed to blockify during reload.");
    }
    return success;
}