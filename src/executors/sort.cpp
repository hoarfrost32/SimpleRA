#include "../global.h"
#include <algorithm>
#include <queue>
#include <fstream>
#include <sstream>

/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * SORT <table-name> BY <col1>,<col2>,<col3> IN <ASC|DESC>,<ASC|DESC>,<ASC|DESC>
 *
 */

bool syntacticParseSORT() {
    logger.log("syntacticParseSORT");

    // Ensure syntactic correctness
    if (tokenizedQuery.size() < 5 || tokenizedQuery[0] != "SORT" ||
        tokenizedQuery[2] != "BY") { 
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Find where "IN"
    int inPos = -1;
    for (size_t i = 3; i < tokenizedQuery.size(); i++)
        if (tokenizedQuery[i] == "IN") {
            inPos = i;
            break;
        }

    // "IN" has to be ahead of "BY"
    if (inPos == -1 || inPos <= 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1];

    // Go over the columns...
    vector<string> columns;
    for (int i = 3; i < inPos; i++) {
        string col = tokenizedQuery[i];
        
        //... and clean each column
        if (!col.empty() && col.back() == ',')
            col = col.substr(0, col.length() - 1);

        if (col.empty() || col == ",")
            continue;
        
        // store columns in a vec
        columns.push_back(col);
    }

    // Literally the same thing as columns but for directions
    vector<string> directions;
    for (size_t i = inPos + 1; i < tokenizedQuery.size(); i++) {
        string dir = tokenizedQuery[i];

        if (!dir.empty() && dir.back() == ',')
            dir = dir.substr(0, dir.length() - 1);

        if (dir.empty() || dir == ",")
            continue;

        // Wauw wrong direction
        if (dir != "ASC" && dir != "DESC") {
            cout << "SYNTAX ERROR: Invalid sorting direction '" << dir << "'" << endl;
            return false;
        }

        directions.push_back(dir);
    }

    // num(column) == num(dir)
    if (columns.size() != directions.size()) {
        cout << "SYNTAX ERROR: Number of columns (" << columns.size()
             << ") doesn't match number of directions (" << directions.size() << ")" << endl;
        return false;
    }

    // Store column-direction pairs
    parsedQuery.sortColumns.clear();
    for (size_t i = 0; i < columns.size(); i++)
        parsedQuery.sortColumns.emplace_back(columns[i], directions[i]);

    return true;
}

bool semanticParseSORT() {
    logger.log("semanticParseSORT");

    // no table :(
    if (!tableCatalogue.isTable(parsedQuery.sortRelationName)) {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

    // no column :((
    for (auto &columnPair : parsedQuery.sortColumns)
        if (!table->isColumn(columnPair.first)) {
            cout << "SEMANTIC ERROR: Column " << columnPair.first << " doesn't exist in relation" << endl;
            return false;
        }

    return true;
}

void executeSORT() {
    logger.log("executeSORT");

    // Get table pointer
    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

    // Convert column names to indices and get sort directions
    vector<int> columnIndices;
    vector<string> sortDirections;

    for (auto &pair : parsedQuery.sortColumns) {
        columnIndices.push_back(table->getColumnIndex(pair.first));
        sortDirections.push_back(pair.second);
    }

    // buffer of 10
    const int MAX_BUFFER_BLOCKS = 10;

    // to store sorted runs
    vector<string> runs;
    int runCounter = 0;

    // get a cursor to read rows from the table
    Cursor cursor = table->getCursor();

    // max rows that will fit in buffer
    int maxRowsInMemory = MAX_BUFFER_BLOCKS * table->maxRowsPerBlock;

    // now we sort the chunks
    vector<int> row = cursor.getNext();
    while (!row.empty()){
        vector<vector<int>> memoryRows;

        while (!row.empty() && memoryRows.size() < maxRowsInMemory){
            memoryRows.push_back(row);
            row = cursor.getNext();
        }

        // no rows :(
        if (!memoryRows.size())
            continue;

        // rows! :) Now sort the rows 
        sort(memoryRows.begin(), memoryRows.end(),
             [&columnIndices, &sortDirections](const vector<int> &a, const vector<int> &b){
                 for (size_t i = 0; i < columnIndices.size(); i++){
                     int col = columnIndices[i];
                     if (a[col] != b[col])
                         return sortDirections[i] == "ASC" ? a[col] < b[col] : a[col] > b[col];
                 }
                 return false;
             });

        // store runs in a temp file
        string runName = table->tableName + "_run_" + to_string(runCounter++);
        runs.push_back(runName);

        // also store them in a temp table
        Table *runTable = new Table(runName, table->columns);
        tableCatalogue.insertTable(runTable);

        for (const auto &sortedRow : memoryRows) {
            runTable->writeRow<int>(sortedRow);
        }

        runTable->blockify();
    }

    // no runs :(
    if (runs.empty()) {
        cout << "Table " << table->tableName << " is empty or already sorted" << endl;
        return;
    }

    // mergers & acquisitions
    while (runs.size() > 1) {
        vector<string> newRuns;

        // merge MAX_BUFFER_BLOCKS-1 runs at a time (one block reserved for output)
        for (size_t i = 0; i < runs.size(); i += (MAX_BUFFER_BLOCKS - 1)) {
            // which runs to merge?
            vector<string> runsToMerge;
            for (size_t j = i; j < runs.size() && j < i + (MAX_BUFFER_BLOCKS - 1); j++)
                runsToMerge.push_back(runs[j]);

            // just go ahead if only one run
            if (runsToMerge.size() == 1) {
                newRuns.push_back(runsToMerge[0]);
                continue;
            }

            // create merged run table 
            string mergedRunName = table->tableName + "_run_" + to_string(runCounter++);
            Table *mergedRunTable = new Table(mergedRunName, table->columns);
            tableCatalogue.insertTable(mergedRunTable);

            // open cursors for all runs to merge
            vector<Cursor> cursors;
            vector<vector<int>> currentRows;

            for (const string &run : runsToMerge) {
                cursors.emplace_back(run, 0);
                currentRows.push_back(cursors.back().getNext());
            }

            // merger
            while (true) {
                // find the run with min/max row (depending on sort order)
                int minRunIndex = -1;

                for (size_t j = 0; j < currentRows.size(); j++) {
                    if (currentRows[j].empty())
                        continue;

                    if (minRunIndex == -1)
                        minRunIndex = j;

                    else {
                        bool isSmaller = true;
                        for (size_t k = 0; k < columnIndices.size(); k++) {
                            int col = columnIndices[k];
                            if (currentRows[j][col] != currentRows[minRunIndex][col]) {
                                isSmaller = sortDirections[k] == "ASC" ? currentRows[j][col] < currentRows[minRunIndex][col] : currentRows[j][col] > currentRows[minRunIndex][col];
                                break;
                            }
                        }

                        if (isSmaller)
                            minRunIndex = j;
                    }
                }

                // no more rows :(
                if (minRunIndex == -1)
                    break;

                // write min row to the merged run
                mergedRunTable->writeRow<int>(currentRows[minRunIndex]);

                // get next row from the same run
                currentRows[minRunIndex] = cursors[minRunIndex].getNext();
            }

            // blockify the merged run
            mergedRunTable->blockify();

            // add to new runs
            newRuns.push_back(mergedRunName);

            // delete the merged runs (except the one just created)
            for (const string &run : runsToMerge) {
                tableCatalogue.deleteTable(run);
            }
        }

        // Replace old runs with new runs for next pass
        runs = newRuns;
    }
    
    // clear all existing blocks
    for (int i = 0; i < table->blockCount; i++)
        bufferManager.deleteFile(table->tableName, i);

    // reset all block metadata for table
    table->blockCount = 0;
    table->rowsPerBlockCount.clear();

    // write sorted data directly to blocks
    int pageCounter = 0;
    vector<int> emptyRow(table->columnCount, 0);
    vector<vector<int>> rowsInPage(table->maxRowsPerBlock, emptyRow);

    // get cursor to the sorted run
    Cursor sortedCursor(runs[0], 0);
    vector<int> sortedRow = sortedCursor.getNext();

    while (!sortedRow.empty()) {

        for (int colCounter = 0; colCounter < table->columnCount; colCounter++)
              rowsInPage[pageCounter][colCounter] = sortedRow[colCounter];

        pageCounter++;

        // if page is full or this was the last row in the current page, write it
        if (pageCounter == table->maxRowsPerBlock || sortedCursor.pagePointer == 0) {
            bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
            table->blockCount++;
            table->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }

        sortedRow = sortedCursor.getNext();
    }

    // write any remaining rows in a final block
    if (pageCounter > 0) {
        bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
        table->blockCount++;
        table->rowsPerBlockCount.emplace_back(pageCounter);
    }

    // remove the temp sorted run
    tableCatalogue.deleteTable(runs[0]);

    cout << "Table " << table->tableName << " sorted successfully" << endl;
}