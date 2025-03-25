#include "../global.h"
#include <algorithm>
#include <queue>
#include <fstream>
#include <sstream>

/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 *
 * sorting_order = ASC | DESC
 */

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");

    // Syntax: SORT <table-name> BY <col1>,<col2>,<col3> IN <ASC|DESC>,<ASC|DESC>,<ASC|DESC>

    // Debugging output
    cout << "Tokenized Query in Sort.cpp: [";
    for (size_t i = 0; i < tokenizedQuery.size(); i++)
    {
        cout << "\"" << tokenizedQuery[i] << "\"";
        if (i < tokenizedQuery.size() - 1)
            cout << ", ";
    }
    cout << "]" << endl;

    // Check for minimum tokens and proper placement of "SORT", "BY"
    if (tokenizedQuery.size() < 5 || tokenizedQuery[0] != "SORT" ||
        tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Find position of "IN" token
    int inPos = -1;
    for (size_t i = 3; i < tokenizedQuery.size(); i++)
    {
        if (tokenizedQuery[i] == "IN")
        {
            inPos = i;
            break;
        }
    }

    if (inPos == -1 || inPos <= 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1]; // Table name

    // Parse columns (between "BY" and "IN")
    vector<string> columns;
    for (int i = 3; i < inPos; i++)
    {
        string col = tokenizedQuery[i];

        // Remove trailing comma if present
        if (!col.empty() && col.back() == ',')
        {
            col = col.substr(0, col.length() - 1);
        }

        // Skip empty strings and commas
        if (col.empty() || col == ",")
        {
            continue;
        }

        columns.push_back(col);
    }

    // Parse sorting directions (after "IN")
    vector<string> directions;
    for (size_t i = inPos + 1; i < tokenizedQuery.size(); i++)
    {
        string dir = tokenizedQuery[i];

        // Remove trailing comma if present
        if (!dir.empty() && dir.back() == ',')
        {
            dir = dir.substr(0, dir.length() - 1);
        }

        // Skip empty strings and commas
        if (dir.empty() || dir == ",")
        {
            continue;
        }

        if (dir != "ASC" && dir != "DESC")
        {
            cout << "SYNTAX ERROR: Invalid sorting direction '" << dir << "'" << endl;
            return false;
        }

        directions.push_back(dir);
    }

    // Debug output
    cout << "Parsed columns: [";
    for (size_t i = 0; i < columns.size(); i++)
    {
        cout << columns[i];
        if (i < columns.size() - 1)
            cout << ", ";
    }
    cout << "]" << endl;

    cout << "Parsed directions: [";
    for (size_t i = 0; i < directions.size(); i++)
    {
        cout << directions[i];
        if (i < directions.size() - 1)
            cout << ", ";
    }
    cout << "]" << endl;

    // Check if number of columns matches number of directions
    if (columns.size() != directions.size())
    {
        cout << "SYNTAX ERROR: Number of columns (" << columns.size()
             << ") doesn't match number of directions (" << directions.size() << ")" << endl;
        return false;
    }

    // Store column-direction pairs
    parsedQuery.sortColumns.clear();
    for (size_t i = 0; i < columns.size(); i++)
    {
        parsedQuery.sortColumns.emplace_back(columns[i], directions[i]);
    }

    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

    // Check that all specified columns exist in the table
    for (auto &columnPair : parsedQuery.sortColumns)
    {
        if (!table->isColumn(columnPair.first))
        {
            cout << "SEMANTIC ERROR: Column " << columnPair.first << " doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}

void executeSORT()
{
    logger.log("executeSORT");

    // Get table pointer (not reference)
    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

    // [Keep the external sort logic as is...]

    // Convert the column names to indices and get sort directions
    vector<int> columnIndices;
    vector<string> sortDirections;

    for (auto &pair : parsedQuery.sortColumns)
    {
        columnIndices.push_back(table->getColumnIndex(pair.first));
        sortDirections.push_back(pair.second);
    }

    // Implement 2-phase external merge sort with buffer constraint of 10 blocks
    const int MAX_BUFFER_BLOCKS = 10;

    // PHASE 1: Create initial sorted runs
    vector<string> runs;
    int runCounter = 0;

    // First, get a cursor to read rows from the table
    Cursor cursor = table->getCursor();

    // Calculate maximum rows per memory load
    int maxRowsInMemory = MAX_BUFFER_BLOCKS * table->maxRowsPerBlock;

    // Read and sort chunks of data
    vector<int> row = cursor.getNext();
    while (!row.empty())
    {
        // Load a chunk of rows into memory
        vector<vector<int>> memoryRows;

        while (!row.empty() && memoryRows.size() < maxRowsInMemory)
        {
            memoryRows.push_back(row);
            row = cursor.getNext();
        }

        // If we got rows, sort them and write to a temporary run file
        if (!memoryRows.size())
        {
            continue;
        }

        // Sort the rows in memory
        sort(memoryRows.begin(), memoryRows.end(),
             [&columnIndices, &sortDirections](const vector<int> &a, const vector<int> &b)
             {
                 for (size_t i = 0; i < columnIndices.size(); i++)
                 {
                     int col = columnIndices[i];
                     if (a[col] != b[col])
                     {
                         return sortDirections[i] == "ASC" ? a[col] < b[col] : a[col] > b[col];
                     }
                 }
                 return false; // Equal rows
             });

        // Create a temporary file for this run
        string runName = table->tableName + "_run_" + to_string(runCounter++);
        runs.push_back(runName);

        // Create a temporary table for this run
        Table *runTable = new Table(runName, table->columns);
        tableCatalogue.insertTable(runTable);

        // Write the sorted rows to the temporary table
        for (const auto &sortedRow : memoryRows)
        {
            runTable->writeRow<int>(sortedRow);
        }

        // Blockify the run table
        runTable->blockify();
    }

    if (runs.empty())
    {
        cout << "Table " << table->tableName << " is empty or already sorted" << endl;
        return; // This is in the 'void executeSORT()' function, so no return value is needed
    }

    // [Keep merge phase logic as is...]
    //
    while (runs.size() > 1)
    {
        vector<string> newRuns;

        // Merge MAX_BUFFER_BLOCKS-1 runs at a time (one block reserved for output)
        for (size_t i = 0; i < runs.size(); i += (MAX_BUFFER_BLOCKS - 1))
        {
            // Determine runs to merge in this pass
            vector<string> runsToMerge;
            for (size_t j = i; j < runs.size() && j < i + (MAX_BUFFER_BLOCKS - 1); j++)
            {
                runsToMerge.push_back(runs[j]);
            }

            // If only one run, just pass it to the next level
            if (runsToMerge.size() == 1)
            {
                newRuns.push_back(runsToMerge[0]);
                continue;
            }

            // Create merged run
            string mergedRunName = table->tableName + "_run_" + to_string(runCounter++);
            Table *mergedRunTable = new Table(mergedRunName, table->columns);
            tableCatalogue.insertTable(mergedRunTable);

            // Open cursors for all runs to merge
            vector<Cursor> cursors;
            vector<vector<int>> currentRows;

            for (const string &run : runsToMerge)
            {
                cursors.emplace_back(run, 0);
                currentRows.push_back(cursors.back().getNext());
            }

            // Merge the runs
            while (true)
            {
                // Find the run with the minimum/maximum row (depending on sort order)
                int minRunIndex = -1;

                for (size_t j = 0; j < currentRows.size(); j++)
                {
                    if (currentRows[j].empty())
                        continue;

                    if (minRunIndex == -1)
                    {
                        minRunIndex = j;
                    }
                    else
                    {
                        bool isSmaller = true;
                        for (size_t k = 0; k < columnIndices.size(); k++)
                        {
                            int col = columnIndices[k];
                            if (currentRows[j][col] != currentRows[minRunIndex][col])
                            {
                                isSmaller = sortDirections[k] == "ASC" ? currentRows[j][col] < currentRows[minRunIndex][col] : currentRows[j][col] > currentRows[minRunIndex][col];
                                break;
                            }
                        }

                        if (isSmaller)
                        {
                            minRunIndex = j;
                        }
                    }
                }

                // If no more rows, break
                if (minRunIndex == -1)
                {
                    break;
                }

                // Write the minimum row to the merged run
                mergedRunTable->writeRow<int>(currentRows[minRunIndex]);

                // Get next row from the same run
                currentRows[minRunIndex] = cursors[minRunIndex].getNext();
            }

            // Blockify the merged run
            mergedRunTable->blockify();

            // Add to new runs
            newRuns.push_back(mergedRunName);

            // Delete the merged runs (except the one just created)
            for (const string &run : runsToMerge)
            {
                tableCatalogue.deleteTable(run);
            }
        }

        // Replace old runs with new runs for next pass
        runs = newRuns;
    }

    // 1. Clear all existing blocks
    for (int i = 0; i < table->blockCount; i++)
    {
        bufferManager.deleteFile(table->tableName, i);
    }

    // 2. Reset table's block-related metadata
    table->blockCount = 0;
    table->rowsPerBlockCount.clear();

    // 3. Write sorted data directly to blocks
    int pageCounter = 0;
    // Use a different name to avoid redeclaration
    vector<int> emptyRow(table->columnCount, 0);
    vector<vector<int>> rowsInPage(table->maxRowsPerBlock, emptyRow);

    // Get cursor to the sorted run
    Cursor sortedCursor(runs[0], 0);
    vector<int> sortedRow = sortedCursor.getNext();

    while (!sortedRow.empty())
    {
        // Copy data to current page buffer
        for (int colCounter = 0; colCounter < table->columnCount; colCounter++)
        {
            rowsInPage[pageCounter][colCounter] = sortedRow[colCounter];
        }
        pageCounter++;

        // If page is full or this was the last row in the current page, write it
        // Fixed: use sortedCursor's current state to determine if we need a new page
        if (pageCounter == table->maxRowsPerBlock || sortedCursor.pagePointer == 0)
        {
            bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
            table->blockCount++;
            table->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }

        sortedRow = sortedCursor.getNext();
    }

    // Write any remaining rows in a final block
    if (pageCounter > 0)
    {
        bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
        table->blockCount++;
        table->rowsPerBlockCount.emplace_back(pageCounter);
    }

    // Remove the temporary sorted run
    tableCatalogue.deleteTable(runs[0]);

    cout << "Table " << table->tableName << " sorted successfully" << endl;
}