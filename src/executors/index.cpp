#include "../global.h"
#include "../table.h" // Make sure Table definition is included
#include "../index.h" // Make sure BTree definition is included

/**
 * @brief
 * SYNTAX: INDEX ON column_name FROM relation_name USING indexing_strategy
 * indexing_strategy: BTREE | HASH | NOTHING
 */
bool syntacticParseINDEX()
{
	logger.log("syntacticParseINDEX");
	if (tokenizedQuery.size() != 7 || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "USING")
	{
		cout << "SYNTAX ERROR: Invalid INDEX syntax." << endl;
        cout << "Expected: INDEX ON <col> FROM <relation> USING <BTREE|HASH|NOTHING>" << endl;
		return false;
	}
	parsedQuery.queryType = INDEX;
	parsedQuery.indexColumnName = tokenizedQuery[2];
	parsedQuery.indexRelationName = tokenizedQuery[4];
	string indexingStrategy = tokenizedQuery[6];
	if (indexingStrategy == "BTREE")
		parsedQuery.indexingStrategy = BTREE;
	else if (indexingStrategy == "HASH")
	    // parsedQuery.indexingStrategy = HASH; // HASH not implemented yet
        {
            cout << "SYNTAX ERROR: HASH indexing strategy not implemented." << endl;
		    return false;
        }
	else if (indexingStrategy == "NOTHING")
		parsedQuery.indexingStrategy = NOTHING;
	else
	{
		cout << "SYNTAX ERROR: Invalid indexing strategy '" << indexingStrategy << "'." << endl;
		return false;
	}
	return true;
}

bool semanticParseINDEX()
{
	logger.log("semanticParseINDEX");
	if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
	{
		cout << "SEMANTIC ERROR: Relation '" << parsedQuery.indexRelationName << "' doesn't exist." << endl;
		return false;
	}
    Table *table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    int colIndex = table->getColumnIndex(parsedQuery.indexColumnName); // Check existence and get index
	if (colIndex < 0) // getColumnIndex returns -1 if not found
	{
		// Error message already printed by getColumnIndex
		cout << "SEMANTIC ERROR: Column '" << parsedQuery.indexColumnName << "' doesn't exist in relation '" << parsedQuery.indexRelationName << "'." << endl;
		return false;
	}

    // Handle Cases:
    // 1. Trying to index (BTREE/HASH) an already indexed table.
    // 2. Trying to remove (NOTHING) index from a non-indexed table.
    // 3. Trying to create an index on a different column than the existing one.
	if (table->indexed) // Table already has an index
	{
        if (parsedQuery.indexingStrategy == NOTHING) {
             // Trying to remove the index - check if it's on the specified column
             if (table->indexedColumn != parsedQuery.indexColumnName) {
                 cout << "SEMANTIC ERROR: Table '" << parsedQuery.indexRelationName
                      << "' is indexed on column '" << table->indexedColumn
                      << "', not '" << parsedQuery.indexColumnName << "'. Cannot remove." << endl;
                 return false;
             }
             // OK to remove index on the specified column
             return true;
        } else {
            // Trying to create a new index (BTREE/HASH)
             cout << "SEMANTIC ERROR: Table '" << parsedQuery.indexRelationName
                  << "' is already indexed on column '" << table->indexedColumn << "'." << endl;
             return false;
        }
	}
    else // Table is not indexed
    {
        if (parsedQuery.indexingStrategy == NOTHING) {
            // Trying to remove index from a non-indexed table
            cout << "SEMANTIC ERROR: Table '" << parsedQuery.indexRelationName << "' is not indexed." << endl;
            return false;
        } else {
            // Trying to create a new index (BTREE/HASH) - This is allowed.
            return true;
        }
    }
}

void executeINDEX()
{
	logger.log("executeINDEX");

    Table* table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    int columnIndex = table->getColumnIndex(parsedQuery.indexColumnName); // Already validated in semantic parse

	switch (parsedQuery.indexingStrategy)
    {
        case BTREE:
            // Check if already indexed (should have been caught in semantic, but double-check)
            if (table->indexed) {
                 cout << "Error: executeINDEX called to create BTREE on already indexed table." << endl;
                 return;
            }
            cout << "Building B+ Tree index on column '" << parsedQuery.indexColumnName
                 << "' for table '" << parsedQuery.indexRelationName << "'..." << endl;

            // Create the B+ Tree object
            table->index = new BTree(table->tableName, parsedQuery.indexColumnName, columnIndex);

            // Build the index using data from the table
            if (table->index->buildIndex(table)) {
                // Update table metadata
                table->indexed = true;
                table->indexedColumn = parsedQuery.indexColumnName;
                table->indexingStrategy = BTREE;
                // Optional: Persist index info (e.g., table->index->getRootPageIndex()) if needed across sessions
                cout << "Successfully created B+ Tree index." << endl;
                // Optionally print tree stats or leaf chain for debugging
                // table->index->printTree();
                // table->index->printLeafChain();
            } else {
                 cout << "Error: Failed to build B+ Tree index." << endl;
                 // Clean up the partially created index object and files
                 if (table->index) {
                    table->index->dropIndex(); // Attempt to delete files
                    delete table->index;
                    table->index = nullptr;
                 }
            }
            break;

        case HASH:
             cout << "Error: HASH index strategy not implemented." << endl;
             // If implemented:
             // Create HashIndex object
             // Build hash index
             // Update table metadata
             break;

        case NOTHING:
             // Check if it's actually indexed (should have been caught in semantic)
             if (!table->indexed) {
                 cout << "Error: executeINDEX called to remove index from non-indexed table." << endl;
                 return;
             }
             // Check if removing the correct index column
             if (table->indexedColumn != parsedQuery.indexColumnName) {
                  cout << "Error: executeINDEX called to remove index on wrong column." << endl;
                  return;
             }

             cout << "Removing index on column '" << parsedQuery.indexColumnName
                  << "' from table '" << parsedQuery.indexRelationName << "'..." << endl;

             if (table->index != nullptr) {
                // Drop the index (deletes files)
                table->index->dropIndex();
                // Delete the BTree object
                delete table->index;
                table->index = nullptr; // Important: reset pointer
             } else {
                // Should not happen if table->indexed is true, but handle defensively
                 logger.log("executeINDEX - Warning: Table marked as indexed but index pointer was null.");
             }

             // Update table metadata
             table->indexed = false;
             table->indexedColumn = "";
             table->indexingStrategy = NOTHING;
             cout << "Successfully removed index." << endl;
             break;

        default:
             cout << "Error: Unknown indexing strategy encountered in executeINDEX." << endl;
             break;
    }
	return;
}