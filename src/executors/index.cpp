#include "../global.h"
#include "../table.h" // Make sure Table definition is included
#include "../index.h" // Make sure BTree definition is included
#include <memory>

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

    // Check if an index already exists *on this specific column*
    bool indexExistsOnColumn = table->isIndexed(parsedQuery.indexColumnName);

    if (parsedQuery.indexingStrategy == NOTHING) {
        // Trying to remove index
        if (!indexExistsOnColumn) {
            cout << "SEMANTIC ERROR: No index exists on column '" << parsedQuery.indexColumnName
                 << "' in table '" << parsedQuery.indexRelationName << "' to remove." << endl;
            return false;
        }
        // OK to remove existing index on this column
        return true;
    } else {
        // Trying to create index (BTREE or HASH)
        if (indexExistsOnColumn) {
            cout << "SEMANTIC ERROR: An index already exists on column '" << parsedQuery.indexColumnName
                 << "' in table '" << parsedQuery.indexRelationName << "'." << endl;
            return false;
        }
        // OK to create index on this column
        return true;
    }
}

void executeINDEX()
{
    logger.log("executeINDEX");

    Table* table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    if (!table) {
        // Semantic parse should prevent this, but check anyway
        cout << "FATAL ERROR: Table '" << parsedQuery.indexRelationName << "' not found during execution." << endl;
        return;
    }
    int columnIndex = table->getColumnIndex(parsedQuery.indexColumnName);
    if (columnIndex < 0) {
        // Semantic parse should prevent this
        cout << "FATAL ERROR: Column '" << parsedQuery.indexColumnName << "' not found during execution." << endl;
        return;
    }

    // std::unique_ptr<BTree> newIndex; // OLD
    BTree* newIndexPtr = nullptr; // NEW: Use raw pointer, initialize to nullptr

    switch (parsedQuery.indexingStrategy)
    {
        case BTREE:
            if (table->isIndexed(parsedQuery.indexColumnName)) {
                 cout << "Error: executeINDEX called to create BTREE on already indexed column '" << parsedQuery.indexColumnName << "'." << endl;
                 return;
            }
            cout << "Building B+ Tree index on column '" << parsedQuery.indexColumnName
                 << "' for table '" << parsedQuery.indexRelationName << "'..." << endl;

            // Create the B+ Tree object using new
            // newIndex = std::make_unique<BTree>(table->tableName, parsedQuery.indexColumnName, columnIndex); // OLD
            newIndexPtr = new BTree(table->tableName, parsedQuery.indexColumnName, columnIndex); // NEW

            // Build the index using data from the table
            if (newIndexPtr->buildIndex(table)) {
                // Add the successfully built index to the table's map
                // if (table->addIndex(parsedQuery.indexColumnName, std::move(newIndex))) { // OLD
                if (table->addIndex(parsedQuery.indexColumnName, newIndexPtr)) { // NEW: Pass raw pointer
                    cout << "Successfully created B+ Tree index on column '" << parsedQuery.indexColumnName << "'." << endl;
                    // newIndexPtr is now owned by the table, do not delete here.
                } else {
                    // This should ideally not happen if semantic check passed
                    cout << "Error: Failed to add the created index to the table's metadata (maybe already exists?)." << endl;
                    // Table did not take ownership, so we need to clean up.
                    if(newIndexPtr) {
                        newIndexPtr->dropIndex(); // Attempt to clean up files
                        delete newIndexPtr;       // Delete the object
                    }
                }
            } else {
                 cout << "Error: Failed to build B+ Tree index for column '" << parsedQuery.indexColumnName << "'." << endl;
                 // buildIndex failed, clean up the allocated object
                 if(newIndexPtr) {
                    // dropIndex might have been called internally by buildIndex on failure,
                    // but call again just in case. It should be safe to call multiple times.
                    newIndexPtr->dropIndex();
                    delete newIndexPtr; // Delete the object
                 }
            }
            break;

        case HASH:
             cout << "Error: HASH index strategy not implemented." << endl;
             break;

        case NOTHING: // This corresponds to removing an index
             if (!table->isIndexed(parsedQuery.indexColumnName)) {
                 cout << "Error: executeINDEX called to remove index from non-indexed column '" << parsedQuery.indexColumnName << "'." << endl;
                 return; // Should be caught by semantic parse
             }

             cout << "Removing index on column '" << parsedQuery.indexColumnName
                  << "' from table '" << parsedQuery.indexRelationName << "'..." << endl;

             // Remove the index using the table's method (which now handles deletion)
             if (table->removeIndex(parsedQuery.indexColumnName)) {
                 cout << "Successfully removed index from column '" << parsedQuery.indexColumnName << "'." << endl;
             } else {
                 // Should not happen if semantic check passed
                 cout << "Error: Failed to remove index from column '" << parsedQuery.indexColumnName << "' (not found?)." << endl;
             }
             break;

        default:
             cout << "Error: Unknown indexing strategy encountered in executeINDEX." << endl;
             break;
    }
    return;
}