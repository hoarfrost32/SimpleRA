#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{
	logger.log("Cursor::Cursor");
	this->page = bufferManager.getPage(tableName, pageIndex);
	this->pagePointer = 0;
	this->tableName = tableName;
	this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getNext()
{
    logger.log("Cursor::geNext");
    
    // Loop to find the next valid row
    while (true) {
        // Attempt to get a row from the current page
        if (this->pagePointer < this->page.getRowCount()) {
            vector<int> result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
            if (!result.empty()) {
                logger.log("Cursor::geNext - Successfully fetched row from page " + to_string(this->pageIndex) + " at row index " + to_string(this->pagePointer -1));
                return result; // Found a valid row
            } else {
                 // This case (empty row within supposedly valid range) might indicate data corruption or an issue in Page::getRow or Page loading
                 logger.log("Cursor::geNext - WARNING: page.getRow returned empty for a supposedly valid pointer. Page: " + to_string(this->pageIndex) + ", Pointer: " + to_string(this->pagePointer-1) + ", PageRowCount: " + to_string(this->page.getRowCount()));
                 // Continue to try advancing page, as this row is effectively invalid
            }
        }

        // Current page is exhausted or was empty to begin with, move to the next page
        Table* table = tableCatalogue.getTable(this->tableName);
        if (!table) {
            logger.log("Cursor::geNext - ERROR: Table " + this->tableName + " not found in catalogue.");
            return {}; // Return empty vector
        }

        // Check if there is a next page in the table's metadata
        if (this->pageIndex < table->blockCount - 1) {
            logger.log("Cursor::geNext - Current page " + to_string(this->pageIndex) + " exhausted. Attempting to load next page.");
            this->nextPage(this->pageIndex + 1); // nextPage resets pagePointer to 0
            // Loop will continue, and the next iteration will try to getRow from the new page
        } else {
            // No more pages in the table
            logger.log("Cursor::geNext - No more pages in table " + this->tableName + ". End of cursor.");
            return {}; // Return empty vector, indicating end of table
        }
    }
}
/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex
 */
void Cursor::nextPage(int pageIndex)
{
	logger.log("Cursor::nextPage for page index " + to_string(pageIndex)); // Added specific page index
	this->page = bufferManager.getPage(this->tableName, pageIndex);
    logger.log("Cursor::nextPage - Loaded page " + to_string(pageIndex) + " for table " + this->tableName + ". New page.rowCount: " + to_string(this->page.getRowCount())); // Added log for rowCount
	this->pageIndex = pageIndex;
	this->pagePointer = 0;
}