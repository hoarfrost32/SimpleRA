#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
	this->pageName = "";
	this->tableName = "";
	this->pageIndex = -1;
	this->rowCount = 0;
	this->columnCount = 0;
	this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName
 * @param pageIndex
 */
Page::Page(string tableName, int pageIndex)
{
	logger.log("Page::Page");
	this->tableName = tableName;
	this->pageIndex = pageIndex;
	this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);

    // Attempt to get Table metadata
    if (tableCatalogue.isTable(tableName)) // Check if it's a table
    {
        Table* table_obj_ptr = tableCatalogue.getTable(tableName); // Get pointer, NO DEREFERENCE/COPY
        if (!table_obj_ptr) { 
            logger.log("Page::Page - ERROR: Table " + tableName + " reported by catalogue but getTable returned null.");
            this->rowCount = 0; this->columnCount = 0; this->rows.clear(); return;
        }
        this->columnCount = table_obj_ptr->columnCount;

        if (this->columnCount == 0 && pageIndex >=0) { // Table exists but has no columns
             logger.log("Page::Page - Warning: Table " + tableName + " has 0 columns. Page will be empty.");
             this->rowCount = 0;
             // this->rows.clear(); // rows is already empty or will be correctly sized if maxRowsPerBlock is 0
             return;
        }


        // Validate pageIndex before accessing rowsPerBlockCount
        if (pageIndex < 0 || pageIndex >= table_obj_ptr->blockCount) {
            logger.log("Page::Page - ERROR: Invalid pageIndex " + std::to_string(pageIndex) + " for table " + tableName + " (blockCount: " + std::to_string(table_obj_ptr->blockCount) + "). Page will be empty.");
            this->rowCount = 0;
            // Ensure rows is empty or sized for 0 rows but with correct column count if needed by other parts of system
            if(this->columnCount > 0) {
                vector<int> row_template(this->columnCount, 0);
                this->rows.assign(0, row_template); // 0 rows, but columns are defined
            } else {
                this->rows.clear();
            }
            return; 
        }
        
        uint max_rows_curr_page = table_obj_ptr->maxRowsPerBlock; 
        this->rowCount = table_obj_ptr->rowsPerBlockCount[pageIndex];

        if (this->columnCount > 0) { // Only proceed if columns are defined
            vector<int> row_template(this->columnCount, 0);
            this->rows.assign(max_rows_curr_page > 0 ? max_rows_curr_page : 1, row_template); // Pre-allocate buffer, ensure at least 1 row capacity if maxRowsPerBlock is 0 but columnCount > 0

            ifstream fin(this->pageName, ios::in);
            if (!fin.is_open()) {
                logger.log("Page::Page - ERROR: Could not open page file: " + this->pageName + ". Page will be empty.");
                this->rowCount = 0; 
                return;
            }

            int number;
            for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
                if (rowCounter >= this->rows.size()) { // Defensive check against pre-allocation mismatch
                    logger.log("Page::Page - ERROR: rowCounter exceeds allocated rows size in " + this->pageName);
                    this->rowCount = rowCounter;
                    break;
                }
                for (int colCounter = 0; colCounter < this->columnCount; colCounter++) {
                    if (!(fin >> number)) {
                        logger.log("Page::Page - ERROR: File format error or premature EOF in " + this->pageName + " at row " + std::to_string(rowCounter) + ", col " + std::to_string(colCounter) + ". Read " + std::to_string(rowCounter) + " rows.");
                        this->rowCount = rowCounter; 
                        fin.close();
                        return;
                    }
                    this->rows[rowCounter][colCounter] = number;
                }
            }
            fin.close();
        } else { // columnCount is 0
             this->rowCount = 0; // No columns means no data rows
             this->rows.clear();
        }
    }
    else if (matrixCatalogue.isMatrix(tableName)) // Check if it's a matrix
    {
        Matrix* matrix_obj_ptr = matrixCatalogue.getMatrix(tableName); 
        if (!matrix_obj_ptr) {
             logger.log("Page::Page - ERROR: Matrix " + tableName + " reported by catalogue but getMatrix returned null.");
            this->rowCount = 0; this->columnCount = 0; this->rows.clear(); return;
        }
        this->columnCount = matrix_obj_ptr->dimension;

        if (this->columnCount == 0 && pageIndex >=0) {
             logger.log("Page::Page - Warning: Matrix " + tableName + " has 0 dimension. Page will be empty.");
             this->rowCount = 0;
             return;
        }

        if (pageIndex < 0 || pageIndex >= matrix_obj_ptr->blockCount) {
            logger.log("Page::Page - ERROR: Invalid pageIndex " + std::to_string(pageIndex) + " for matrix " + tableName + " (blockCount: " + std::to_string(matrix_obj_ptr->blockCount) + "). Page will be empty.");
            this->rowCount = 0;
            if(this->columnCount > 0) {
                 vector<int> row_template(this->columnCount, 0);
                 this->rows.assign(0, row_template);
            } else {
                this->rows.clear();
            }
            return;
        }

        uint max_rows_curr_page = matrix_obj_ptr->maxRowsPerBlock;
        this->rowCount = matrix_obj_ptr->rowsPerBlockCount[pageIndex];
        
        if (this->columnCount > 0) {
            vector<int> row_template(this->columnCount, 0);
            this->rows.assign(max_rows_curr_page > 0 ? max_rows_curr_page : 1, row_template);
            
            ifstream fin(this->pageName, ios::in);
            if (!fin.is_open()) { logger.log("Page::Page - ERROR: Could not open page file: " + this->pageName + ". Page will be empty."); this->rowCount = 0; return; }
            int number;
            for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
                 if (rowCounter >= this->rows.size()) { logger.log("Page::Page - ERROR: rowCounter exceeds allocated rows size in " + this->pageName); this->rowCount = rowCounter; break; }
                for (int colCounter = 0; colCounter < this->columnCount; colCounter++) {
                    if (!(fin >> number)) { logger.log("Page::Page - ERROR: File format error or premature EOF in " + this->pageName + " at row " + std::to_string(rowCounter) + ", col " + std::to_string(colCounter) + ". Read " + std::to_string(rowCounter) + " rows."); this->rowCount = rowCounter; fin.close(); return; }
                    this->rows[rowCounter][colCounter] = number;
                }
            }
            fin.close();
        } else {
            this->rowCount = 0;
            this->rows.clear();
        }
    }
    else 
    {
        logger.log("Page::Page - ERROR: Entity " + tableName + " not found in any catalogue for page loading.");
        this->rowCount = 0;
        this->columnCount = 0;
        this->rows.clear();
    }
}

/**
 * @brief Get row from page indexed by rowIndex
 *
 * @param rowIndex
 * @return vector<int>
 */
vector<int> Page::getRow(int rowIndex)
{
	logger.log("Page::getRow");
	vector<int> result;
	// result.clear(); // Not needed as it's default constructed to empty
	if (rowIndex < 0 || rowIndex >= this->rowCount || rowIndex >= this->rows.size()) // Added rowIndex < 0 and rows.size() check
		return result; // Return empty vector
	return this->rows[rowIndex];
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
	logger.log("Page::Page");
	this->tableName = tableName;
	this->pageIndex = pageIndex;
	this->rows = rows;
	this->rowCount = rowCount;

    if (this->rowCount > 0 && !this->rows.empty() && !this->rows[0].empty()) {
        this->columnCount = this->rows[0].size();
    } else {
        // If rows are empty or rowCount is 0, get columnCount from catalogue
        Table* table_obj = tableCatalogue.getTable(this->tableName);
        if (table_obj) {
            this->columnCount = table_obj->columnCount;
        } else {
            Matrix* matrix_obj = matrixCatalogue.getMatrix(this->tableName);
            if (matrix_obj) {
                this->columnCount = matrix_obj->dimension;
            } else {
                logger.log("Page::Page - Warning: Could not determine columnCount for page " + this->tableName + "_Page" + to_string(pageIndex) + " from catalogue. Setting to 0 as rows are empty/rowCount is 0.");
                this->columnCount = 0; 
                 // If rows vector was somehow provided (e.g. pre-allocated but empty data), attempt to use its structure
                 if (!this->rows.empty() && !this->rows[0].empty()) {
                     this->columnCount = this->rows[0].size();
                 }
            }
        }
    }
	this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
}

/**
 * @brief writes current page contents to file.
 *
 */
void Page::writePage()
{
	logger.log("Page::writePage");
	ofstream fout(this->pageName, ios::trunc);
	for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
	{
        if (rowCounter >= this->rows.size()) { // Defensive check
            logger.log("Page::writePage - ERROR: rowCounter out of bounds for this->rows. Skipping remaining rows.");
            break;
        }
        if (this->columnCount > 0 && this->rows[rowCounter].size() != this->columnCount) { // Defensive check
             logger.log("Page::writePage - ERROR: Mismatch between page columnCount and actual row columnCount at row " + to_string(rowCounter) + ". Skipping row.");
             continue;
        }
		for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
		{
			if (columnCounter != 0)
				fout << " ";
			fout << this->rows[rowCounter][columnCounter];
		}
		fout << endl;
	}
	fout.close();
}

int Page::getRowCount() const
{
	return this->rowCount;
}