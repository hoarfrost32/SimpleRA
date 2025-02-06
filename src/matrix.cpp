#include "global.h"
#include <fstream>
#include <sstream>

Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->matrixName = matrixName;
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->dimension = 0;
    this->blockCount = 0;
    this->maxRowsPerBlock = 0;
    this->rowsPerBlockCount.clear();
}

/**
 * @brief Loads the matrix by:
 *  1) Determining dimension n from the CSV
 *  2) Calling blockify() to split the data into blocks (page files)
 */
bool Matrix::load()
{
    logger.log("Matrix::load");

    // Step 1: find the dimension from the CSV
    if (!this->determineMatrixDimension())
        return false;

    // Step 2: write out pages to /data/temp
    return this->blockify();
}

/**
 * @brief Reads the CSV file once just to count how many rows (lines) are there.
 *        Since matrix is n x n, the dimension is simply the number of lines.
 */
bool Matrix::determineMatrixDimension()
{
    logger.log("Matrix::determineMatrixDimension");
    ifstream fin(this->sourceFileName, ios::in);
    if (!fin.is_open()) {
        return false;
	}

    int lineCount = 0;
    string line;
    while (getline(fin, line))
        lineCount++;

    fin.close();
    this->dimension = lineCount;
    // You might optionally verify each row has exactly 'dimension' columns, etc.
    // but for simplicity, we assume it's well-formed.


    return (this->dimension > 0);
}

/**
 * @brief Splits the matrix data row-by-row into blocks of size maxRowsPerBlock.
 *        Each block is written out to ../data/temp/<matrixName>_Page<i>.
 */
bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    if (!fin.is_open())
        return false;

    // We'll store entire rows (each row has 'dimension' integers) in a block,
    // just like how a table with 'dimension' columns is stored.
    this->maxRowsPerBlock =
        (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->dimension));
    // Fallback in case dimension is large or BLOCK_SIZE is small
    if (this->maxRowsPerBlock == 0)
        this->maxRowsPerBlock = 1;

    // Prepare a buffer of row vectors
    vector<int> singleRow(this->dimension, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, singleRow);

    int rowsInCurrentPage = 0;
    this->blockCount = 0;
    this->rowsPerBlockCount.clear();

    string line;
    // For each line in the CSV, parse it, store into rowsInPage
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int col = 0; col < this->dimension; col++)
        {
            string word;
            if (!getline(s, word, ','))
            {
                // Malformed row
				cout << "Malformed row" << endl;
                fin.close();
                return false;
            }

			cout << "word: " << word << endl;
            rowsInPage[rowsInCurrentPage][col] = stoi(word);
        }
        rowsInCurrentPage++;

        // If this page is full, write it out
        if (rowsInCurrentPage == (int)this->maxRowsPerBlock)
        {
            bufferManager.writePage(
                this->matrixName,
                this->blockCount,
                rowsInPage,
                rowsInCurrentPage
            );
            this->blockCount++;
            this->rowsPerBlockCount.push_back(rowsInCurrentPage);
            rowsInCurrentPage = 0;
        }
    }
    fin.close();

    // Flush any leftover rows that did not fill up the last page
    if (rowsInCurrentPage > 0)
    {
        bufferManager.writePage(
            this->matrixName,
            this->blockCount,
            rowsInPage,
            rowsInCurrentPage
        );
        this->blockCount++;
        this->rowsPerBlockCount.push_back(rowsInCurrentPage);
    }

    return true;
}

void Matrix::unload()
{
    logger.log("Matrix::unload");
    // Delete all temp pages
    for (int page = 0; page < this->blockCount; page++)
    {
        bufferManager.deleteFile(this->matrixName, page);
    }
    // Optionally remove the original CSV if not permanent, etc.
}