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
	if (!fin.is_open())
	{
		return false;
	}

	int lineCount = 0;
	string line;
	while (getline(fin, line))
		lineCount++;

	fin.close();
	this->dimension = lineCount;

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

			rowsInPage[rowsInCurrentPage][col] = stoi(word);
		}
		rowsInCurrentPage++;

		if (rowsInCurrentPage == (int)this->maxRowsPerBlock)
		{
			bufferManager.writePage(
				this->matrixName,
				this->blockCount,
				rowsInPage,
				rowsInCurrentPage);
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
			rowsInCurrentPage);
		this->blockCount++;
		this->rowsPerBlockCount.push_back(rowsInCurrentPage);
	}

	return true;
}

void Matrix::unload()
{
	logger.log("Matrix::unload");
	for (int page = 0; page < this->blockCount; page++)
	{
		bufferManager.deleteFile(this->matrixName, page);
	}
}

void Matrix::print()
{
	logger.log("Matrix::print");
	int limit = min(dimension, 20); // dont print more than 20 rows

	int rowsPrinted = 0;
	for (int blockIndex = 0; blockIndex < this->blockCount && rowsPrinted < limit; blockIndex++)
	{
		int rowsInThisBlock = this->rowsPerBlockCount[blockIndex];

		Page page = bufferManager.getPage(this->matrixName, blockIndex);

		for (int r = 0; r < rowsInThisBlock && rowsPrinted < limit; r++)
		{
			vector<int> rowData = page.getRow(r);
			for (int c = 0; c < limit; c++)
			{
				cout << rowData[c];
				if (c < limit - 1)
					cout << " ";
			}
			cout << endl;
			rowsPrinted++;
		}
	}

	cout << "Matrix dimension: " << dimension << " x " << dimension << endl;
}

void Matrix::makePermanent()
{
	logger.log("Matrix::makePermanent");
	string newSourceFile = "../data/" + this->matrixName + ".csv";
	ofstream fout(newSourceFile, ios::out);
	if (!fout.is_open())
	{
		cout << "Error opening file for matrix export." << endl;
		return;
	}

	int totalBlocks = this->blockCount;
	for (int blockIndex = 0; blockIndex < totalBlocks; blockIndex++)
	{
		Page page = bufferManager.getPage(this->matrixName, blockIndex);
		int rowsInThisBlock = this->rowsPerBlockCount[blockIndex];

		for (int r = 0; r < rowsInThisBlock; r++)
		{
			vector<int> rowData = page.getRow(r);
			for (int c = 0; c < this->dimension; c++)
			{
				fout << rowData[c];
				if (c < this->dimension - 1)
					fout << ",";
			}
			fout << "\n";
		}
	}

	fout.close();
}
