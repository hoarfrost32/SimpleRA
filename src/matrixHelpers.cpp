#include "global.h"
#include "matrixHelpers.h"

int readMatrixElement(const std::string &matrixName, int row, int col)
{
	Matrix *matrix = matrixCatalogue.getMatrix(matrixName);
	int blockIndex = row / matrix->maxRowsPerBlock;
	int offsetInBlock = row % matrix->maxRowsPerBlock;

	Page page = bufferManager.getPage(matrixName, blockIndex);
	vector<int> rowData = page.getRow(offsetInBlock);
	return rowData[col];
}

void writeMatrixElement(const std::string &matrixName, int row, int col, int val)
{
	Matrix *matrix = matrixCatalogue.getMatrix(matrixName);
	int blockIndex = row / matrix->maxRowsPerBlock;
	int offsetInBlock = row % matrix->maxRowsPerBlock;

	Page page = bufferManager.getPage(matrixName, blockIndex);
	vector<vector<int>> data(matrix->maxRowsPerBlock,
							 vector<int>(matrix->dimension, 0));
	int actualRows = matrix->rowsPerBlockCount[blockIndex];

	for (int r = 0; r < actualRows; r++)
		data[r] = page.getRow(r);

	data[offsetInBlock][col] = val;
	bufferManager.writePage(matrixName, blockIndex, data, actualRows);
}
