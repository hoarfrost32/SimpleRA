#include "../global.h"

bool syntacticParseROTATEMATRIX()
{
	logger.log("syntacticParseROTATEMATRIX");
	// Expect exactly 2 tokens:  ROTATE <matrixName>
	if (tokenizedQuery.size() != 2)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.queryType = ROTATEMATRIX;
	parsedQuery.rotateMatrixName = tokenizedQuery[1];
	return true;
}

bool semanticParseROTATEMATRIX()
{
	logger.log("semanticParseROTATEMATRIX");
	// The matrix must already exist in the matrixCatalogue:
	if (!matrixCatalogue.isMatrix(parsedQuery.rotateMatrixName))
	{
		cout << "SEMANTIC ERROR: No such matrix loaded" << endl;
		return false;
	}
	return true;
}

// A small helper to read one element from matrix storage
int readMatrixElement(const string &matrixName, int row, int col)
{
	Matrix *matrix = matrixCatalogue.getMatrix(matrixName);

	// figure out which page holds "row"
	int blockIndex = row / matrix->maxRowsPerBlock;
	int offsetInBlock = row % matrix->maxRowsPerBlock;

	// fetch the page from buffer
	Page page = bufferManager.getPage(matrixName, blockIndex);

	// get that row from the page
	vector<int> rowData = page.getRow(offsetInBlock);

	return rowData[col]; // one integer
}

// A helper to write one element (val) into (row,col)
void writeMatrixElement(const string &matrixName, int row, int col, int val)
{
	Matrix *matrix = matrixCatalogue.getMatrix(matrixName);
	int blockIndex = row / matrix->maxRowsPerBlock;
	int offsetInBlock = row % matrix->maxRowsPerBlock;

	// get page from buffer
	Page page = bufferManager.getPage(matrixName, blockIndex);

	// read out all rows from the page so we can modify
	vector<vector<int>> data(matrix->maxRowsPerBlock,
							 vector<int>(matrix->dimension, 0));

	int actualRows = matrix->rowsPerBlockCount[blockIndex];
	for (int r = 0; r < actualRows; r++)
		data[r] = page.getRow(r);

	// now update the single cell
	data[offsetInBlock][col] = val;

	// rewrite the page to disk
	bufferManager.writePage(matrixName, blockIndex, data, actualRows);
}

void executeROTATEMATRIX()
{
	logger.log("executeROTATEMATRIX");
	// get the matrix object
	Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.rotateMatrixName);
	int n = matrix->dimension;

	// We rotate layer by layer
	for (int layer = 0; layer < n / 2; layer++)
	{
		// from 'layer' up to 'n - layer - 1'
		for (int i = layer; i < n - layer - 1; i++)
		{
			// Indices of the 4 corners to swap
			int topRow = layer, topCol = i;
			int rightRow = i, rightCol = n - 1 - layer;
			int bottomRow = n - 1 - layer, bottomCol = n - 1 - i;
			int leftRow = n - 1 - i, leftCol = layer;

			// read the four corners
			int topVal = readMatrixElement(matrix->matrixName, topRow, topCol);
			int rightVal = readMatrixElement(matrix->matrixName, rightRow, rightCol);
			int bottomVal = readMatrixElement(matrix->matrixName, bottomRow, bottomCol);
			int leftVal = readMatrixElement(matrix->matrixName, leftRow, leftCol);

			// do the 4-way rotation:
			// top -> right
			writeMatrixElement(matrix->matrixName, rightRow, rightCol, topVal);
			// right -> bottom
			writeMatrixElement(matrix->matrixName, bottomRow, bottomCol, rightVal);
			// bottom -> left
			writeMatrixElement(matrix->matrixName, leftRow, leftCol, bottomVal);
			// left -> top
			writeMatrixElement(matrix->matrixName, topRow, topCol, leftVal);
		}
	}

	cout << "Matrix " << matrix->matrixName << ":" << endl;
	for (int i = 0; i < n; i++)
	{
		for (int j = 0; j < n; j++)
		{
			cout << readMatrixElement(matrix->matrixName, i, j) << " ";
		}
		cout << endl;
	}

	cout << "Matrix " << matrix->matrixName << " rotated 90 degrees clockwise.\n";
}
