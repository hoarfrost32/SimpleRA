#include "../global.h"
#include "../matrixHelpers.h"

bool syntacticParseROTATEMATRIX()
{
	logger.log("syntacticParseROTATEMATRIX");
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
	if (!matrixCatalogue.isMatrix(parsedQuery.rotateMatrixName))
	{
		cout << "SEMANTIC ERROR: No such matrix loaded" << endl;
		return false;
	}
	return true;
}

void executeROTATEMATRIX()
{
	logger.log("executeROTATEMATRIX");
	Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.rotateMatrixName);
	int n = matrix->dimension;

	for (int layer = 0; layer < n / 2; layer++)
	{
		for (int i = layer; i < n - layer - 1; i++)
		{
			int topRow = layer, topCol = i;
			int rightRow = i, rightCol = n - 1 - layer;
			int bottomRow = n - 1 - layer, bottomCol = n - 1 - i;
			int leftRow = n - 1 - i, leftCol = layer;

			int topVal = readMatrixElement(matrix->matrixName, topRow, topCol);
			int rightVal = readMatrixElement(matrix->matrixName, rightRow, rightCol);
			int bottomVal = readMatrixElement(matrix->matrixName, bottomRow, bottomCol);
			int leftVal = readMatrixElement(matrix->matrixName, leftRow, leftCol);

			writeMatrixElement(matrix->matrixName, rightRow, rightCol, topVal);
			writeMatrixElement(matrix->matrixName, bottomRow, bottomCol, rightVal);
			writeMatrixElement(matrix->matrixName, leftRow, leftCol, bottomVal);
			writeMatrixElement(matrix->matrixName, topRow, topCol, leftVal);
		}
	}

	cout << "Matrix " << matrix->matrixName << " rotated 90 degrees clockwise.\n";
}
