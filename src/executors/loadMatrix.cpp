#include "../global.h"

/**
 * @brief
 * SYNTAX: LOAD MATRIX matrixName
 */

bool syntacticParseLOADMATRIX()
{
	logger.log("syntacticParseLOADMATRIX");
	if (tokenizedQuery.size() != 3)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.queryType = LOADMATRIX;
	parsedQuery.loadMatrixName = tokenizedQuery[2];
	return true;
}

bool semanticParseLOADMATRIX()
{
	logger.log("semanticParseLOADMATRIX");
	if (matrixCatalogue.isMatrix(parsedQuery.loadMatrixName))
	{
		cout << "SEMANTIC ERROR: Matrix already exists" << endl;
		return false;
	}
	if (!isFileExists(parsedQuery.loadMatrixName))
	{
		cout << "SEMANTIC ERROR: File doesn't exist" << endl;
		return false;
	}
	return true;
}

void executeLOADMATRIX()
{
	logger.log("executeLOADMATRIX");

	// Construct Matrix object
	Matrix *matrix = new Matrix(parsedQuery.loadMatrixName);

	// Load from CSV
	if (matrix->load())
	{
		matrixCatalogue.insertMatrix(matrix);
		cout << "Loaded Matrix. Dimensions: "
			 << matrix->n << " x " << matrix->n << endl;
	}
	return;
}
