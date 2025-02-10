#include "../global.h"
#include "../matrixHelpers.h"

/**
 * @brief
 * SYNTAX: CROSSTRANSPOSE <matrix_name1> <matrix_name2>
 *
 * - Transpose matrix_name1 in-place
 * - Transpose matrix_name2 in-place
 * - Swap their contents in-place.
 */

static void transposeMatrixInPlace(const string &matrixName)
{
	Matrix *M = matrixCatalogue.getMatrix(matrixName);
	int n = M->dimension;

	for (int i = 0; i < n; i++)
	{
		for (int j = i + 1; j < n; j++)
		{
			int val1 = readMatrixElement(matrixName, i, j);
			int val2 = readMatrixElement(matrixName, j, i);

			writeMatrixElement(matrixName, i, j, val2);
			writeMatrixElement(matrixName, j, i, val1);
		}
	}
}

bool syntacticParseCROSSTRANSPOSE()
{
	logger.log("syntacticParseCROSSTRANSPOSE");

	if (tokenizedQuery.size() != 3)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = CROSSTRANSPOSE;
	parsedQuery.crossTransposeMatrixName1 = tokenizedQuery[1];
	parsedQuery.crossTransposeMatrixName2 = tokenizedQuery[2];
	return true;
}

bool semanticParseCROSSTRANSPOSE()
{
	logger.log("semanticParseCROSSTRANSPOSE");
	if (!matrixCatalogue.isMatrix(parsedQuery.crossTransposeMatrixName1) ||
		!matrixCatalogue.isMatrix(parsedQuery.crossTransposeMatrixName2))
	{
		cout << "SEMANTIC ERROR: One or both matrices do not exist." << endl;
		return false;
	}
	Matrix *M1 = matrixCatalogue.getMatrix(parsedQuery.crossTransposeMatrixName1);
	Matrix *M2 = matrixCatalogue.getMatrix(parsedQuery.crossTransposeMatrixName2);
	if (M1->dimension != M2->dimension)
	{
		cout << "SEMANTIC ERROR: Matrices must have the same dimensions." << endl;
		return false;
	}
	return true;
}

void executeCROSSTRANSPOSE()
{
	logger.log("executeCROSSTRANSPOSE");
	string mat1 = parsedQuery.crossTransposeMatrixName1;
	string mat2 = parsedQuery.crossTransposeMatrixName2;

	transposeMatrixInPlace(mat1);
	transposeMatrixInPlace(mat2);

	Matrix *M1 = matrixCatalogue.getMatrix(mat1);
	Matrix *M2 = matrixCatalogue.getMatrix(mat2);
	int n = M1->dimension;

	for (int i = 0; i < n; i++)
	{
		for (int j = 0; j < n; j++)
		{
			int val1 = readMatrixElement(mat1, i, j);
			int val2 = readMatrixElement(mat2, i, j);

			writeMatrixElement(mat1, i, j, val2);
			writeMatrixElement(mat2, i, j, val1);
		}
	}

	cout << "CROSSTRANSPOSE done. \""
		 << mat1 << "\" is now transpose of original \"" << mat2
		 << "\", and \"" << mat2 << "\" is now transpose of original \"" << mat1 << "\".\n";
}
