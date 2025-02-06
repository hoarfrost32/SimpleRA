#include "../global.h"

/**
 * @brief
 * SYNTAX: EXPORT MATRIX <matrix_name>
 */
void executeEXPORTMATRIX()
{
	logger.log("executeEXPORTMATRIX");
	Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.exportMatrixName);
	// We make it “permanent” by writing out all rows to ../data/<matrixName>.csv
	matrix->makePermanent();
	cout << "Exported matrix " << matrix->matrixName << " to file: "
		 << matrix->matrixName << ".csv" << endl;
}

bool syntacticParseEXPORTMATRIX()
{
	logger.log("syntacticParseEXPORTMATRIX");
	// Expect exactly 3 tokens: EXPORT MATRIX <matrixName>
	if (tokenizedQuery.size() != 3 || tokenizedQuery[1] != "MATRIX")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = EXPORTMATRIX;
	parsedQuery.exportMatrixName = tokenizedQuery[2];
	return true;
}

bool semanticParseEXPORTMATRIX()
{
	logger.log("semanticParseEXPORTMATRIX");

	// Matrix must exist in the matrixCatalogue
	if (!matrixCatalogue.isMatrix(parsedQuery.exportMatrixName))
	{
		cout << "SEMANTIC ERROR: No such matrix exists" << endl;
		return false;
	}
	return true;
}
