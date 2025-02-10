#include "../global.h"
#include "../matrixHelpers.h"

/**
 * @brief
 * SYNTAX: CHECKANTISYM <matrixName1> <matrixName2>
 *
 * Prints "True" if A = - B^T, else "False".
 */

bool syntacticParseCHECKANTISYM()
{
    logger.log("syntacticParseCHECKANTISYM");

    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = CHECKANTISYM;
    parsedQuery.checkAntiSymMatrixName1 = tokenizedQuery[1];
    parsedQuery.checkAntiSymMatrixName2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCHECKANTISYM()
{
    logger.log("semanticParseCHECKANTISYM");
    if (!matrixCatalogue.isMatrix(parsedQuery.checkAntiSymMatrixName1) ||
        !matrixCatalogue.isMatrix(parsedQuery.checkAntiSymMatrixName2))
    {
        cout << "SEMANTIC ERROR: One or both matrices do not exist." << endl;
        return false;
    }
    Matrix *M1 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymMatrixName1);
    Matrix *M2 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymMatrixName2);
    if (M1->dimension != M2->dimension)
    {
        cout << "SEMANTIC ERROR: Matrices have different dimensions." << endl;
        return false;
    }
    return true;
}

void executeCHECKANTISYM()
{
    logger.log("executeCHECKANTISYM");
    string mat1 = parsedQuery.checkAntiSymMatrixName1;
    string mat2 = parsedQuery.checkAntiSymMatrixName2;

    Matrix *M1 = matrixCatalogue.getMatrix(mat1);
    Matrix *M2 = matrixCatalogue.getMatrix(mat2);
    int n = M1->dimension;

    bool isAntiSym = true;
    for (int i = 0; i < n; i++)
    {
        for (int j = 0; j < n; j++)
        {
            if (readMatrixElement(mat1, i, j) != -readMatrixElement(mat2, j, i))
            {
                isAntiSym = false;
                break;
            }
        }
        if (!isAntiSym)
            break;
    }

    cout << (isAntiSym ? "True" : "False") << endl;
}