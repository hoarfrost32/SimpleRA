#include "global.h"

void executeCommand()
{

	switch (parsedQuery.queryType)
	{
	case CLEAR:
		executeCLEAR();
		break;
	case CROSS:
		executeCROSS();
		break;
	case DISTINCT:
		executeDISTINCT();
		break;
	case EXPORT:
		executeEXPORT();
		break;
	case INDEX:
		executeINDEX();
		break;
	case JOIN:
		executeJOIN();
		break;
	case LIST:
		executeLIST();
		break;
	case LOAD:
		executeLOAD();
		break;
	case PRINT:
		executePRINT();
		break;
	case PROJECTION:
		executePROJECTION();
		break;
	case RENAME:
		executeRENAME();
		break;
	case SELECTION:
		executeSELECTION();
		break;
	case SORT:
		executeSORT();
		break;
	case SOURCE:
		executeSOURCE();
		break;
	case LOADMATRIX:
		executeLOADMATRIX();
		break;
	case PRINTMATRIX:
		executePRINTMATRIX();
		break;
	case EXPORTMATRIX:
		executeEXPORTMATRIX();
		break;
	case ROTATEMATRIX:
		executeROTATEMATRIX();
		break;
	case CROSSTRANSPOSE:
		executeCROSSTRANSPOSE();
		break;
	case CHECKANTISYM:
		executeCHECKANTISYM();
		break;
	case ORDERBY:
		executeORDERBY();
		break;
	case GROUPBY:
		executeGROUPBY();
		break;
	case INSERT:
		executeINSERT();
		break;
	case UPDATE:
		executeUPDATE();
		break;
	case DELETE:
		executeDELETE();
		break;
	default:
		cout << "PARSING ERROR" << endl;
	}

	return;
}

void printRowCount(int rowCount)
{
	cout << "\n\nRow Count: " << rowCount << endl;
	return;
}
