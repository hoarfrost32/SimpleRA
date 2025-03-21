#ifndef SYNTACTICPARSER_H
#define SYNTACTICPARSER_H

#pragma once
#include "tableCatalogue.h"
#include "matrixCatalogue.h"

using namespace std;

enum QueryType
{
	CLEAR,
	CROSS,
	DISTINCT,
	EXPORT,
	INDEX,
	JOIN,
	LIST,
	LOAD,
	PRINT,
	PROJECTION,
	RENAME,
	SELECTION,
	SORT,
	SOURCE,
	LOADMATRIX,
	PRINTMATRIX,
	EXPORTMATRIX,
	ROTATEMATRIX,
	CROSSTRANSPOSE,
	CHECKANTISYM,
	ORDERBY,
	UNDETERMINED
};

enum BinaryOperator
{
	LESS_THAN,
	GREATER_THAN,
	LEQ,
	GEQ,
	EQUAL,
	NOT_EQUAL,
	NO_BINOP_CLAUSE
};

enum SortingStrategy
{
	ASC,
	DESC,
	NO_SORT_CLAUSE
};

enum SelectType
{
	COLUMN,
	INT_LITERAL,
	NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
	QueryType queryType = UNDETERMINED;

	string clearRelationName = "";

	string crossResultRelationName = "";
	string crossFirstRelationName = "";
	string crossSecondRelationName = "";

	string distinctResultRelationName = "";
	string distinctRelationName = "";

	string exportRelationName = "";

	IndexingStrategy indexingStrategy = NOTHING;
	string indexColumnName = "";
	string indexRelationName = "";

	BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
	string joinResultRelationName = "";
	string joinFirstRelationName = "";
	string joinSecondRelationName = "";
	string joinFirstColumnName = "";
	string joinSecondColumnName = "";

	string loadRelationName = "";

	string printRelationName = "";

	string projectionResultRelationName = "";
	vector<string> projectionColumnList;
	string projectionRelationName = "";

	string renameFromColumnName = "";
	string renameToColumnName = "";
	string renameRelationName = "";

	SelectType selectType = NO_SELECT_CLAUSE;
	BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
	string selectionResultRelationName = "";
	string selectionRelationName = "";
	string selectionFirstColumnName = "";
	string selectionSecondColumnName = "";
	int selectionIntLiteral = 0;

	SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
	string sortResultRelationName = "";
	string sortColumnName = "";
	string sortRelationName = "";

	string sourceFileName = "";

	string loadMatrixName = "";
	string printMatrixName = "";
	string exportMatrixName = "";
	string rotateMatrixName = "";
	string crossTransposeMatrixName1 = "";
	string crossTransposeMatrixName2 = "";
	string checkAntiSymMatrixName1 = "";
	string checkAntiSymMatrixName2 = "";

	string orderByResultRelationName = "";					 // new table to be created
	string orderByRelationName = "";						 // existing table to order
	string orderByColumnName = "";							 // the column to sort on
	SortingStrategy orderBySortingStrategy = NO_SORT_CLAUSE; // ASC or DESC

	ParsedQuery();
	void clear();
};

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINTMATRIX();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseROTATEMATRIX();
bool syntacticParseCROSSTRANSPOSE();
bool syntacticParseCHECKANTISYM();
bool syntacticParseORDERBY();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);

#endif