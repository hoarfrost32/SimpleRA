#ifndef SYNTACTICPARSER_H
#define SYNTACTICPARSER_H

#pragma once
#include "tableCatalogue.h"
#include "matrixCatalogue.h"
#include <unordered_map>

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
	GROUPBY,
	INSERT,
	UPDATE,
	DELETE,
	SEARCH,
	QUIT,
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

enum UpdateOpType
{
	SET_LITERAL, // col = <int>
	ADD_LITERAL, // col = col + <int>
	SUB_LITERAL, // col = col - <int>
	NO_UPDATE_OP
};

enum AggregateFunction
{
	MAX_F,
	MIN_F,
	COUNT_F,
	SUM_F,
	AVG_F,
	NO_AGGREGATE_FUNC
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
	vector<pair<string, string>> sortColumns; // Pairs of (columnName, sortDirection)

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

	string groupByResultRelationName = "";					 // Result table name
	string groupByRelationName = "";						 // Source table name
	string groupByAttribute = "";							 // Grouping attribute
	string groupByHavingAttribute = "";						 // Attribute for HAVING condition
	AggregateFunction groupByHavingFunc = NO_AGGREGATE_FUNC; // HAVING aggregate function
	BinaryOperator groupByHavingOperator = NO_BINOP_CLAUSE;	 // HAVING binary operator
	int groupByHavingValue = 0;								 // HAVING comparison value
	string groupByReturnAttribute = "";						 // Attribute for RETURN
	AggregateFunction groupByReturnFunc = NO_AGGREGATE_FUNC; // RETURN aggregate function

	/* ---------- INSERT ---------- */
	string insertRelationName = "";
	std::unordered_map<string, int> insertColumnValueMap;

	/* ---------- UPDATE ---------- */
	string updateRelationName = "";
	string updateTargetColumn = "";
	UpdateOpType updateOpType = NO_UPDATE_OP;
	int updateLiteral = 0; // meaning depends on opType
	string updateCondColumn = "";
	BinaryOperator updateCondOperator = NO_BINOP_CLAUSE;
	int updateCondValue = 0;

	/* ---------- DELETE ---------- */
	string deleteRelationName = "";
	string deleteCondColumn = "";
	BinaryOperator deleteCondOperator = NO_BINOP_CLAUSE;
	int deleteCondValue = 0;
	
    /* ---------- SEARCH ---------- */ // <-- ADDED BLOCK
    string searchResultRelationName = "";
    string searchRelationName = "";
    string searchColumnName = "";
    BinaryOperator searchOperator = NO_BINOP_CLAUSE;
    int searchLiteralValue = 0;

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
bool syntacticParseGROUPBY();
bool syntacticParseINSERT();
bool syntacticParseUPDATE();
bool syntacticParseDELETE();
bool syntacticParseSEARCH();
bool syntacticParseQUIT();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);

#endif