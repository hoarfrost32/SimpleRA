#include "global.h"

bool syntacticParse()
{
	logger.log("syntacticParse");
	string possibleQueryType = tokenizedQuery[0];
	
	if (tokenizedQuery.size() == 1 && tokenizedQuery[0] == "QUIT")
	  return syntacticParseQUIT();

	if (tokenizedQuery.size() < 2)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	if (possibleQueryType == "CLEAR")
		return syntacticParseCLEAR();
	else if (possibleQueryType == "INDEX")
		return syntacticParseINDEX();
	else if (possibleQueryType == "INSERT")
		return syntacticParseINSERT();
	else if (possibleQueryType == "UPDATE")
		return syntacticParseUPDATE();
	else if (possibleQueryType == "DELETE")
		return syntacticParseDELETE();
	else if (possibleQueryType == "LIST")
		return syntacticParseLIST();
	else if (possibleQueryType == "CROSSTRANSPOSE")
		return syntacticParseCROSSTRANSPOSE();
	else if (possibleQueryType == "LOAD")
	{
		if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "MATRIX")
			return syntacticParseLOADMATRIX();
		else
			return syntacticParseLOAD();
	}
	else if (possibleQueryType == "PRINT")
	{
		if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "MATRIX")
			return syntacticParsePRINTMATRIX();
		else
			return syntacticParsePRINT();
	}
	else if (possibleQueryType == "RENAME")
		return syntacticParseRENAME();
	else if (possibleQueryType == "EXPORT")
	{
		if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "MATRIX")
			return syntacticParseEXPORTMATRIX();
		else
			return syntacticParseEXPORT();
	}
	else if (possibleQueryType == "SOURCE")
		return syntacticParseSOURCE();
	else if (possibleQueryType == "ROTATE")
		return syntacticParseROTATEMATRIX();
	else if (possibleQueryType == "CHECKANTISYM")
		return syntacticParseCHECKANTISYM();
	else if (possibleQueryType == "SORT")
		return syntacticParseSORT();
	else
	{
		string resultantRelationName = possibleQueryType;
		if (tokenizedQuery[1] != "<-" || tokenizedQuery.size() < 3)
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}
		possibleQueryType = tokenizedQuery[2];
		if (possibleQueryType == "PROJECT")
			return syntacticParsePROJECTION();
		else if (possibleQueryType == "SELECT")
			return syntacticParseSELECTION();
		else if (possibleQueryType == "JOIN")
			return syntacticParseJOIN();
		else if (possibleQueryType == "CROSS")
			return syntacticParseCROSS();
		else if (possibleQueryType == "DISTINCT")
			return syntacticParseDISTINCT();
		else if (possibleQueryType == "ORDER")
			return syntacticParseORDERBY();
		else if (possibleQueryType == "GROUP")
			return syntacticParseGROUPBY();
		else if (possibleQueryType == "SEARCH")
			return syntacticParseSEARCH();
		else
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}
	}
	return false;
}

ParsedQuery::ParsedQuery()
{
}

void ParsedQuery::clear()
{
	logger.log("ParseQuery::clear");
	this->queryType = UNDETERMINED;

	this->clearRelationName = "";

	this->crossResultRelationName = "";
	this->crossFirstRelationName = "";
	this->crossSecondRelationName = "";

	this->distinctResultRelationName = "";
	this->distinctRelationName = "";

	this->exportRelationName = "";

	this->indexingStrategy = NOTHING;
	this->indexColumnName = "";
	this->indexRelationName = "";

	this->joinBinaryOperator = NO_BINOP_CLAUSE;
	this->joinResultRelationName = "";
	this->joinFirstRelationName = "";
	this->joinSecondRelationName = "";
	this->joinFirstColumnName = "";
	this->joinSecondColumnName = "";

	this->loadRelationName = "";

	this->printRelationName = "";

	this->projectionResultRelationName = "";
	this->projectionColumnList.clear();
	this->projectionRelationName = "";

	this->renameFromColumnName = "";
	this->renameToColumnName = "";
	this->renameRelationName = "";

	this->selectType = NO_SELECT_CLAUSE;
	this->selectionBinaryOperator = NO_BINOP_CLAUSE;
	this->selectionResultRelationName = "";
	this->selectionRelationName = "";
	this->selectionFirstColumnName = "";
	this->selectionSecondColumnName = "";
	this->selectionIntLiteral = 0;

	this->sortingStrategy = NO_SORT_CLAUSE;
	this->sortResultRelationName = "";
	this->sortColumnName = "";
	this->sortRelationName = "";

	this->groupByResultRelationName = "";
	this->groupByRelationName = "";
	this->groupByAttribute = "";
	this->groupByHavingAttribute = "";
	this->groupByHavingFunc = NO_AGGREGATE_FUNC;
	this->groupByHavingOperator = NO_BINOP_CLAUSE;
	this->groupByHavingValue = 0;
	this->groupByReturnAttribute = "";
	this->groupByReturnFunc = NO_AGGREGATE_FUNC;

	this->sourceFileName = "";
	this->rotateMatrixName = "";

	this->insertRelationName.clear();
	this->insertColumnValueMap.clear();

	/* UPDATE */
	updateRelationName = "";
	updateTargetColumn = "";
	updateOpType = NO_UPDATE_OP;
	updateLiteral = 0;
	updateCondColumn = "";
	updateCondOperator = NO_BINOP_CLAUSE;
	updateCondValue = 0;

	/* DELETE */
	deleteRelationName = "";
	deleteCondColumn = "";
	deleteCondOperator = NO_BINOP_CLAUSE;
	deleteCondValue = 0;
	
	/* SEARCH */
    searchResultRelationName = "";
    searchRelationName = "";
    searchColumnName = "";
    searchOperator = NO_BINOP_CLAUSE;
    searchLiteralValue = 0;
}

/**
 * @brief Checks to see if source file exists. Called when LOAD command is
 * invoked.
 *
 * @param tableName
 * @return true
 * @return false
 */
bool isFileExists(string tableName)
{
	string fileName = "../data/" + tableName + ".csv";
	struct stat buffer;
	return (stat(fileName.c_str(), &buffer) == 0);
}

/**
 * @brief Checks to see if source file exists. Called when SOURCE command is
 * invoked.
 *
 * @param tableName
 * @return true
 * @return false
 */
bool isQueryFile(string fileName)
{
	fileName = "../data/" + fileName + ".ra";
	struct stat buffer;
	return (stat(fileName.c_str(), &buffer) == 0);
}
