#include "../global.h"
#include <regex>

/**
 * Grammar:
 *   DELETE FROM <table> WHERE <col> <binop> <int>
 * tokenizedQuery = ["DELETE","FROM","T","WHERE","col","<", "10"]
 */

static bool parseBinOp(const string &tok, BinaryOperator &op)
{
	if (tok == "==")
		op = EQUAL;
	else if (tok == "!=")
		op = NOT_EQUAL;
	else if (tok == "<")
		op = LESS_THAN;
	else if (tok == "<=" || tok == "=<")
		op = LEQ;
	else if (tok == ">")
		op = GREATER_THAN;
	else if (tok == ">=" || tok == "=>")
		op = GEQ;
	else
		return false;
	return true;
}

bool syntacticParseDELETE()
{
	logger.log("syntacticParseDELETE");
	if (tokenizedQuery.size() != 7 ||
		tokenizedQuery[1] != "FROM" ||
		tokenizedQuery[3] != "WHERE")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = DELETE;
	parsedQuery.deleteRelationName = tokenizedQuery[2];
	parsedQuery.deleteCondColumn = tokenizedQuery[4];

	if (!parseBinOp(tokenizedQuery[5], parsedQuery.deleteCondOperator))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	regex numeric("[-]?[0-9]+");
	if (!regex_match(tokenizedQuery[6], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.deleteCondValue = stoi(tokenizedQuery[6]);
	return true;
}

/* keep semantic & executor dummy */
bool semanticParseDELETE() { return false; }
void executeDELETE() { cout << "DELETE not implemented yet.\n"; }
