#include "../global.h"
#include <regex>

/**
 * Supported limited grammar:
 *   UPDATE <table> SET <col> = <int> WHERE <col2> <binop> <int>
 * tokenizedQuery = ["UPDATE","T","SET","c","=","5","WHERE","d",">","10"]
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

bool syntacticParseUPDATE()
{
	logger.log("syntacticParseUPDATE");

	if (tokenizedQuery.size() != 10 ||
		tokenizedQuery[2] != "SET" ||
		tokenizedQuery[4] != "=" || // index 4 is the “=”
		tokenizedQuery[6] != "WHERE")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	/* left side */
	parsedQuery.queryType = UPDATE;
	parsedQuery.updateRelationName = tokenizedQuery[1];
	parsedQuery.updateTargetColumn = tokenizedQuery[3];

	/* right side */
	regex numeric("[-]?[0-9]+");
	if (!regex_match(tokenizedQuery[5], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateOpType = SET_LITERAL;
	parsedQuery.updateLiteral = stoi(tokenizedQuery[5]);

	/* WHERE clause */
	parsedQuery.updateCondColumn = tokenizedQuery[7];

	if (!parseBinOp(tokenizedQuery[8], parsedQuery.updateCondOperator))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	if (!regex_match(tokenizedQuery[9], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateCondValue = stoi(tokenizedQuery[9]);
	return true;
}

/* semantic & executor remain dummy */
bool semanticParseUPDATE() { return false; }
void executeUPDATE() { cout << "UPDATE not implemented yet.\n"; }
