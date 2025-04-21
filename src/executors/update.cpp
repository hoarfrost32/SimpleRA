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

	/* Grammar we accept:
	   UPDATE table_name WHERE col1 <binop> int_literal SET col2 = int_literal
	   Minimum tokens = 10  (UPDATE T WHERE c1 == 5 SET c2 = 7)               */

	if (tokenizedQuery.size() < 10 ||
		tokenizedQuery[2] != "WHERE")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = UPDATE;
	parsedQuery.updateRelationName = tokenizedQuery[1];

	/* -------- condition part -------- */
	parsedQuery.updateCondColumn = tokenizedQuery[3];

	string binOpTok = tokenizedQuery[4];
	if (binOpTok == "<")
		parsedQuery.updateCondOperator = LESS_THAN;
	else if (binOpTok == ">")
		parsedQuery.updateCondOperator = GREATER_THAN;
	else if (binOpTok == "<=")
		parsedQuery.updateCondOperator = LEQ;
	else if (binOpTok == ">=")
		parsedQuery.updateCondOperator = GEQ;
	else if (binOpTok == "==")
		parsedQuery.updateCondOperator = EQUAL;
	else if (binOpTok == "!=")
		parsedQuery.updateCondOperator = NOT_EQUAL;
	else
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	/* RHS of condition must be int literal */
	regex numeric("[-]?[0-9]+");
	if (!regex_match(tokenizedQuery[5], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateCondValue = stoi(tokenizedQuery[5]);

	/* -------- SET part -------- */
	size_t setPos = 6;
	if (tokenizedQuery[setPos] != "SET" ||
		setPos + 3 >= tokenizedQuery.size() ||
		tokenizedQuery[setPos + 2] != "=")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.updateTargetColumn = tokenizedQuery[setPos + 1];

	/* literal after '=' */
	if (!regex_match(tokenizedQuery[setPos + 3], numeric))
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.updateLiteral = stoi(tokenizedQuery[setPos + 3]);

	/* extra tokens? */
	if (setPos + 4 != tokenizedQuery.size())
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	return true;
}

/* semantic & executor remain dummy */
bool semanticParseUPDATE()
{
	logger.log("semanticParseUPDATE");

	/* table must exist */
	if (!tableCatalogue.isTable(parsedQuery.updateRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.updateRelationName);

	/* target column must exist */
	if (!table->isColumn(parsedQuery.updateTargetColumn))
	{
		cout << "SEMANTIC ERROR: Target column not found" << endl;
		return false;
	}

	/* WHERE column must exist */
	if (!table->isColumn(parsedQuery.updateCondColumn))
	{
		cout << "SEMANTIC ERROR: Condition column not found" << endl;
		return false;
	}
	return true;
}

static void writeIntRow(ofstream &fout, const vector<int> &row)
{
	for (size_t c = 0; c < row.size(); ++c)
	{
		if (c)
			fout << ", ";
		fout << row[c];
	}
	fout << '\n';
}

void executeUPDATE()
{
	logger.log("executeUPDATE");

	Table *table = tableCatalogue.getTable(parsedQuery.updateRelationName);
	int tgtIndex = table->getColumnIndex(parsedQuery.updateTargetColumn);
	int condIndex = table->getColumnIndex(parsedQuery.updateCondColumn);

	/* --- create temp csv -------------------------------------------- */
	string tmpCSV = table->sourceFileName + ".upd";
	ofstream fout(tmpCSV, ios::trunc);

	/* header */
	table->writeRow<string>(table->columns, fout);

	/* iterate rows */
	Cursor cursor = table->getCursor();
	vector<int> row = cursor.getNext();
	long long rowsTouched = 0;

	while (!row.empty())
	{
		if (evaluateBinOp(row[condIndex], parsedQuery.updateCondValue,
						  parsedQuery.updateCondOperator))
		{
			/* apply operation – only SET_LITERAL in Phase 3 */
			row[tgtIndex] = parsedQuery.updateLiteral;
			rowsTouched++;
		}
		writeIntRow(fout, row);
		row = cursor.getNext();
	}
	fout.close();

	/* swap files */
	remove(table->sourceFileName.c_str());
	rename(tmpCSV.c_str(), table->sourceFileName.c_str());

	/* rebuild pages */
	table->reload();

	cout << rowsTouched << " row(s) updated in \"" << table->tableName << "\"\n";
}
