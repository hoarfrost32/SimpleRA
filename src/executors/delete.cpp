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
bool semanticParseDELETE()
{
	logger.log("semanticParseDELETE");

	if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);

	if (!table->isColumn(parsedQuery.deleteCondColumn))
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

void executeDELETE()
{
	logger.log("executeDELETE");

	Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
	int condIndex = table->getColumnIndex(parsedQuery.deleteCondColumn);

	string tmpCSV = table->sourceFileName + ".del";
	ofstream fout(tmpCSV, ios::trunc);

	/* header */
	table->writeRow<string>(table->columns, fout);

	Cursor cursor = table->getCursor();
	vector<int> row = cursor.getNext();
	long long rowsDeleted = 0;

	while (!row.empty())
	{
		bool match = evaluateBinOp(row[condIndex], parsedQuery.deleteCondValue,
								   parsedQuery.deleteCondOperator);
		if (!match)
			writeIntRow(fout, row);
		else
			rowsDeleted++;

		row = cursor.getNext();
	}
	fout.close();

	/* overwrite */
	remove(table->sourceFileName.c_str());
	rename(tmpCSV.c_str(), table->sourceFileName.c_str());

	table->reload();

	cout << rowsDeleted << " row(s) deleted from \"" << table->tableName << "\"\n";
}
