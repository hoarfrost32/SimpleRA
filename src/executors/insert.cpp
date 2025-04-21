#include "../global.h"
#include <regex>

/**
 * Grammar (comma already stripped by tokenizer):
 *   INSERT INTO <table> VALUES <v1> <v2> ... <vn>
 * tokenizedQuery = ["INSERT","INTO","T","VALUES","1","2",...]
 */

bool syntacticParseINSERT()
{
	logger.log("syntacticParseINSERT");
	if (tokenizedQuery.size() < 6 ||
		tokenizedQuery[1] != "INTO" ||
		tokenizedQuery[3] != "VALUES")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = INSERT;
	parsedQuery.insertRelationName = tokenizedQuery[2];

	parsedQuery.insertValues.clear();
	regex numeric("[-]?[0-9]+");
	for (size_t i = 4; i < tokenizedQuery.size(); i++)
	{
		if (!regex_match(tokenizedQuery[i], numeric))
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}
		parsedQuery.insertValues.push_back(stoi(tokenizedQuery[i]));
	}
	return true;
}

/* semantic & execute still stub */
bool semanticParseINSERT()
{
	logger.log("semanticParseINSERT");

	/* table must exist */
	if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);

	/* value count must match column count */
	if (parsedQuery.insertValues.size() != table->columnCount)
	{
		cout << "SEMANTIC ERROR: Column count mismatch" << endl;
		return false;
	}
	return true; // all good
}

void executeINSERT()
{
	logger.log("executeINSERT");

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);

	/* 1.  Append the row to the CSV */
	table->writeRow<int>(parsedQuery.insertValues); // appends to temp CSV

	/* 2.  Re‑paginate & refresh stats */
	if (!table->reload())
	{
		cout << "Error while reloading table after INSERT.\n";
		return;
	}

	cout << "1 row inserted into \"" << table->tableName << "\"\n";
}
