#include "../global.h"
#include <regex>
#include <unordered_map>

/**
 * Grammar (comma already stripped by tokenizer):
 *   INSERT INTO <table> VALUES <v1> <v2> ... <vn>
 * tokenizedQuery = ["INSERT","INTO","T","VALUES","1","2",...]
 */

bool syntacticParseINSERT()
{
	logger.log("syntacticParseINSERT");

	/* Expected token pattern:
	   0 1     2           3 4 ... n‑2 n‑1
	   INSERT INTO <table> ( col = val , col = val … )                */

	if (tokenizedQuery.size() < 8 || // minimal length
		tokenizedQuery[1] != "INTO" ||
		tokenizedQuery[3] != "(" ||
		tokenizedQuery.back() != ")")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = INSERT;
	parsedQuery.insertRelationName = tokenizedQuery[2];
	parsedQuery.insertColumnValueMap.clear();

	/* Scan tokens between '(' and ')' */
	size_t i = 4;
	while (i < tokenizedQuery.size() - 1) // stop before ')'
	{
		string col = tokenizedQuery[i];
		if (col == ",")
		{
			i++;
			continue;
		}

		/* expect: col = val */
		if (i + 2 >= tokenizedQuery.size() ||
			tokenizedQuery[i + 1] != "=")
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}

		/* numeric literal? */
		regex numeric("[-]?[0-9]+");
		string valTok = tokenizedQuery[i + 2];
		if (!regex_match(valTok, numeric))
		{
			cout << "SYNTAX ERROR" << endl;
			return false;
		}

		int value = stoi(valTok);
		parsedQuery.insertColumnValueMap[col] = value;

		i += 3;							   // jump past "col = val"
		if (i < tokenizedQuery.size() - 1) // if not at ')'
		{
			if (tokenizedQuery[i] == ",")
				i++; // consume comma
		}
	}

	if (parsedQuery.insertColumnValueMap.empty())
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	return true;
}

bool semanticParseINSERT()
{
	logger.log("semanticParseINSERT");

	if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);

	/* Each referenced column must exist */
	for (auto &kv : parsedQuery.insertColumnValueMap)
	{
		if (!table->isColumn(kv.first))
		{
			cout << "SEMANTIC ERROR: Column " << kv.first
				 << " doesn't exist in relation" << endl;
			return false;
		}
	}
	return true;
}

static void buildRow(const Table *table,
					 const std::unordered_map<string, int> &colVal,
					 std::vector<int> &outRow)
{
	outRow.assign(table->columnCount, 0); // default 0

	for (size_t c = 0; c < table->columnCount; ++c)
	{
		const string &colName = table->columns[c];
		auto it = colVal.find(colName);
		if (it != colVal.end())
			outRow[c] = it->second;
	}
}

void executeINSERT()
{
	logger.log("executeINSERT");

	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);

	/* Build full row in table‑schema order */
	vector<int> newRow;
	buildRow(table, parsedQuery.insertColumnValueMap, newRow);

	/* Append row to CSV */
	table->writeRow<int>(newRow);

	/* Reblockify for later operators */
	table->reload();

	cout << "1 row inserted into \"" << table->tableName << "\"\n";
}
