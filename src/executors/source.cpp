#include "../global.h"

bool syntacticParseSOURCE()
{
	logger.log("syntacticParseSOURCE");
	if (tokenizedQuery.size() != 2)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	parsedQuery.queryType = SOURCE;
	parsedQuery.sourceFileName = tokenizedQuery[1];
	return true;
}

bool semanticParseSOURCE()
{
	logger.log("semanticParseSOURCE");
	if (!isQueryFile(parsedQuery.sourceFileName))
	{
		cout << "SEMANTIC ERROR: File doesn't exist" << endl;
		return false;
	}
	return true;
}

void executeSOURCE()
{
	logger.log("executeSOURCE");

	string fileName = "../data/" + parsedQuery.sourceFileName + ".ra";
	ifstream fin(fileName);
	if (!fin.is_open())
	{

		cout << "SEMANTIC ERROR: Could not open file \"" << fileName << "\"" << endl;
		return;
	}

	regex delim("[^\\s,]+");
	string command;

	while (getline(fin, command))
	{
		logger.log(command);

		tokenizedQuery.clear();
		parsedQuery.clear();
		auto words_begin = sregex_iterator(command.begin(), command.end(), delim);
		auto words_end = sregex_iterator();
		for (auto i = words_begin; i != words_end; ++i)
			tokenizedQuery.emplace_back((*i).str());

		if (tokenizedQuery.empty())
			continue;

		if (syntacticParse() && semanticParse())
			executeCommand();
	}

	fin.close();
	return;
}