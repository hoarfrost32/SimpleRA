#include "../global.h"
/**
 * @brief
 * SYNTAX: QUIT
 */
bool syntacticParseQUIT()
{
	logger.log("syntacticParseQUIT");
	if (tokenizedQuery.size() != 1)
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}
	
  parsedQuery.queryType = QUIT;
	return true;
}

bool semanticParseQUIT()
{
	logger.log("semanticParseQUIT");
	return true;
}

void executeQUIT()
{
	logger.log("executeQUIT");
	exit(0);
	return;
}
