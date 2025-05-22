#include "../global.h"
/**
 * @brief
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseSELECTION()
{
	logger.log("syntacticParseSELECTION");
	if (tokenizedQuery.size() != 8 || tokenizedQuery[6] != "FROM")
	{
		cout << "SYNTAC ERROR" << endl;
		return false;
	}
	parsedQuery.queryType = SELECTION;
	parsedQuery.selectionResultRelationName = tokenizedQuery[0];
	parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
	parsedQuery.selectionRelationName = tokenizedQuery[7];

	string binaryOperator = tokenizedQuery[4];
	if (binaryOperator == "<")
		parsedQuery.selectionBinaryOperator = LESS_THAN;
	else if (binaryOperator == ">")
		parsedQuery.selectionBinaryOperator = GREATER_THAN;
	else if (binaryOperator == ">=" || binaryOperator == "=>")
		parsedQuery.selectionBinaryOperator = GEQ;
	else if (binaryOperator == "<=" || binaryOperator == "=<")
		parsedQuery.selectionBinaryOperator = LEQ;
	else if (binaryOperator == "==")
		parsedQuery.selectionBinaryOperator = EQUAL;
	else if (binaryOperator == "!=")
		parsedQuery.selectionBinaryOperator = NOT_EQUAL;
	else
	{
		cout << "SYNTAC ERROR" << endl;
		return false;
	}
	regex numeric("[-]?[0-9]+");
	string secondArgument = tokenizedQuery[5];
	if (regex_match(secondArgument, numeric))
	{
		parsedQuery.selectType = INT_LITERAL;
		parsedQuery.selectionIntLiteral = stoi(secondArgument);
	}
	else
	{
		parsedQuery.selectType = COLUMN;
		parsedQuery.selectionSecondColumnName = secondArgument;
	}
	return true;
}

bool semanticParseSELECTION()
{
	logger.log("semanticParseSELECTION");

	if (tableCatalogue.isTable(parsedQuery.selectionResultRelationName))
	{
		cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
		return false;
	}

	if (!tableCatalogue.isTable(parsedQuery.selectionRelationName))
	{
		cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
		return false;
	}

	if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionFirstColumnName, parsedQuery.selectionRelationName))
	{
		cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
		return false;
	}

	if (parsedQuery.selectType == COLUMN)
	{
		if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionSecondColumnName, parsedQuery.selectionRelationName))
		{
			cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
			return false;
		}
	}
	return true;
}

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator)
{
	switch (binaryOperator)
	{
	case LESS_THAN:
		return (value1 < value2);
	case GREATER_THAN:
		return (value1 > value2);
	case LEQ:
		return (value1 <= value2);
	case GEQ:
		return (value1 >= value2);
	case EQUAL:
		return (value1 == value2);
	case NOT_EQUAL:
		return (value1 != value2);
	default:
		return false;
	}
}

void executeSELECTION()
{
	logger.log("executeSELECTION");
    Table* table_ptr = tableCatalogue.getTable(parsedQuery.selectionRelationName); // Renamed to avoid conflict
    if (table_ptr) {
        logger.log("executeSELECTION: Source table '" + table_ptr->tableName + "' info:");
        logger.log("  rowCount: " + to_string(table_ptr->rowCount));
        logger.log("  blockCount: " + to_string(table_ptr->blockCount));
        logger.log("  columnCount: " + to_string(table_ptr->columnCount));
        string rpb_str = "  rowsPerBlockCount: ";
        for(size_t i=0; i < table_ptr->rowsPerBlockCount.size(); ++i) {
            rpb_str += to_string(i) + ":" + to_string(table_ptr->rowsPerBlockCount[i]) + " ";
            if (i > 30 && i < table_ptr->rowsPerBlockCount.size() - 5) { // To keep log shorter
                rpb_str += "... ";
                i = table_ptr->rowsPerBlockCount.size() - 6;
            }
        }
        logger.log(rpb_str);
    } else {
        logger.log("executeSELECTION: Source table '" + parsedQuery.selectionRelationName + "' not found in catalogue!");
        // If table not found, we probably should not proceed.
        // Create an empty resultant table and return, or just return.
        // For now, let's ensure a resultantTable object is created if needed by other logic,
        // but it will be empty.
        Table *resultantTable = new Table(parsedQuery.selectionResultRelationName, {}); // Empty columns
        if (resultantTable->blockify()) // Attempt to blockify (will be empty)
    		tableCatalogue.insertTable(resultantTable);
    	else
    	{
    		cout << "Empty Table" << endl;
    		resultantTable->unload();
    		delete resultantTable;
    	}
        return; 
    }

	Table table = *table_ptr; // Use the fetched pointer
	Table *resultantTable = new Table(parsedQuery.selectionResultRelationName, table.columns);
	Cursor cursor = table.getCursor();
    logger.log("executeSELECTION: About to call cursor.getNext() for the first time.");
	vector<int> row = cursor.getNext();
    if(row.empty()) {
        logger.log("executeSELECTION: First call to cursor.getNext() returned an EMPTY row.");
    } else {
        string row_str_log_first = "executeSELECTION: First call to cursor.getNext() returned row: "; // Changed variable name
        for(size_t i=0; i<row.size(); ++i) row_str_log_first += (i==0?"":", ") + to_string(row[i]);
        logger.log(row_str_log_first);
    }

	int firstColumnIndex = table.getColumnIndex(parsedQuery.selectionFirstColumnName);
	int secondColumnIndex;
	if (parsedQuery.selectType == COLUMN)
		secondColumnIndex = table.getColumnIndex(parsedQuery.selectionSecondColumnName);
	while (!row.empty())
	{
        string row_str_log = "executeSELECTION: Processing row: ";
        for(size_t i=0; i<row.size(); ++i) row_str_log += (i==0?"":", ") + to_string(row[i]);
        logger.log(row_str_log);

		int value1 = row[firstColumnIndex];
		int value2;
		if (parsedQuery.selectType == INT_LITERAL)
			value2 = parsedQuery.selectionIntLiteral;
		else
			value2 = row[secondColumnIndex];
		
        bool eval_res = evaluateBinOp(value1, value2, parsedQuery.selectionBinaryOperator);
        logger.log("executeSELECTION: Cond Col Val1: " + to_string(value1) + ", Literal/Col Val2: " + to_string(value2) + ", Op: " + to_string(parsedQuery.selectionBinaryOperator) + ", Result: " + (eval_res ? "true" : "false"));

		if (eval_res) // Use the stored result
			resultantTable->writeRow<int>(row);
		row = cursor.getNext();
	}
	if (resultantTable->blockify())
		tableCatalogue.insertTable(resultantTable);
	else
	{
		cout << "Empty Table" << endl;
		resultantTable->unload();
		delete resultantTable;
	}
	return;
}