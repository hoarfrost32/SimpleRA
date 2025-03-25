#include "../global.h"
#include <algorithm>

/**
 * @brief 
 * SYNTAX: 
 * Result-table <- GROUP BY <attribute1>
 * FROM <table>
 * HAVING <Aggregate-Func1(attribute2)> <bin-op> <attribute-value>
 * RETURN <Aggregate-Func2(attribute3)>
 */

// Helper function to get aggregate function name as string
string getAggregateFunctionName(AggregateFunction func) {
    switch (func) {
        case MAX_F: return "MAX";
        case MIN_F: return "MIN";
        case COUNT_F: return "COUNT";
        case SUM_F: return "SUM";
        case AVG_F: return "AVG";
        default: return "";
    }
}

bool syntacticParseGROUPBY() {
    logger.log("syntacticParseGROUPBY");
    
    if (tokenizedQuery.size() < 13) {
        cout << "SYNTAX ERROR: Too few arguments for GROUP BY" << endl;
        return false;
    }
    
    // Parse basic structure: Result <- GROUP BY attr FROM table ...
    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];
    
    // Check "<-" token
    if (tokenizedQuery[1] != "<-") {
        cout << "SYNTAX ERROR: Expected '<-'" << endl;
        return false;
    }
    
    // Check "GROUP" and "BY" tokens
    if (tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY") {
        cout << "SYNTAX ERROR: Expected 'GROUP BY'" << endl;
        return false;
    }
    
    // Parse grouping attribute
    parsedQuery.groupByAttribute = tokenizedQuery[4];
    
    // Check "FROM" token
    if (tokenizedQuery[5] != "FROM") {
        cout << "SYNTAX ERROR: Expected 'FROM'" << endl;
        return false;
    }
    
    // Parse source table
    parsedQuery.groupByRelationName = tokenizedQuery[6];
    
    // Check "HAVING" token
    if (tokenizedQuery[7] != "HAVING") {
        cout << "SYNTAX ERROR: Expected 'HAVING'" << endl;
        return false;
    }
    
    // Parse HAVING condition - Expecting format: AGG_FUNC(attr) op value
    string havingClause = tokenizedQuery[8];
    
    // Extract aggregate function and attribute
    size_t openParenPos = havingClause.find('(');
    size_t closeParenPos = havingClause.find(')');
    
    if (openParenPos == string::npos || closeParenPos == string::npos || 
        openParenPos >= closeParenPos || closeParenPos != havingClause.length() - 1) {
        cout << "SYNTAX ERROR: Invalid format for HAVING condition" << endl;
        return false;
    }
    
    string havingFunc = havingClause.substr(0, openParenPos);
    parsedQuery.groupByHavingAttribute = havingClause.substr(openParenPos + 1, closeParenPos - openParenPos - 1);
    
    // Convert function name to enum
    if (havingFunc == "MAX") 
        parsedQuery.groupByHavingFunc = MAX_F;
    else if (havingFunc == "MIN") 
        parsedQuery.groupByHavingFunc = MIN_F;
    else if (havingFunc == "AVG") 
        parsedQuery.groupByHavingFunc = AVG_F;
    else if (havingFunc == "SUM") 
        parsedQuery.groupByHavingFunc = SUM_F;
    else if (havingFunc == "COUNT") 
        parsedQuery.groupByHavingFunc = COUNT_F;
    else {
        cout << "SYNTAX ERROR: Invalid aggregate function in HAVING clause" << endl;
        return false;
    }
    
    // Parse binary operator
    string binaryOperator = tokenizedQuery[9];
    if (binaryOperator == ">")
        parsedQuery.groupByHavingOperator = GREATER_THAN;
    else if (binaryOperator == ">=")
        parsedQuery.groupByHavingOperator = GEQ;
    else if (binaryOperator == "<")
        parsedQuery.groupByHavingOperator = LESS_THAN;
    else if (binaryOperator == "<=")
        parsedQuery.groupByHavingOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.groupByHavingOperator = EQUAL;
    else {
        cout << "SYNTAX ERROR: Invalid binary operator in HAVING clause" << endl;
        return false;
    }
    
    // Parse HAVING value
    try {
        parsedQuery.groupByHavingValue = stoi(tokenizedQuery[10]);
    } catch (exception &e) {
        cout << "SYNTAX ERROR: Invalid numeric value in HAVING clause" << endl;
        return false;
    }
    
    // Check "RETURN" token
    if (tokenizedQuery[11] != "RETURN") {
        cout << "SYNTAX ERROR: Expected 'RETURN'" << endl;
        return false;
    }
    
    // Parse RETURN clause - Expecting format: AGG_FUNC(attr)
    string returnClause = tokenizedQuery[12];
    
    // Extract return aggregate function and attribute
    openParenPos = returnClause.find('(');
    closeParenPos = returnClause.find(')');
    
    if (openParenPos == string::npos || closeParenPos == string::npos || 
        openParenPos >= closeParenPos || closeParenPos != returnClause.length() - 1) {
        cout << "SYNTAX ERROR: Invalid format for RETURN clause" << endl;
        return false;
    }
    
    string returnFunc = returnClause.substr(0, openParenPos);
    parsedQuery.groupByReturnAttribute = returnClause.substr(openParenPos + 1, closeParenPos - openParenPos - 1);
    
    // Convert function name to enum
    if (returnFunc == "MAX") 
        parsedQuery.groupByReturnFunc = MAX_F;
    else if (returnFunc == "MIN") 
        parsedQuery.groupByReturnFunc = MIN_F;
    else if (returnFunc == "AVG") 
        parsedQuery.groupByReturnFunc = AVG_F;
    else if (returnFunc == "SUM") 
        parsedQuery.groupByReturnFunc = SUM_F;
    else if (returnFunc == "COUNT") 
        parsedQuery.groupByReturnFunc = COUNT_F;
    else {
        cout << "SYNTAX ERROR: Invalid aggregate function in RETURN clause" << endl;
        return false;
    }
    
    return true;
}

bool semanticParseGROUPBY() {
    logger.log("semanticParseGROUPBY");
    
    // Check if result table already exists
    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName)) {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }
    
    // Check if source table exists
    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Source relation doesn't exist" << endl;
        return false;
    }
    
    Table* table = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    
    // Check if grouping attribute exists in the table
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Grouping attribute doesn't exist in the source relation" << endl;
        return false;
    }
    
    // Check if HAVING attribute exists in the table
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByHavingAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: HAVING attribute doesn't exist in the source relation" << endl;
        return false;
    }
    
    // Check if RETURN attribute exists in the table
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByReturnAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: RETURN attribute doesn't exist in the source relation" << endl;
        return false;
    }
    
    return true;
}

// Helper function to evaluate the binary operator
bool evaluateBinaryOperator(int left, int right, BinaryOperator op) {
    switch (op) {
        case GREATER_THAN:
            return left > right;
        case LESS_THAN:
            return left < right;
        case GEQ:
            return left >= right;
        case LEQ:
            return left <= right;
        case EQUAL:
            return left == right;
        default:
            return false;
    }
}

// Helper function to calculate aggregate values for a group
int calculateAggregate(const vector<int>& values, AggregateFunction func) {
    if (values.empty()) return 0;
    
    switch (func) {
        case MAX_F: {
            return *max_element(values.begin(), values.end());
        }
        case MIN_F: {
            return *min_element(values.begin(), values.end());
        }
        case COUNT_F: {
            return values.size();
        }
        case SUM_F: {
            int sum = 0;
            for (int val : values) sum += val;
            return sum;
        }
        case AVG_F: {
            if (values.empty()) return 0;
            int sum = 0;
            for (int val : values) sum += val;
            return sum / values.size();
        }
        default:
            return 0;
    }
}

void executeGROUPBY() {
    logger.log("executeGROUPBY");
    
    // Create a temporary sorted table to use for grouping
    string tempTableName = parsedQuery.groupByRelationName + "_temp_sorted";
    int attempt = 0;
    while (tableCatalogue.isTable(tempTableName)) {
        tempTableName = parsedQuery.groupByRelationName + "_temp_sorted_" + to_string(++attempt);
    }
    
    // Save current query state
    auto savedQuery = parsedQuery;
    
    // Setup SORT query to sort the table by the grouping attribute
    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = parsedQuery.groupByRelationName;
    parsedQuery.sortColumns.clear();
    parsedQuery.sortColumns.push_back(make_pair(parsedQuery.groupByAttribute, "ASC"));
    
    // Execute sort (modifies the original table)
    executeSORT();
    
    // Restore query state
    parsedQuery = savedQuery;
    
    // Get source table
    Table* sourceTable = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    
    // Get column indices
    int groupColIndex = sourceTable->getColumnIndex(parsedQuery.groupByAttribute);
    int havingColIndex = sourceTable->getColumnIndex(parsedQuery.groupByHavingAttribute);
    int returnColIndex = sourceTable->getColumnIndex(parsedQuery.groupByReturnAttribute);
    
    // Create result table columns
    vector<string> resultColumns = {
        parsedQuery.groupByAttribute,
        getAggregateFunctionName(parsedQuery.groupByReturnFunc) + parsedQuery.groupByReturnAttribute
    };
    
    // Create result table
    Table* resultTable = new Table(parsedQuery.groupByResultRelationName, resultColumns);
    
    // Process the sorted table and perform grouping
    Cursor cursor = sourceTable->getCursor();
    vector<int> row = cursor.getNext();
    
    if (row.empty()) {
        cout << "Empty source table" << endl;
        delete resultTable;
        return;
    }
    
    // Initialize current group
    int currentGroupValue = row[groupColIndex];
    vector<int> havingValues;
    vector<int> returnValues;
    
    // Add first row values
    havingValues.push_back(row[havingColIndex]);
    returnValues.push_back(row[returnColIndex]);
    
    // Process all rows
    row = cursor.getNext();
    while (!row.empty()) {
        // If we found a new group
        if (row[groupColIndex] != currentGroupValue) {
            // Calculate aggregates for the completed group
            int havingResult = calculateAggregate(havingValues, parsedQuery.groupByHavingFunc);
            
            // Check if this group passes the HAVING condition
            if (evaluateBinaryOperator(havingResult, parsedQuery.groupByHavingValue, parsedQuery.groupByHavingOperator)) {
                // Calculate return aggregate
                int returnResult = calculateAggregate(returnValues, parsedQuery.groupByReturnFunc);
                
                // Write group result to output table
                vector<int> resultRow = {currentGroupValue, returnResult};
                resultTable->writeRow<int>(resultRow);
            }
            
            // Start new group
            currentGroupValue = row[groupColIndex];
            havingValues.clear();
            returnValues.clear();
        }
        
        // Add current row to the current group
        havingValues.push_back(row[havingColIndex]);
        returnValues.push_back(row[returnColIndex]);
        
        // Get next row
        row = cursor.getNext();
    }
    
    // Process the last group
    if (!havingValues.empty()) {
        int havingResult = calculateAggregate(havingValues, parsedQuery.groupByHavingFunc);
        
        if (evaluateBinaryOperator(havingResult, parsedQuery.groupByHavingValue, parsedQuery.groupByHavingOperator)) {
            int returnResult = calculateAggregate(returnValues, parsedQuery.groupByReturnFunc);
            
            vector<int> resultRow = {currentGroupValue, returnResult};
            resultTable->writeRow<int>(resultRow);
        }
    }
    
    // Blockify result table and insert into catalogue
    if (resultTable->blockify()) {
        tableCatalogue.insertTable(resultTable);
        cout << "Group By operation successful" << endl;
    } else {
        cout << "Empty Result. No groups matched the HAVING condition." << endl;
        delete resultTable;
    }
}