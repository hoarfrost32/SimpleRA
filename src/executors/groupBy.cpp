#include "../global.h"
#include <algorithm>
#include <limits>

/**
 * @brief
 * SYNTAX:
 *  Result-table <- GROUP BY <attribute1>
 *  FROM <table>
 *  HAVING <Aggregate-Func1(attribute2)> <bin-op> <attribute-value>
 *  RETURN <Aggregate-Func2(attribute3)>
 */

string getAggregateFunctionName(AggregateFunction func) {
    switch (func) {
        case MAX_F:   return "MAX";
        case MIN_F:   return "MIN";
        case COUNT_F: return "COUNT";
        case SUM_F:   return "SUM";
        case AVG_F:   return "AVG";
        default:      return "";
    }
}

bool syntacticParseGROUPBY() {
    logger.log("syntacticParseGROUPBY");

    if (tokenizedQuery.size() < 13) {
        cout << "SYNTAX ERROR: Too few arguments for GROUP BY" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];
    if (tokenizedQuery[1] != "<-") {
        cout << "SYNTAX ERROR: Expected '<-'" << endl;
        return false;
    }

    if (tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY") {
        cout << "SYNTAX ERROR: Expected 'GROUP BY'" << endl;
        return false;
    }
    parsedQuery.groupByAttribute = tokenizedQuery[4];

    if (tokenizedQuery[5] != "FROM") {
        cout << "SYNTAX ERROR: Expected 'FROM'" << endl;
        return false;
    }
    parsedQuery.groupByRelationName = tokenizedQuery[6];

    if (tokenizedQuery[7] != "HAVING") {
        cout << "SYNTAX ERROR: Expected 'HAVING'" << endl;
        return false;
    }

    string havingClause = tokenizedQuery[8];
    size_t openParenPos = havingClause.find('(');
    size_t closeParenPos = havingClause.find(')');

    if (openParenPos == string::npos || closeParenPos == string::npos ||
        openParenPos >= closeParenPos || closeParenPos != havingClause.length() - 1) {
        cout << "SYNTAX ERROR: Invalid format for HAVING condition" << endl;
        return false;
    }

    string havingFunc = havingClause.substr(0, openParenPos);
    parsedQuery.groupByHavingAttribute = havingClause.substr(openParenPos + 1,
                                         closeParenPos - openParenPos - 1);

    if (havingFunc == "MAX")      parsedQuery.groupByHavingFunc = MAX_F;
    else if (havingFunc == "MIN") parsedQuery.groupByHavingFunc = MIN_F;
    else if (havingFunc == "AVG") parsedQuery.groupByHavingFunc = AVG_F;
    else if (havingFunc == "SUM") parsedQuery.groupByHavingFunc = SUM_F;
    else if (havingFunc == "COUNT") parsedQuery.groupByHavingFunc = COUNT_F;
    else {
        cout << "SYNTAX ERROR: Invalid aggregate function in HAVING clause" << endl;
        return false;
    }

    string binOp = tokenizedQuery[9];
    if (binOp == ">")       parsedQuery.groupByHavingOperator = GREATER_THAN;
    else if (binOp == ">=") parsedQuery.groupByHavingOperator = GEQ;
    else if (binOp == "<")  parsedQuery.groupByHavingOperator = LESS_THAN;
    else if (binOp == "<=") parsedQuery.groupByHavingOperator = LEQ;
    else if (binOp == "==") parsedQuery.groupByHavingOperator = EQUAL;
    else {
        cout << "SYNTAX ERROR: Invalid binary operator in HAVING clause" << endl;
        return false;
    }

    try {
        parsedQuery.groupByHavingValue = stoi(tokenizedQuery[10]);
    } catch (...) {
        cout << "SYNTAX ERROR: Invalid numeric value in HAVING clause" << endl;
        return false;
    }

    if (tokenizedQuery[11] != "RETURN") {
        cout << "SYNTAX ERROR: Expected 'RETURN'" << endl;
        return false;
    }

    string returnClause = tokenizedQuery[12];
    openParenPos  = returnClause.find('(');
    closeParenPos = returnClause.find(')');

    if (openParenPos == string::npos || closeParenPos == string::npos ||
        openParenPos >= closeParenPos || closeParenPos != returnClause.length() - 1) {
        cout << "SYNTAX ERROR: Invalid format for RETURN clause" << endl;
        return false;
    }

    string returnFunc = returnClause.substr(0, openParenPos);
    parsedQuery.groupByReturnAttribute = returnClause.substr(openParenPos + 1,
                                            closeParenPos - openParenPos - 1);

    if (returnFunc == "MAX")       parsedQuery.groupByReturnFunc = MAX_F;
    else if (returnFunc == "MIN")  parsedQuery.groupByReturnFunc = MIN_F;
    else if (returnFunc == "AVG")  parsedQuery.groupByReturnFunc = AVG_F;
    else if (returnFunc == "SUM")  parsedQuery.groupByReturnFunc = SUM_F;
    else if (returnFunc == "COUNT")parsedQuery.groupByReturnFunc = COUNT_F;
    else {
        cout << "SYNTAX ERROR: Invalid aggregate function in RETURN clause" << endl;
        return false;
    }

    return true;
}

bool semanticParseGROUPBY() {
    logger.log("semanticParseGROUPBY");

    // 1) Check if the result table already exists
    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName)) {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    // 2) Check if the source table exists
    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Source relation doesn't exist" << endl;
        return false;
    }

    Table* table = tableCatalogue.getTable(parsedQuery.groupByRelationName);

    // 3) Check if the grouping attribute exists in the table
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Grouping attribute doesn't exist in the source relation" << endl;
        return false;
    }

    // 4) Check if HAVING attribute exists
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByHavingAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: HAVING attribute doesn't exist in the source relation" << endl;
        return false;
    }

    // 5) Check if RETURN attribute exists
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByReturnAttribute, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: RETURN attribute doesn't exist in the source relation" << endl;
        return false;
    }

    return true;
}

bool evaluateBinaryOperator(int left, int right, BinaryOperator op) {
    switch (op) {
        case GREATER_THAN: return (left >  right);
        case LESS_THAN:    return (left <  right);
        case GEQ:          return (left >= right);
        case LEQ:          return (left <= right);
        case EQUAL:        return (left == right);
        default:           return false;
    }
}

struct Aggregator {
    AggregateFunction func;
    long long sum;
    long long count;
    int minVal;
    int maxVal;

    Aggregator(AggregateFunction f)
    {
        func   = f;
        sum    = 0;
        count  = 0;
        minVal = INT_MAX;
        maxVal = INT_MIN;
    }

    // Called once for each row's relevant value
    void update(int x)
    {
        sum += x;
        count++;
        if (x < minVal) minVal = x;
        if (x > maxVal) maxVal = x;
    }

    // Called at group boundary to get final aggregator value
    int getFinal() const
    {
        switch (func)
        {
        case MAX_F:   return maxVal;
        case MIN_F:   return minVal;
        case SUM_F:   return (int) sum;
        case COUNT_F: return (int) count;
        case AVG_F:
            if (count == 0) return 0;
            return (int)(sum / count);
        default:
            return 0;
        }
    }
};

void executeGROUPBY() {
    logger.log("executeGROUPBY");

    // 1) Sort the source table on groupByAttribute (in ASC),
    auto oldQuery = parsedQuery;
    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = parsedQuery.groupByRelationName;
    parsedQuery.sortColumns.clear();
    parsedQuery.sortColumns.emplace_back(parsedQuery.groupByAttribute, "ASC");

    executeSORT(); // modifies the source table in place
    parsedQuery = oldQuery;

    // 2) Now the table is sorted by groupByAttribute
    Table* sourceTable = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    int groupColIndex  = sourceTable->getColumnIndex(parsedQuery.groupByAttribute);
    int havingColIndex = sourceTable->getColumnIndex(parsedQuery.groupByHavingAttribute);
    int returnColIndex = sourceTable->getColumnIndex(parsedQuery.groupByReturnAttribute);

    // 3) Create a new table for results
    vector<string> resultCols;
    resultCols.push_back(parsedQuery.groupByAttribute);
    string colName = getAggregateFunctionName(parsedQuery.groupByReturnFunc)
                     + parsedQuery.groupByReturnAttribute;
    resultCols.push_back(colName);

    Table* resultTable = new Table(parsedQuery.groupByResultRelationName, resultCols);

    // 4) Read rows 1 at a time. If none, done.
    Cursor cursor = sourceTable->getCursor();
    vector<int> row = cursor.getNext();
    if (row.empty())
    {
        cout << "Empty source table" << endl;
        delete resultTable;
        return;
    }

    Aggregator havingAgg(parsedQuery.groupByHavingFunc);
    Aggregator returnAgg(parsedQuery.groupByReturnFunc);

    // track the current group
    int currentGroupVal = row[groupColIndex];

    // update aggregator
    havingAgg.update(row[havingColIndex]);
    returnAgg.update(row[returnColIndex]);

    // 5) Keep reading
    row = cursor.getNext();
    while (!row.empty())
    {
        int thisGroupVal = row[groupColIndex];
        if (thisGroupVal != currentGroupVal)
        {
            // group boundary => finalize old group
            int havingResult = havingAgg.getFinal();
            if (evaluateBinaryOperator(havingResult,
                                       parsedQuery.groupByHavingValue,
                                       parsedQuery.groupByHavingOperator))
            {
                // pass => compute return aggregator
                int returnResult = returnAgg.getFinal();

                // write row: [groupVal, aggregatedVal]
                vector<int> outRow = { currentGroupVal, returnResult };
                resultTable->writeRow<int>(outRow);
            }

            // start new group
            currentGroupVal = thisGroupVal;
            havingAgg = Aggregator(parsedQuery.groupByHavingFunc);
            returnAgg = Aggregator(parsedQuery.groupByReturnFunc);
        }

        // always update aggregator
        havingAgg.update(row[havingColIndex]);
        returnAgg.update(row[returnColIndex]);

        row = cursor.getNext();
    }

    // 6) flush last group
    if (havingAgg.count > 0) {
        int havingResult = havingAgg.getFinal();
        if (evaluateBinaryOperator(havingResult,
                                   parsedQuery.groupByHavingValue,
                                   parsedQuery.groupByHavingOperator))
        {
            int returnResult = returnAgg.getFinal();
            vector<int> outRow = { currentGroupVal, returnResult };
            resultTable->writeRow<int>(outRow);
        }
    }

    // 7) blockify + insert
    if (resultTable->blockify()) {
        tableCatalogue.insertTable(resultTable);
        cout << "Group By operation successful" << endl;
    } else {
        cout << "Empty Result. No groups matched the HAVING condition." << endl;
        delete resultTable;
    }
}
