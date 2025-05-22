#include "../global.h"
#include <unordered_map>
#include <fstream>
#include <sstream>

/**
 * @brief
 * SYNTAX:
 *   <newRelation> <- JOIN <table1>, <table2> ON <col1> <bin_op> <col2>
 *
 * Implementation:
 * - Partition Hash Join for EQUI (=) condition.
 * - Nested Loop Join for other conditions (<, >, <=, >=, !=).
 */

// ============== UTILITY HELPERS =============

static string makeBucketFileName(const string &relationName, int passNum, int bucketID)
{
	ostringstream oss;
	oss << "../data/temp/" << relationName << "_joinPass" << passNum
		<< "_Bucket" << bucketID;
	return oss.str();
}

static vector<vector<int>> readBucketIntoMemory(const string &bucketFileName, int columnCount);

static vector<string> partitionRelation(Table *table, int colIndex, int passNum, int numBuckets)
{
	vector<string> bucketFileNames(numBuckets);
	vector<ofstream> bucketOut(numBuckets);

	for (int b = 0; b < numBuckets; b++)
	{
		bucketFileNames[b] = makeBucketFileName(table->tableName, passNum, b);
		bucketOut[b].open(bucketFileNames[b], ios::trunc);
	}

	Cursor cursor = table->getCursor();
	vector<int> row = cursor.getNext();
	while (!row.empty())
	{
		int key = row[colIndex];
		int bucketID = (key >= 0) ? (key % numBuckets) : ((-key) % numBuckets); // Basic hash

		for (int c = 0; c < (int)table->columnCount; c++)
		{
			bucketOut[bucketID] << row[c];
			if (c < (int)table->columnCount - 1)
				bucketOut[bucketID] << " ";
		}
		bucketOut[bucketID] << "\n";

		row = cursor.getNext();
	}

	for (int b = 0; b < numBuckets; b++)
		bucketOut[b].close();

	return bucketFileNames;
}

static vector<vector<int>> readBucketIntoMemory(const string &bucketFileName, int columnCount)
{
	vector<vector<int>> result;
	ifstream fin(bucketFileName);
	if (!fin.is_open())
		return result;

	string line;
    while (getline(fin, line))
    {
        stringstream ss(line);
        vector<int> row(columnCount);
        bool success = true;
        for (int i = 0; i < columnCount; i++)
        {
            if (!(ss >> row[i]))
            {
                success = false; // Failed to read expected number of integers
                break;
            }
        }
        if (success && ss.rdbuf()->in_avail() == 0) { // Ensure no trailing characters
             result.push_back(row);
        } else if (success && ss.rdbuf()->in_avail() != 0) {
            //logger.log("readBucketIntoMemory: Warning - Trailing characters found in line: " + line);
        } else if (!line.empty()){ // Log if line was not empty but parsing failed
           // logger.log("readBucketIntoMemory: Warning - Could not parse line: " + line);
        }
    }
	fin.close();
	return result;
}

// Helper function to parse binary operators
static bool parseJoinBinOp(const string &tok, BinaryOperator &op)
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

// ============== SYNTAX + SEMANTIC PARSE ==============

bool syntacticParseJOIN()
{
	logger.log("syntacticParseJOIN");

	// Expect 9 tokens: R <- JOIN T1, T2 ON col1 bin_op col2
	if (tokenizedQuery.size() != 9 || tokenizedQuery[1] != "<-" || tokenizedQuery[2] != "JOIN" || tokenizedQuery[5] != "ON")
	{
		cout << "SYNTAX ERROR: Invalid JOIN format. Expected: <newRelation> <- JOIN <table1>, <table2> ON <col1> <bin_op> <col2>" << endl;
		return false;
	}

	parsedQuery.queryType = JOIN;
	parsedQuery.joinResultRelationName = tokenizedQuery[0];
	parsedQuery.joinFirstRelationName = tokenizedQuery[3];
	parsedQuery.joinSecondRelationName = tokenizedQuery[4];
	parsedQuery.joinFirstColumnName = tokenizedQuery[6];
	parsedQuery.joinSecondColumnName = tokenizedQuery[8];

	string binOpStr = tokenizedQuery[7];
	if (!parseJoinBinOp(binOpStr, parsedQuery.joinBinaryOperator))
	{
		cout << "SYNTAX ERROR: Invalid binary operator '" << binOpStr << "' in JOIN condition." << endl;
		return false;
	}

	return true;
}

bool semanticParseJOIN()
{
	logger.log("semanticParseJOIN");

	// result must not exist
	if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
	{
		cout << "SEMANTIC ERROR: Result table already exists" << endl;
		return false;
	}

	// both input tables must exist
	if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) ||
		!tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
	{
		cout << "SEMANTIC ERROR: One or both input tables do not exist" << endl;
		return false;
	}

	// columns must exist
	if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName,
										  parsedQuery.joinFirstRelationName) ||
		!tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName,
										  parsedQuery.joinSecondRelationName))
	{
		cout << "SEMANTIC ERROR: Column doesn't exist in one of the tables" << endl;
		return false;
	}

	return true;
}

// ============== EXECUTE: PARTITION HASH JOIN (for EQUAL) or NESTED LOOP JOIN ==============
void executeJOIN()
{
	logger.log("executeJOIN");

	Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
	Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

	int colIdx1 = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
	int colIdx2 = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

	// Build final table schema with robust column naming
	vector<string> resultColumnNames;
    string t1Name = table1->tableName; // Use actual table names for prefixes
    string t2Name = table2->tableName;

    // Columns from table1
    for (const string& colName : table1->columns) {
        if (table2->isColumn(colName)) {
            resultColumnNames.push_back(t1Name + "_" + colName);
        } else {
            resultColumnNames.push_back(colName);
        }
    }

    // Columns from table2
    for (const string& colName : table2->columns) {
        if (table1->isColumn(colName)) {
            resultColumnNames.push_back(t2Name + "_" + colName);
        } else {
            resultColumnNames.push_back(colName);
        }
    }
	Table *resultTable = new Table(parsedQuery.joinResultRelationName, resultColumnNames);

	if (parsedQuery.joinBinaryOperator == EQUAL)
	{
		logger.log("executeJOIN: Using Partition Hash Join for EQUI-JOIN.");
		// We'll say 10 blocks => 9 buckets
		const int MAX_BUFFER_BLOCKS_FOR_JOIN = 10; // Kept original constant for minimal change here
		int numBuckets = MAX_BUFFER_BLOCKS_FOR_JOIN - 1;
		if (numBuckets < 1) numBuckets = 1;

		// Phase 1: partition both tables
		vector<string> partFiles1 = partitionRelation(table1, colIdx1, 1, numBuckets);
		vector<string> partFiles2 = partitionRelation(table2, colIdx2, 2, numBuckets);

		// Phase 2: For each bucket i
		for (int b = 0; b < numBuckets; b++)
		{
			vector<vector<int>> part1Rows = readBucketIntoMemory(partFiles1[b], table1->columnCount);
			if (part1Rows.empty()) {
                remove(partFiles1[b].c_str()); // Clean up empty partition file
                remove(partFiles2[b].c_str()); // Clean up corresponding partition file
                continue;
            }

			unordered_map<int, vector<vector<int>>> hashTable;
			hashTable.reserve(part1Rows.size());
			for (auto &r1 : part1Rows)
			{
				int k = r1[colIdx1];
				hashTable[k].push_back(r1);
			}
			part1Rows.clear(); // Release memory

			vector<vector<int>> part2Rows = readBucketIntoMemory(partFiles2[b], table2->columnCount);
            if (part2Rows.empty()) {
                remove(partFiles1[b].c_str());
                remove(partFiles2[b].c_str());
                continue;
            }

			for (auto &r2 : part2Rows)
			{
				int k = r2[colIdx2];
				if (hashTable.count(k)) // Use .count for check, .at or [] for access
				{
					auto &vecMatchingRows1 = hashTable.at(k);
					for (auto &row1_match : vecMatchingRows1)
					{
						vector<int> outRow = row1_match; // Start with row1
						outRow.insert(outRow.end(), r2.begin(), r2.end()); // Append row2
						resultTable->writeRow<int>(outRow);
					}
				}
			}
			part2Rows.clear(); // Release memory
            remove(partFiles1[b].c_str()); // Clean up processed partition file
            remove(partFiles2[b].c_str()); // Clean up processed partition file
		}
		cout << "Partition Hash Join complete." << endl;
	}
	else // Non-equi-join, use Nested Loop Join
	{
		logger.log("executeJOIN: Using Nested Loop Join for NON-EQUI-JOIN.");
		Cursor cursor1 = table1->getCursor();
		vector<int> row1 = cursor1.getNext();

		while (!row1.empty())
		{
			Cursor cursor2 = table2->getCursor(); // Reset inner cursor for each row of outer table
			vector<int> row2 = cursor2.getNext();
			while (!row2.empty())
			{
                // Ensure column indices are valid for the rows
                if (colIdx1 < 0 || colIdx1 >= row1.size() || colIdx2 < 0 || colIdx2 >= row2.size()) {
                    logger.log("executeJOIN (NLJ): Column index out of bounds. Skipping row comparison.");
                    // This indicates a severe issue, possibly caught by semantic checks or earlier.
                    // For safety, skip this pair.
                    row2 = cursor2.getNext();
                    continue;
                }
				int val1 = row1[colIdx1];
				int val2 = row2[colIdx2];

				if (evaluateBinOp(val1, val2, parsedQuery.joinBinaryOperator))
				{
					vector<int> resultantRow = row1;
					resultantRow.insert(resultantRow.end(), row2.begin(), row2.end());
					resultTable->writeRow<int>(resultantRow);
				}
				row2 = cursor2.getNext();
			}
			row1 = cursor1.getNext();
		}
		cout << "Nested Loop Join complete." << endl;
	}

	// finalize
	if (resultTable->blockify()) {
	    tableCatalogue.insertTable(resultTable);
        cout << "JOIN operation successful. New table \"" << parsedQuery.joinResultRelationName << "\" created." << endl;
    } else {
        cout << "JOIN operation resulted in an empty table or failed to blockify." << endl;
        resultTable->unload(); // Clean up temp file if empty
        delete resultTable;
    }
}

