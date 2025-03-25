#include "../global.h"
#include <unordered_map>
#include <fstream>
#include <sstream>

/**
 * @brief
 * SYNTAX:
 *   <newRelation> <- JOIN <table1>, <table2> ON <col1> == <col2>
 *
 * Implementation: Partition Hash Join supporting only EQUI (=) condition
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
		int bucketID = (key >= 0) ? (key % numBuckets) : ((-key) % numBuckets);

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

	while (true)
	{
		vector<int> row(columnCount, 0);
		for (int i = 0; i < columnCount; i++)
		{
			if (!(fin >> row[i]))
			{
				// we are done
				row.clear();
				break;
			}
		}
		if (row.empty())
			break;
		result.push_back(row);
	}
	fin.close();
	return result;
}

// ============== SYNTAX + SEMANTIC PARSE ==============

bool syntacticParseJOIN()
{
	logger.log("syntacticParseJOIN");

	// Expect 9 tokens: R <- JOIN T1, T2 ON col1 == col2
	if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "ON")
	{
		cout << "SYNTAX ERROR" << endl;
		return false;
	}

	parsedQuery.queryType = JOIN;
	parsedQuery.joinResultRelationName = tokenizedQuery[0];
	parsedQuery.joinFirstRelationName = tokenizedQuery[3];
	parsedQuery.joinSecondRelationName = tokenizedQuery[4];
	parsedQuery.joinFirstColumnName = tokenizedQuery[6];
	parsedQuery.joinSecondColumnName = tokenizedQuery[8];

	// We only allow "=="
	string binOp = tokenizedQuery[7];
	if (binOp == "==")
		parsedQuery.joinBinaryOperator = EQUAL;
	else
	{
		cout << "SYNTAX ERROR: Only equi-join (==) is supported." << endl;
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

	// only EQUAL
	if (parsedQuery.joinBinaryOperator != EQUAL)
	{
		cout << "SEMANTIC ERROR: Only equi-join is supported" << endl;
		return false;
	}

	return true;
}

// ============== EXECUTE: PARTITION HASH JOIN ==============

void executeJOIN()
{
	logger.log("executeJOIN");

	Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
	Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

	int colIdx1 = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
	int colIdx2 = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

	// build final table schema
	vector<string> resultCols;
	for (auto &c : table1->columns)
		resultCols.push_back(c);
	for (auto &c : table2->columns)
		resultCols.push_back(c);

	Table *resultTable = new Table(parsedQuery.joinResultRelationName, resultCols);

	// We'll say 10 blocks => 9 buckets
	const int MAX_BUFFER_BLOCKS = 10;
	int numBuckets = MAX_BUFFER_BLOCKS - 1;
	if (numBuckets < 1)
		numBuckets = 1;

	// Phase 1: partition both tables
	vector<string> partFiles1 = partitionRelation(table1, colIdx1, 1, numBuckets);
	vector<string> partFiles2 = partitionRelation(table2, colIdx2, 2, numBuckets);

	// Phase 2: For each bucket i
	for (int b = 0; b < numBuckets; b++)
	{
		// read table1's bucket i
		vector<vector<int>> part1 = readBucketIntoMemory(partFiles1[b], table1->columnCount);

		// build hash map (key -> vector of rows)
		unordered_map<int, vector<vector<int>>> hashTable;
		hashTable.reserve(part1.size());
		for (auto &r1 : part1)
		{
			int k = r1[colIdx1];
			hashTable[k].push_back(r1);
		}
		part1.clear();

		// read table2's bucket i
		vector<vector<int>> part2 = readBucketIntoMemory(partFiles2[b], table2->columnCount);
		for (auto &r2 : part2)
		{
			int k = r2[colIdx2];
			if (hashTable.find(k) != hashTable.end())
			{
				// we have matching rows from table1
				auto &vecRows = hashTable[k];
				for (auto &row1 : vecRows)
				{
					// combine row1 + r2
					vector<int> outRow;
					outRow.reserve(resultTable->columnCount);

					// first row1
					for (int c = 0; c < (int)table1->columnCount; c++)
						outRow.push_back(row1[c]);

					// then r2
					for (int c = 0; c < (int)table2->columnCount; c++)
						outRow.push_back(r2[c]);

					// write
					resultTable->writeRow<int>(outRow);
				}
			}
		}
		part2.clear();
		hashTable.clear();
	}

	// finalize
	resultTable->blockify();
	tableCatalogue.insertTable(resultTable);

	// remove partition files
	for (int b = 0; b < numBuckets; b++)
	{
		remove(partFiles1[b].c_str());
		remove(partFiles2[b].c_str());
	}

	cout << "Partition Hash Join complete.\n"
		 << "New table \"" << parsedQuery.joinResultRelationName << "\" created.\n";
}
