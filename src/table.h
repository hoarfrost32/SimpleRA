#ifndef TABLE_H
#define TABLE_H

#pragma once
#include "cursor.h"
#include "index.h" // <-- Include BTree header
#include <string>   // <-- Include for string
#include <vector>   // <-- Include for vector
#include <unordered_set> // <-- Include for unordered_set
#include <fstream>  // <-- Include for ostream/ofstream
#include <iostream> // <-- Include for ostream
#include <unordered_map>
#include <memory>


// Forward declare BTree to avoid circular dependency if index.h includes table.h
// class BTree;

enum IndexingStrategy
{
	BTREE,
	HASH, // HASH remains as a potential strategy type, even if not implemented
	NOTHING
};

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT).
 *
 */
class Table
{
	std::vector<std::unordered_set<int>> distinctValuesInColumns; // Make namespace explicit

public:
	string sourceFileName = "";
	string tableName = "";
	vector<string> columns;
	vector<uint> distinctValuesPerColumnCount;
	uint columnCount = 0;
	long long int rowCount = 0;
	uint blockCount = 0;
	uint maxRowsPerBlock = 0;
	vector<uint> rowsPerBlockCount;

	// --- Indexing Information ---
	bool indexed = false;             // Is the table indexed?
	string indexedColumn = "";        // Which column is indexed?
	IndexingStrategy indexingStrategy = NOTHING; // What type of index?
	BTree* index = nullptr;           // Pointer to the actual index object (if indexed)
    // int indexRootPage = -1;        // Optional: Persist root page index here if needed
    // int indexNodeCount = 0;        // Optional: Persist node count here if needed
	// --- End Indexing Information ---

	unordered_map<string, BTree*> indexes; // Maps column names to their indices

	bool extractColumnNames(string firstLine);
	bool blockify();
	void updateStatistics(vector<int> row);
	Table();
	Table(string tableName);
	Table(string tableName, vector<string> columns);
	~Table();
	bool load();
	bool isColumn(string columnName);
	void renameColumn(string fromColumnName, string toColumnName);
	void print();
	void makePermanent();
	bool isPermanent();
	void getNextPage(Cursor *cursor);
	Cursor getCursor();
	bool reload();
	int getColumnIndex(string columnName);
	void unload(); // unload needs to also handle deleting the index object

	// --- Index Management Methods ---
    bool isIndexed(const string& columnName) const;
    BTree* getIndex(const string& columnName) const;
    bool addIndex(const string& columnName, BTree* index);
    bool removeIndex(const string& columnName);
    void removeAllIndexes(); // Helper to clear all indexes
    // --- End Index Management Methods ---

	/**
	 * @brief Static function that takes a vector of valued and prints them out in a
	 * comma seperated format.
	 *
	 * @tparam T current usaages include int and string
	 * @param row
	 */
	template <typename T>
	void writeRow(vector<T> row, std::ostream &fout) // Use std::ostream
	{
		// logger.log("Table::printRow"); // Logger might not be accessible in header easily
		for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
		{
			if (columnCounter != 0)
				fout << ", ";
			fout << row[columnCounter];
		}
		fout << endl;
	}

	/**
	 * @brief Static function that takes a vector of valued and prints them out in a
	 * comma seperated format. Appends to the source file.
	 *
	 * @tparam T current usaages include int and string
	 * @param row
	 */
	template <typename T>
	void writeRow(vector<T> row)
	{
		// logger.log("Table::printRow");
		std::ofstream fout(this->sourceFileName, ios::app); // Use std::ofstream
		this->writeRow(row, fout);
		fout.close();
	}
};

#endif