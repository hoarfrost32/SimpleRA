#ifndef MATRIX_H
#define MATRIX_H

#pragma once
#include "global.h"

/**
 * @brief The Matrix class for holding a square integer matrix.
 * No block-based storage is needed unless you specifically want it.
 */
class Matrix
{
public:
	string matrixName;
	string sourceFileName; // "../data/<matrixName>.csv"
	int dimension;		   // n for an n x n matrix
	int blockCount;
	// For each block we also store how many "rows" went into that block.
	// (In row-major layout, a "row" here means a row of the matrix.)
	vector<int> rowsPerBlockCount;
	uint maxRowsPerBlock;

	Matrix() = default;
	Matrix(string matrixName);

	// Reads from the CSV, sets dimension, then calls blockify
	bool load();

	// This does the actual writing of page files to ../data/temp/<matrixName>_Page<i>
	bool blockify();

	// Helper to just figure out dimension from .csv
	bool determineMatrixDimension();

	// Removes matrix's .temp pages if needed (similar to Table::unload)
	void unload();
	void print();
	void makePermanent();
};

#endif