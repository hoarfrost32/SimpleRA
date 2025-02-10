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
	string sourceFileName;
	int dimension;
	int blockCount;
	// For each block we also store how many "rows" went into that block.
	// (In row-major layout, a "row" here means a row of the matrix.)
	vector<int> rowsPerBlockCount;
	uint maxRowsPerBlock;

	Matrix() = default;
	Matrix(string matrixName);

	bool load();

	bool blockify();

	bool determineMatrixDimension();

	void unload();
	void print();
	void makePermanent();
};

#endif