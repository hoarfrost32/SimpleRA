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
	std::string matrixName;
	int n; // dimension (if n x n)

	// Store the data in memory (be mindful of large n)
	std::vector<std::vector<int>> data;

	Matrix(std::string name);
	bool load();  // loads from ../data/<matrixName>.csv
	void print(); // optional

	// Add more members as needed
};

#endif