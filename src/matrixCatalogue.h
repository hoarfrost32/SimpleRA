#ifndef MATRIXCATALOGUE_H
#define MATRIXCATALOGUE_H

// matrixCatalogue.h
#pragma once
#include "matrix.h"

class Matrix;
class MatrixCatalogue
{
	std::unordered_map<std::string, Matrix *> matrices;

public:
	void insertMatrix(Matrix *matrix);
	void deleteMatrix(std::string matrixName);
	Matrix *getMatrix(std::string matrixName);
	bool isMatrix(std::string matrixName);
	void print(); // optional
	~MatrixCatalogue();
};

#endif