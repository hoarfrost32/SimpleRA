// matrixCatalogue.cpp
#include "global.h"

MatrixCatalogue matrixCatalogue; // define the global

void MatrixCatalogue::insertMatrix(Matrix *matrix)
{
	matrices[matrix->matrixName] = matrix;
}

bool MatrixCatalogue::isMatrix(std::string matrixName)
{
	return (matrices.find(matrixName) != matrices.end());
}

Matrix *MatrixCatalogue::getMatrix(std::string matrixName)
{
	if (!isMatrix(matrixName))
		return nullptr;
	return matrices[matrixName];
}

void MatrixCatalogue::deleteMatrix(std::string matrixName)
{
	if (!isMatrix(matrixName))
		return;
	// Optionally free memory, etc.
	delete matrices[matrixName];
	matrices.erase(matrixName);
}

MatrixCatalogue::~MatrixCatalogue()
{
	for (auto &p : matrices)
		delete p.second;
	matrices.clear();
}
