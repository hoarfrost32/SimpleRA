#include "matrix.h"

Matrix::Matrix(std::string name)
{
	logger.log("Matrix::Matrix constructor");
	this->matrixName = name;
	this->n = 0;
}

bool Matrix::load()
{
	logger.log("Matrix::load");
	// Path to CSV
	std::string fileName = "../data/" + this->matrixName + ".csv";
	std::ifstream fin(fileName);
	if (!fin.is_open())
	{
		cout << "Error opening matrix file: " << fileName << endl;
		return false;
	}

	// Read lines until EOF
	// For each line, parse comma-separated integers
	// assuming NO header row: every row is matrix data
	std::string line;
	std::vector<std::vector<int>> tempData;
	while (std::getline(fin, line))
	{
		std::stringstream s(line);
		std::string token;
		std::vector<int> row;
		while (std::getline(s, token, ','))
		{
			row.push_back(std::stoi(token));
		}
		tempData.push_back(row);
	}

	fin.close();

	// Check it is square: #rows == #cols
	int rows = (int)tempData.size();
	if (rows == 0)
	{
		cout << "Empty matrix file." << endl;
		return false;
	}
	int cols = (int)tempData[0].size();
	if (rows != cols)
	{
		cout << "Not a square matrix: found " << rows << "x" << cols << endl;
		return false;
	}

	// Additional check: all rows must have same length
	for (int i = 1; i < rows; i++)
	{
		if ((int)tempData[i].size() != cols)
		{
			cout << "Inconsistent row length in matrix CSV." << endl;
			return false;
		}
	}

	this->n = rows;
	this->data = tempData;
	return true;
}

void Matrix::print()
{
	logger.log("Matrix::print");
	int limit = std::min(this->n, 5);
	for (int r = 0; r < limit; r++)
	{
		for (int c = 0; c < limit; c++)
		{
			cout << this->data[r][c] << " ";
		}
		cout << endl;
	}
	cout << "Matrix dimension: " << this->n << "x" << this->n << endl;
}
