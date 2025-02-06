#ifndef GLOBAL_H
#define GLOBAL_H

#pragma once
#include "executor.h"
#include <regex>

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern BufferManager bufferManager;
extern MatrixCatalogue matrixCatalogue;

#endif