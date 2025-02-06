#ifndef SEMANTICPARSER_H
#define SEMANTICPARSER_H

#pragma once
#include "syntacticParser.h"

bool semanticParse();

bool semanticParseCLEAR();
bool semanticParseCROSS();
bool semanticParseDISTINCT();
bool semanticParseEXPORT();
bool semanticParseINDEX();
bool semanticParseJOIN();
bool semanticParseLIST();
bool semanticParseLOAD();
bool semanticParsePRINT();
bool semanticParsePROJECTION();
bool semanticParseRENAME();
bool semanticParseSELECTION();
bool semanticParseSORT();
bool semanticParseSOURCE();
bool semanticParseLOADMATRIX();
bool semanticParsePRINTMATRIX();
bool semanticParseEXPORTMATRIX();

#endif
