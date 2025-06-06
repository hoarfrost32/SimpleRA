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
bool semanticParseROTATEMATRIX();
bool semanticParseCROSSTRANSPOSE();
bool semanticParseCHECKANTISYM();
bool semanticParseORDERBY();
bool semanticParseGROUPBY();
bool semanticParseINSERT();
bool semanticParseUPDATE();
bool semanticParseDELETE();
bool semanticParseSEARCH();
bool semanticParseQUIT();

#endif
