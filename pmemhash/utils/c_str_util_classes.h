#pragma once

#include <string.h>

using namespace std;

struct hash_c_str {
	size_t operator()(const char *str)
	{
		string k(str);
		return hash<string>{}(k);
	}
};

struct equal_to_c_str {
	bool operator()(const char *k1, const char *k2)
	{
		return (strcmp(k1, k2) == 0);
	}
};
