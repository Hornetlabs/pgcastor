#ifndef _REGEX_H
#define _REGEX_H

#define POSIX_REG_EXTENDED 000001
#define POSIX_REG_NOSUB 000020

typedef struct REGEX
{
    bool  blike;
    char* tokenbefore;
    char* tokenafter;
} regex;

extern bool cmp_regexbase(regex* regex, char* name);
extern void make_regexbase(regex* regex, char* rule);
extern void free_regexbase(regex* regex);

#endif
