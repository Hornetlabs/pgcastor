#ifndef _RIPPLE_REGEX_H
#define _RIPPLE_REGEX_H

#define RIPPLE_POSIX_REG_EXTENDED   000001
#define RIPPLE_POSIX_REG_NOSUB      000020


typedef struct RIPPLE_REGEX
{
    bool    blike;
    char*   tokenbefore;
    char*   tokenafter;
} ripple_regex;

extern bool ripple_cmp_regexbase(ripple_regex* regex, char* name);
extern void ripple_make_regexbase(ripple_regex* regex, char* rule);
extern void ripple_free_regexbase(ripple_regex* regex);

#endif
