#ifndef _GUC_TABLES_H
#define _GUC_TABLES_H

#include "utils/guc/guc.h"

/* bit values in status field */
#define GUC_IS_IN_FILE 0x0001 /* found it in config file */

enum config_type
{
    RIPPLEC_INT,
    RIPPLEC_STRING
};

union config_var_val
{
    bool   boolval;
    int    intval;
    double realval;
    char*  stringval;
    int    enumval;
};

typedef struct config_var_value
{
    union config_var_val val;
    void*                extra;
} config_var_value;

struct config_generic
{
    /* constant fields, must be set correctly in initial value: */
    const char* name; /* name of variable - MUST BE FIRST */
    GucFlags    reload;
    const char* short_desc; /* short desc. of this variable's purpose */
    const char* long_desc;  /* long desc. of this variable's purpose */
    int         flags;      /* flag bits, see guc.h */
    /* variable fields, initialized at runtime: */
    enum config_type vartype;      /* type of variable (set only at startup) */
    int              status;       /* status bits, see below */
    GucSource        source;       /* source of the current actual value */
    GucSource        reset_source; /* source of the reset_value */
    void*            extra;        /* "extra" pointer for current actual value */
    char*            sourcefile;   /* file current setting is from (NULL if not
                                    * set in config file) */
    int sourceline;                /* line in source file */
};

struct config_int
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    int*             variable;
    int              boot_val;
    int              min;
    int              max;
    GucIntCheckHook  check_hook;
    GucIntAssignHook assign_hook;
    /* variable fields, initialized at runtime: */
    int   reset_val;
    void* reset_extra;
};

struct config_string
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    char**              variable;
    const char*         boot_val;
    GucStringCheckHook  check_hook;
    GucStringAssignHook assign_hook;
    /* variable fields, initialized at runtime: */
    char* reset_val;
    void* reset_extra;
};

#endif
