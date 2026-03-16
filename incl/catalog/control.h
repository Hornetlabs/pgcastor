#ifndef _RIPPLE_CONTROL_H
#define _RIPPLE_CONTROL_H

typedef struct RIPPLE_CONTROLFILEDATA
{
    int                     stat;
    Oid                     database;
    char                    dbname[NAMEDATALEN];
    char                    monetary[NAMEDATALEN];
    char                    numeric[NAMEDATALEN];
    char                    timezone[NAMEDATALEN];
    char                    orgencoding[NAMEDATALEN];
    char                    dstencoding[NAMEDATALEN];
    char                    cahceline[RIPPLE_CACHELINE_SIZE];
} ripple_controlfiledata;

#endif
