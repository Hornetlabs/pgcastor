#ifndef _CONTROL_H
#define _CONTROL_H

typedef struct CONTROLFILEDATA
{
    int  stat;
    Oid  database;
    char dbname[NAMEDATALEN];
    char monetary[NAMEDATALEN];
    char numeric[NAMEDATALEN];
    char timezone[NAMEDATALEN];
    char orgencoding[NAMEDATALEN];
    char dstencoding[NAMEDATALEN];
    char cahceline[CACHELINE_SIZE];
} controlfiledata;

#endif
