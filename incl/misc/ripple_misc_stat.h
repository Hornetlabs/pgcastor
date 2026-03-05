#ifndef _RIPPLE_MISC_STAT_H
#define _RIPPLE_MISC_STAT_H

typedef struct RIPPLE_CAPTUREBASE
{
    XLogRecPtr          redolsn;
    XLogRecPtr          restartlsn;
    XLogRecPtr          confirmedlsn;
    TimeLineID          curtlid;
    uint64              fileid;
    uint64              fileoffset;
} ripple_capturebase;

typedef struct RIPPLE_COLLECTORBASE
{
    XLogRecPtr          redolsn;
    XLogRecPtr          restartlsn;
    XLogRecPtr          confirmedlsn;
    uint64              pfileid;
    uint64              cfileid;
    uint64              coffset;
} ripple_collectorbase;

void ripple_misc_stat_decodewrite(ripple_capturebase* base, int* pfd);

void ripple_misc_capturestat_init(void);

void ripple_misc_stat_loaddecode(ripple_capturebase* decodebase);

void ripple_misc_stat_collectorwrite(ripple_collectorbase* collectorbase, char* name, int* pfd);

void ripple_misc_collectorstat_init(char* name);

void ripple_misc_stat_loadcollector(ripple_collectorbase* collectorbase, char* name);

#endif
