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

void ripple_misc_stat_decodewrite(ripple_capturebase* base, int* pfd);

void ripple_misc_capturestat_init(void);

void ripple_misc_stat_loaddecode(ripple_capturebase* decodebase);

#endif
