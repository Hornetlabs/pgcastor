#ifndef _MISC_STAT_H
#define _MISC_STAT_H

typedef struct CAPTUREBASE
{
    XLogRecPtr redolsn;
    XLogRecPtr restartlsn;
    XLogRecPtr confirmedlsn;
    TimeLineID curtlid;
    uint64     fileid;
    uint64     fileoffset;
} capturebase;

void misc_stat_decodewrite(capturebase* base, int* pfd);

void misc_capturestat_init(void);

void misc_stat_loaddecode(capturebase* decodebase);

#endif
