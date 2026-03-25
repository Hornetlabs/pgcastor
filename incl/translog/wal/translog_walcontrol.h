#ifndef _TRANSLOG_WALCONTROL_H_
#define _TRANSLOG_WALCONTROL_H_

typedef enum TRANSLOG_WALCONTROL_STAT
{
    /* Initialize */
    TRANSLOG_WALCONTROL_STAT_INIT = 0x00,

    /* Recovery status */
    TRANSLOG_WALCONTROL_STAT_RECOVERY,

    /* Working status */
    TRANSLOG_WALCONTROL_STAT_WORK,

    /* Close */
    TRANSLOG_WALCONTROL_STAT_SHUTDOWN
} translog_walcontrol_stat;

typedef struct TRANSLOG_WALCONTROL
{
    /* Status */
    translog_walcontrol_stat stat;

    /* Log size */
    uint32 segsize;

    /* Stream replication starting point */
    XLogRecPtr startpos;

    /* Timeline currently syncing */
    TimeLineID tli;

    /* Database timeline */
    TimeLineID dbtli;

    /* slotname */
    char slotname[NAMEDATALEN];

    /* restorecommand */
    char restorecmd[MAXPATH];
} translog_walcontrol;

/* Set starting lsn for stream replication */
void translog_walcontrol_setstartpos(translog_walcontrol* walctrl, XLogRecPtr lsn);

/* Set timeline for stream replication */
void translog_walcontrol_settli(translog_walcontrol* walctrl, TimeLineID tli);

/* Set current database timeline in stream replication */
void translog_walcontrol_setdbtli(translog_walcontrol* walctrl, TimeLineID tli);

/* Set starting lsn for stream replication */
void translog_walcontrol_setsegsize(translog_walcontrol* walctrl, uint32 segsize);

/* Set replication slot name */
void translog_walcontrol_setslotname(translog_walcontrol* walctrl, char* slotname);

/* Set restore command */
void translog_walcontrol_setrestorecmd(translog_walcontrol* walctrl, char* restorecmd);

/* Load Control file */
translog_walcontrol* translog_walcontrol_load(char* abspath);

/* Write Control file to disk */
bool translog_walcontrol_flush(translog_walcontrol* walctrl, char* data);

#endif
