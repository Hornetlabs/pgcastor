#ifndef LOADWALRECORDS_H
#define LOADWALRECORDS_H

typedef enum LOADWALRECORDS_STATUS
{
    LOADWALRECORDS_STATUS_INIT = 0,
    LOADWALRECORDS_STATUS_REWIND,
    LOADWALRECORDS_STATUS_NORMAL
} loadwalrecords_status;

typedef struct LOADWALRECORDS
{
    loadrecords loadrecords;

    /* wal file version */
    bool        need_decrypt;   /* Need decryption */
    TimeLineID  timeline;       /* Timeline */
    XLogRecPtr  block_startptr; /* lsn where block starts */
    XLogRecPtr  startptr;       /* Starting lsn */
    XLogRecPtr  endptr;         /* Ending lsn */
    XLogRecPtr  prev;           /* lsn of last divided record */

    mpage*      page; /* Read buffer */

    recordcross*
        page_last_record_incomplete;   /* (If exists) Last incomplete record of parsed page* */
    recordcross* seg_first_incomplete; /* (If exists) First incomplete record of current file* */
    recordcross*
              seg_first_incomplete_next; /* (If exists) First incomplete record of next wal file*/

    dlist*    records; /* Linked list of divided complete records */
    loadpage* loadpage;
    loadpageroutine* loadpageroutine;
} loadwalrecords;

/* Initialize */
extern loadwalrecords* loadwalrecords_init(void);

extern void loadwalrecords_free(loadwalrecords* loadrecords);

extern bool loadwalrecords_load(loadwalrecords* loadrecords);

extern void loadwalrecords_clean(loadwalrecords* loadrecords);

extern bool loadwalrecords_merge_seg_last_record(loadwalrecords* rctl);

extern bool loadwalrecords_checkend(XLogRecPtr cur, loadwalrecords* rctl);

#endif
