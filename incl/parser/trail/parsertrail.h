#ifndef _PARSERTRAIL_H
#define _PARSERTRAIL_H

typedef struct PARSERTRAIL
{
    dlist*        records;
    txn*          lasttxn;
    transcache*   transcache;
    ffsmgr_state* ffsmgrstate;

    /* Committed transaction */
    dlist*        dtxns;
    cache_txn*    parser2txn;
} parsertrail;

typedef bool (*parsertrailtokenapplyfunc)(parsertrail* parsertrail, void* data);
typedef void (*parsertrailtokenclean)(parsertrail* parsertrail, void* data);

/* Clean up memory and reset content */
void parsertrail_reset(parsertrail* parsertrail);

/* Parse records into transactions */
bool parsertrail_traildecode(parsertrail* parsertrail);

/* Parse records into transactions */
bool parsertrail_parser(parsertrail* parsertrail);

void parsertrail_traildata_shiftfile(parsertrail* parsertrail);

void parsertrail_free(parsertrail* parsertrail);

#endif
