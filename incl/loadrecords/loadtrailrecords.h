#ifndef _LOADTRAILRECORDS_H_
#define _LOADTRAILRECORDS_H_

typedef struct LOADTRAILRECOREDS
{
    loadrecords      loadrecords;

    /* Trail file compatible version */
    int              compatibility;
    uint64           fileid;
    uint64           foffset;
    recpos           orgpos;

    /* Store cross-page/file records */
    recordcross      recordcross;
    mpage*           mp;

    /* Temporary linked list for complete records, should be TAIL/HEAD/DBMETA types */
    dlist*           remainrecords;
    dlist*           records;
    loadpage*        loadpage;
    loadpageroutine* loadpageroutine;
} loadtrailrecords;

/* Initialize */
loadtrailrecords* loadtrailrecords_init(void);

/* Set method for loading trail file */
bool loadtrailrecords_setloadpageroutine(loadtrailrecords* loadrecords, loadpage_type type);

/* Set starting point for loading */
void loadtrailrecords_setloadposition(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/* Set loading path */
bool loadtrailrecords_setloadsource(loadtrailrecords* loadrecords, char* source);

/* Load records */
bool loadtrailrecords_load(loadtrailrecords* loadrecords);

/*
 * Filter record
 *  Return value description:
 *   true           Still need to continue filtering
 *   false          No need to continue filtering
 */
bool loadtrailrecords_filterfortransbegin(loadtrailrecords* loadrecords);

/* Close file descriptor */
void loadtrailrecords_fileclose(loadtrailrecords* loadrecords);

/*
 * Filter by fileid and offset, values less than this are not needed
 */
void loadtrailrecords_filter(loadtrailrecords* loadrecords, uint64 fileid, uint64 foffset);

/*
 * Filter by fileid and offset, but keep metadata
 * Return value description:
 *   true           Still need to continue filtering
 *   false          No need to continue filtering
 */
bool loadtrailrecords_filterremainmetadata(loadtrailrecords* loadrecords,
                                           uint64            fileid,
                                           uint64            foffset);

/* Release */
void loadtrailrecords_free(loadtrailrecords* loadrecords);

#endif
