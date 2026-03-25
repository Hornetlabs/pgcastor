#ifndef _REBUILD_BURST_H
#define _REBUILD_BURST_H

#define REBUILD_BURSTNODEFLAG_NO 0x01 /* No primary key/unique index */
#define REBUILD_BURSTNODEFLAG_INDEX \
    0x02 /* Has primary key/unique index, do burst elimination at index */
#define REBUILD_BURSTNODEFLAG_NOINDEX 0x04 /* Use PBE mode */

typedef enum REBUILD_BURSTNODETYPE
{
    REBUILD_BURSTNODETYPE_NOP = 0x00,
    REBUILD_BURSTNODETYPE_DML,
    REBUILD_BURSTNODETYPE_OTHER
} rebuild_burstnodetype;

typedef enum REBUILD_BURSTROWFLAG
{
    REBUILD_BURSTROWFLAG_NOP = 0x00,
    REBUILD_BURSTROWFLAG_CHANGECONSKEY,
    REBUILD_BURSTROWFLAG_REMOVEDELETE /* Delete split from update with constraint needs to be
                                         removed */
} rebuild_burstrowflag;

typedef enum REBUILD_BURSTROWTYPE
{
    REBUILD_BURSTROWTYPE_INVALID = 0x00,
    REBUILD_BURSTROWTYPE_INSERT,
    REBUILD_BURSTROWTYPE_UPDATE,
    REBUILD_BURSTROWTYPE_DELETE
} rebuild_burstrowtype;

typedef struct REBUILD_BURSTCOLUMN
{
    int    colno;   /* Column index, index in table */
    uint32 coltype; /* Column type */
    char*  colname; /* Column name */
} rebuild_burstcolumn;

typedef struct REBUILD_BURSTROW
{
    rebuild_burstrowtype     op;   /* Operation type  */
    rebuild_burstrowflag     flag; /* Modified column type constraint/other  */
    int                      missingmapsize;
    int                      missingcnt; /* Missing column count */
    uint8*                   missingmap; /* Missing columns */
    uint8                    md5[16];
    struct REBUILD_BURSTROW* relatedrow; /* Associated statements split from update insert --->
                                            delete, delete ---> insert  */
    void* row;                           /* Original data pg_praser_translog_tbcol_values */
} rebuild_burstrow;

typedef struct REBUILD_BURSTTABLE
{
    Oid                  oid;
    int                  keycnt; /* Primary key count */
    uint64               no;     /* Number */
    char*                schema; /* Schema name */
    char*                table;  /* Table name */
    rebuild_burstcolumn* keys;   /* Primary key columns */
} rebuild_bursttable;

typedef struct REBUILD_BURSTNODE
{
    int                   flag;         /* Operation mode  */
    rebuild_burstnodetype type;         /* Type  */
    rebuild_bursttable    table;        /* Table information */
    dlist*                dlinsertrows; /* rebuild_burstrow */
    dlist*                dldeleterows; /* rebuild_burstrow */
    void*                 stmt;         /* meta/ddl etc types, reserved is: txnstmt */
} rebuild_burstnode;

typedef struct REBUILD_BURST
{
    uint64 number;
    dlist* dlbursttable; /* rebuild_bursttable */
    dlist* dlburstnodes; /* rebuild_burstnode */
} rebuild_burst;

/* burstcolumn initialization */
extern rebuild_burstcolumn* rebuild_burstcolumn_init(int colcnt);

/* burstrow initialization */
extern rebuild_burstrow* rebuild_burstrow_init(int colcnt);

/* bursttable initialization */
extern rebuild_bursttable* rebuild_bursttable_init(void);

/* burstnode initialization */
extern rebuild_burstnode* rebuild_burstnode_init(void);

/* burst initialization */
extern rebuild_burst* rebuild_burst_init(void);

/* bursttable comparison function */
extern int rebuild_bursttable_cmp(void* s1, void* s2);

/* burstnode and bursttable comparison function */
extern int rebuild_burstnode_tablecmp(void* s1, void* s2);

/* Get burst node */
extern bool rebuild_burst_getnode(HTAB* hclass, HTAB* hattrs, HTAB* hindex, rebuild_burst* burst,
                                  rebuild_burstnode** pburstnode, rebuild_bursttable* bursttable);

/* Split update into insert/delete */
extern bool rebuild_burst_decomposeupdate(rebuild_burstnode* burstnode, rebuild_burstrow** delrow,
                                          rebuild_burstrow** insertrow, void* rows);

/*
 * Merge insert/delete
 * Return true indicates merge succeeded, return false indicates merge failed
 */
extern bool rebuild_burst_mergeinsert(rebuild_burstnode* pburstnode, rebuild_burstrow* insertrow);

/*
 * Merge delete/insert
 * Return true indicates merge succeeded, return false indicates merge failed
 */
extern bool rebuild_burst_mergedelete(rebuild_burstnode* pburstnode, rebuild_burstrow* delrow);
/*
 * updateMerge delete/insert
 * Parameter description: delrow before value split from update
 *
 * Return value description: Return true indicates merge succeeded, return false indicates merge
 * failed : in_updaterow merge success returns matched insertrow, failure updaterow : error false
 * execution failed exit, true execution succeeded
 */
extern bool rebuild_burst_updatemergedelete(rebuild_burstnode* burstnode, rebuild_burstrow* delrow,
                                            rebuild_burstrow** updaterow, bool* error);

/* burstnode splice statement */
extern bool rebuild_burst_bursts2stmt(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn);

/* Reorganize txn content into burst */
extern bool rebuild_burst_txn2bursts(rebuild_burst* burst, cache_sysdicts* sysdicts, txn* txn);

/* burstcolumn resource release */
extern void rebuild_burstcolumn_free(rebuild_burstcolumn* burstcolumn, int colcnt);

/* burstrow resource release */
extern void rebuild_burstrow_free(void* args);

/* bursttable resource release, not releasing bursttable in function */
extern void rebuild_bursttable_free(void* args);

/* burstnode resource release */
extern void rebuild_burstnode_free(void* args);

/* burst resource release */
extern void rebuild_burst_free(void* args);

#endif
