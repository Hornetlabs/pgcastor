#ifndef _STORAGE_FFSMGR_H
#define _STORAGE_FFSMGR_H

typedef enum FFSMGR_IF_TYPE
{
    FFSMG_IF_TYPE_TRAIL = 0x00
} ffsmgr_if_type;

typedef enum FFSMGR_STATUS
{
    FFSMGR_STATUS_NOP = 0x00,
    FFSMGR_STATUS_INIT = 0x01,     /* Initialize               */
    FFSMGR_STATUS_USED = 0x02,     /* Available   */
    FFSMGR_STATUS_SHIFTFILE = 0x03 /* File switch   */
} ffsmgr_status;

typedef enum FFSMGR_IF_OPTYPE
{
    FFSMGR_IF_OPTYPE_SERIAL = 0x01,
    FFSMGR_IF_OPTYPE_DESERIAL = 0x02
} ffsmgr_if_optype;

typedef struct FFSMGR_IF
{
    /* Initialize */
    bool (*ffsmgr_init)(int optype, void* state);

    /* Serialize header */
    bool (*ffsmgr_serial)(ff_cxt_type type, void* data, void* state);

    /* Deserialize header */
    bool (*ffsmgr_deserial)(ff_cxt_type type, void** data, void* state);

    /* Content release */
    void (*ffsmgr_free)(int type, void* state);

    /* Get minimum value */
    int (*ffsmg_gettokenminsize)(int compatibility);

    /* Get tail value */
    int (*ffsmg_gettailsize)(int compatibility);

    /* Used to verify correctness of record */
    bool (*ffsmgr_validrecord)(ff_cxt_type type, void* state, uint8 infotype, uint64 fileid,
                               uint8* record);

    /* Get grouptype recorded in record */
    void (*ffsmgr_getrecordgrouptype)(void* state, uint8* record, uint8* grouptype);

    /* Get subtype recorded in record */
    bool (*ffsmgr_getrecordsubtype)(void* state, uint8* record, uint16* subtype);

    /* Get lsn recorded in record */
    uint64 (*ffsmgr_getrecordlsn)(void* state, uint8* record);

    /* Get offset of real data based on record header */
    uint16 (*ffsmgr_getrecorddataoffset)(int compatibility);

    /* Get total length */
    uint64 (*ffsmgr_getrecordtotallength)(void* state, uint8* record);

    /* Get record length */
    uint16 (*ffsmgr_getrecordlength)(void* state, uint8* record);

    /* Set record length */
    void (*ffsmgr_setrecordlength)(void* state, uint8* record, uint16 reclength);

    /* Whether it is a record of transaction begin */
    bool (*ffsmgr_isrecordtransstart)(void* state, uint8* record);

} ffsmgr_if;

typedef struct FFSMGR_FDATA
{
    void* ffdata;    /* Formatted structure implementation, internally maintained data content */
                     /*
                      * TAIL format file description:
                      * Serialization:
                      *      fftrail_privdata
                      *
                      * Deserialization:
                      *      fftrail_privdata
                      */
    void* ffdata2;   /* Formatted structure implementation, internally maintained data content */
                     /*
                      * TAIL format file description:
                      * Serialization:
                      *      System dictionary
                      *
                      * Deserialization:
                      *      fftrail_privdata
                      *      During deserialization, file switching may be involved during parsing, then need to
                      * temporarily record dictionary information in new file here
                      */
    void* extradata; /* Formatted structure implementation, required external data structure */
                     /*
                      * TRAIL format file description
                      * Serialization:
                      *      transcache
                      * Deserialization:
                      *      ListCell
                      *          | cacherecord
                      */
} ffsmgr_fdata;

typedef struct FFSMGR_STATE_CALLBACK
{
    /* Get database identifier */
    Oid (*getdboid)(void* serial);

    /* Get database name */
    char* (*getdbname)(void* serial, Oid dboid);

    /* Reset dbid */
    void (*setdboid)(void* serial, Oid dboid);

    /*Get recordcache callback parser thread */
    void* (*getrecords)(void* parser);

    /* Get parser thread status */
    int (*getparserstate)(void* parser);

    /*Get txn2filebuffer serialization*/
    file_buffers* (*getfilebuffer)(void* serial);

    /*Get redosysdicts serialization*/
    void (*setredosysdicts)(void* serial, void* catalogdata);

    /*Append dataset to list */
    void (*setonlinerefreshdataset)(void* serial, void* dataset);

    /* Get pg_namespace data by oid */
    void* (*getnamespace)(void* serial, Oid oid);

    /* Get pg_class data by oid */
    void* (*getclass)(void* serial, Oid oid);

    /* Get pg_index data by oid */
    void* (*getindex)(void* serial, Oid oid);

    /* Get attributes by oid */
    void* (*getattributes)(void* serial, Oid oid);

    /* Get pg_type by oid */
    void* (*gettype)(void* serial, Oid oid);

    /* Release attributes list, for big transactions only */
    void (*freeattributes)(void* attrs);

    /* Apply to system catalog, replacing fftrail_txnmetadata call */
    void (*catalog2transcache)(void* serial, void* catalog);

} ffsmgr_state_callback;

typedef struct FFSMGR_STATE
{
    int   compatibility;
    int   bufid; /* Cache identifier in corresponding file_buffer  */
    int   maxbufid;
    void* privdata; /* External structure, deserialization is parserwork_traildecodecontext
                     *            Serialization is serialstate
                     */
    uint8*                recptr; /* Starting position of record being assembled/parsed        */
    ffsmgr_status         status;
    ffsmgr_fdata*         fdata;
    ffsmgr_if*            ffsmgr;
    ffsmgr_state_callback callback;
} ffsmgr_state;

/* Initialize, set formatting interface to use */
void ffsmgr_init(ffsmgr_if_type fftype, ffsmgr_state* ffsmgrstate);

/* Header structure initialization */
void* ffsmgr_headinit(int compatibility, FullTransactionId xid, uint64 fileid);

/* InitializeDatabase information */
void* ffsmgr_dbmetadatainit(char* dbname);

#endif
