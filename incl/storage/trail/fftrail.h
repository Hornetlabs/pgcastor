#ifndef _FFTRAIL_H
#define _FFTRAIL_H

typedef enum FTRAIL_TOKENDATATYPE
{
    FTRAIL_TOKENDATATYPE_TINYINT = 0x00,
    FTRAIL_TOKENDATATYPE_SMALLINT = 0x01,
    FTRAIL_TOKENDATATYPE_INT = 0x02,
    FTRAIL_TOKENDATATYPE_BIGINT = 0x03,
    FTRAIL_TOKENDATATYPE_STR = 0x04
} ftrail_tokendatatype;

#define ftrail_datatype ftrail_tokendatatype

#define FTRAIL_MAGIC    0x78A555C0
#define TOKENHDRSIZE    6

/* Parameter description:
 * Input parameters:
 *      fhdr             Function header identifier
 *      tid              tokenid
 *      tinfo            tokeninfo
 *      tlen             Total length including group
 * Output parameters:
 *      buffer           Token content saved to this buffer, return new address space
 */
#define FTRAIL_TOKENHDR2BUFFER(fhdr, tid, tinfo, tlen, buffer) \
    {                                                          \
        uint8* __uptr__ = NULL;                                \
        __uptr__ = buffer;                                     \
        CONCAT(fhdr, 8bit)(&__uptr__, tid);                    \
        CONCAT(fhdr, 8bit)(&__uptr__, tinfo);                  \
        CONCAT(fhdr, 32bit)(&__uptr__, tlen);                  \
        buffer = __uptr__;                                     \
    }

/* Add group to buffer */
#define FTRAIL_GROUP2BUFFER FTRAIL_TOKENHDR2BUFFER

/*
 * Parse data in buffer according to token format
 *
 * Input parameters:
 *  buffer        Buffer content
 *
 * Output parameters:
 *  tid           Token identifier
 *  tinfo         Token info identifier
 *  tdatalen      Token length
 *  tdata         Token data content
 */
#define FTRAIL_BUFFER2TOKEN(fhdr, buffer, tid, tinfo, tdatalen, tdata) \
    {                                                                  \
        uint8* _uptr_ = NULL;                                          \
        _uptr_ = buffer;                                               \
        tid = CONCAT(fhdr, 8bit)(&_uptr_);                             \
        tinfo = CONCAT(fhdr, 8bit)(&_uptr_);                           \
        tdatalen = CONCAT(fhdr, 32bit)(&_uptr_);                       \
        tdata = _uptr_;                                                \
        buffer = (_uptr_ + tdatalen - TOKENHDRSIZE);                   \
    }

typedef enum FFTRAIL_GROUPTYPE
{
    FFTRAIL_GROUPTYPE_NOP = 0x00,     /* Invalid                         */
    FFTRAIL_GROUPTYPE_FHEADER = 0x01, /* File header                       */
    FFTRAIL_GROUPTYPE_DATA = 0x02,    /* Data information in file            */
    FFTRAIL_GROUPTYPE_RESET =
        0x03, /* Transaction reset, indicates unfinished transaction cleanup   */
    FFTRAIL_GROUPTYPE_FTAIL = 0x04, /* File tail information                    */
} fftrail_grouptype;

typedef enum FFTRAIL_INFOTYPE
{
    FFTRAIL_INFOTYPE_GROUP = 0x00, /* Identified as group */
    FFTRAIL_INFOTYPE_TOKEN = 0x01, /* Identified as token */
} fftrail_infotype;

/*-------------------------------Private information within trail file
 * begin------------------------*/

/* Key corresponding to tables hash, serialization */
typedef struct FFTRAIL_TABLE_SERIALKEY
{
    Oid dbid;
    Oid tbid;
} fftrail_table_serialkey;

/* Value corresponding to tables hash */
typedef struct FFTRAIL_TABLE_SERIALENTRY
{
    fftrail_table_serialkey key;
    uint32                  dbno;
    uint32                  tbno;
    char                    schema[NAMEDATALEN];
    char                    table[NAMEDATALEN];
} fftrail_table_serialentry;

/* Key during table hash deserialization */
typedef struct FFTRAIL_TABLE_DESERIALKEY
{
    uint32 tbnum;
} fftrail_table_deserialkey;

/* Table hash deserialization value */
typedef struct FFTRAIL_TABLE_DESERIALENTRY
{
    fftrail_table_deserialkey key;
    bool                      haspkey;
    uint16                    colcnt;
    Oid                       oid;
    uint32                    dbno;
    char                      schema[NAMEDATALEN];
    char                      table[NAMEDATALEN];
    ff_column*                columns;
} fftrail_table_deserialentry;

/* Value corresponding to databases hash serialization */
typedef struct FFTRAIL_DATABASE_SERIALENTRY
{
    Oid    oid;
    uint32 no;
    char   database[NAMEDATALEN];
} fftrail_database_serialentry;

/* Value corresponding to databases hash deserialization */
typedef struct FFTRAIL_DATABASE_DESERIALENTRY
{
    uint32 no;
    Oid    oid;
    char   database[NAMEDATALEN];
} fftrail_database_deserialentry;

typedef struct FFTRAIL_PRIVDATA
{
    uint32     dbnum;
    uint32     tbnum;
    XLogRecPtr redolsn;
    XLogRecPtr restartlsn;
    XLogRecPtr confirmlsn;

    /* Data in HTAB* tables */
    List*      tbentrys;

    /* Data in HTAB* databases */
    List*      dbentrys;
    HTAB*      tables;
    HTAB*      databases;
} fftrail_privdata;

/*-------------------------------Private information within trail file end------------------------*/

/* Initialize */
bool fftrail_init(int optype, void* state);

/* Serialize data to buffer */
bool fftrail_serial(ff_cxt_type type, void* data, void* state);

/* Deserialize data to data */
bool fftrail_deserial(ff_cxt_type type, void** data, void* state);

/* Content release */
void fftrail_free(int optype, void* state);

bool fftrail_getrecordsubtype(void* state, uint8* record, uint16* subtype);

/* Get length recorded in record header */
uint64 fftrail_getrecordlsn(void* state, uint8* record);

void fftrail_getrecordgrouptype(void* state, uint8* record, uint8* grouptype);

/* Get offset of real data based on record */
uint16 fftrail_getrecorddataoffset(int compatibility);

/* Get total length */
uint64 fftrail_getrecordtotallength(void* state, uint8* record);

/* Get record length */
uint16 fftrail_getrecordlength(void* state, uint8* record);

/* Set record length */
void fftrail_setrecordlength(void* state, uint8* record, uint16 reclength);

bool fftrail_isrecordtransstart(void* state, uint8* record);

bool fftrail_validrecord(
    ff_cxt_type type, void* state, uint8 infotype, uint64 fileid, uint8* record);

/*
 * Serialization
 * Check if need to switch block before writing data
 *  If need to switch block, execute switch operation
 */
bool fftrail_serialpreshiftblock(void* state);

/* File switch */
bool fftrail_serialshiffile(void* state);

/* Add specific data to buffer */
uint8* fftrail_body2buffer(ftrail_tokendatatype tdtype,
                           uint16               tdatalen,
                           uint8*               tdata,
                           uint8*               buffer);

/* Write specific data from buffer to data */
uint8* fftrail_buffer2body(ftrail_tokendatatype tdtype,
                           uint64               tdatalen,
                           uint8*               tdata,
                           uint8*               buffer);

/* Add token data to buffer */
uint8* fftrail_token2buffer(uint8                tid,
                            uint8                tinfo,
                            ftrail_tokendatatype tdtype,
                            uint16               tdatalen,
                            uint8*               tdata,
                            uint32*              tlen,
                            uint8*               buffer);

/* Get tail length */
int fftrail_resetlen(int compatibility);

/* Get tail length */
int fftrail_taillen(int compatibility);

/* Get tokenminsize */
int fftrail_gettokenminsize(int compatibility);

/* File initialization */
void fftrail_fileinit(void* state);

/*
 * Clean up private variable cache in file
 */
void fftrail_invalidprivdata(int optype, void* privdata);

#endif
