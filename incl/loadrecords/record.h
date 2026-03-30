#ifndef _RECORD_H_
#define _RECORD_H_

#define RECORD_TAIL_LEN 128

/*
 * In traditional record, a normal record contains three items:
 * 1. Record header
 * 2. Data in record
 * 3. Record tail
 *
 * Trail file follows the above design
 * In wal file, only contains record header and data in record
 * So when processing wal and trail files, should note the difference here
 */

typedef enum RECORD_TYPE
{
    RECORD_TYPE_NOP = 0x00,
    RECORD_TYPE_WAL_NORMAL,
    RECORD_TYPE_WAL_CROSS,
    RECORD_TYPE_WAL_CONT,
    RECORD_TYPE_WAL_TIMELINE,
    RECORD_TYPE_TRAIL_HEADER,
    RECORD_TYPE_TRAIL_DBMETA,
    RECORD_TYPE_TRAIL_NORMAL,
    RECORD_TYPE_TRAIL_CROSS,
    RECORD_TYPE_TRAIL_CONT,
    RECORD_TYPE_TRAIL_RESET,
    RECORD_TYPE_TRAIL_TAIL
} record_type;

typedef struct RECORD
{
    int    type;
    recpos start;
    recpos end;
    recpos orgpos;

    /*
     * Data length, current record length
     *  When in trail cross record, this length will be trimmed to length excluding RECTAIL
     */
    uint64 len;

    /* Real data offset based on data header, excluding useless info like record header */
    /* Total length of valid data in data */
    uint64 totallength;

    /* Length of valid data in data */
    uint16 reallength;

    /* Offset of valid data in data based on data header */
    uint64 dataoffset;

    /* for debug */
    uint64 debugno;
    uint8* data;
} record;

typedef struct RECORDCROSS
{
    /* uint16 tail length */
    uint16  rectaillen;

    /* Total length:
     *  trail file: contains header of first crossrecord
     */
    uint64  totallen;

    /* How many bytes are needed to assemble a complete record */
    uint64  remainlen;

    /*
     * Data at tail
     *  trail: contains length of one tail
     */
    uint8   rectail[RECORD_TAIL_LEN];

    record* record;
} recordcross;

record* record_init(void);

/* record release */
void record_free(record* rec);

/* recordcross initialization */
recordcross* recordcross_init(void);

/* recordcross release */
void recordcross_free(recordcross* rec_cross);

/* record release */
void record_freevoid(void* args);

#endif
