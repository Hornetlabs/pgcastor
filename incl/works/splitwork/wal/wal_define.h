#ifndef WAL_DEFINE_H
#define WAL_DEFINE_H

#define XLOG_PAGE_MAGIC 0xD101

typedef struct XLogPageHeaderData
{
	uint16		xlp_magic;		/* magic value for correctness checks */
	uint16		xlp_info;		/* flag bits, see below */
	TimeLineID	xlp_tli;		/* TimeLineID of first record on page */
	XLogRecPtr	xlp_pageaddr;	/* XLOG address of this page */

	/*
	 * When there is not enough space on current page for whole record, we
	 * continue on the next page.  xlp_rem_len is the number of bytes
	 * remaining from a previous page.
	 *
	 * Note that xl_rem_len includes backup-block data; that is, it tracks
	 * xl_tot_len not xl_len in the initial header.  Also note that the
	 * continuation data isn't necessarily aligned.
	 */
	uint32		xlp_rem_len;	/* total len of remaining data for record */
} XLogPageHeaderData;

#define SizeOfXLogShortPHD	MAXALIGN(sizeof(XLogPageHeaderData))

typedef XLogPageHeaderData *XLogPageHeader;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)	The additional fields serve to identify the file accurately.
 */
typedef struct XLogLongPageHeaderData
{
	XLogPageHeaderData std;		/* standard header fields */
	uint64		xlp_sysid;		/* system identifier from pg_control */
	uint32		xlp_seg_size;	/* just as a cross-check */
	uint32		xlp_xlog_blcksz;	/* just as a cross-check */
} XLogLongPageHeaderData;

#define SizeOfXLogLongPHD	MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData *XLogLongPageHeader;

typedef struct XLogRecord
{
	uint32		xl_tot_len;		/* total len of entire record */
	TransactionId xl_xid;		/* xact id */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	/* 2 bytes of padding here, initialize to zero */
	pg_crc32c	xl_crc;			/* CRC for this record */

	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
} XLogRecord;

#define SizeOfXLogRecord	(offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c))

typedef enum XLOG_RMGR_ENUM
{
    XLOG_RMGR_XLOG_ID = 0,
    XLOG_RMGR_XACT_ID,
    XLOG_RMGR_SMGR_ID,
    XLOG_RMGR_CLOG_ID,
    XLOG_RMGR_DBASE_ID,
    XLOG_RMGR_TBLSPC_ID,
    XLOG_RMGR_MULTIXACT_ID,
    XLOG_RMGR_RELMAP_ID,
    XLOG_RMGR_STANDBY_ID,
    XLOG_RMGR_HEAP2_ID,
    XLOG_RMGR_HEAP_ID,
    XLOG_RMGR_BTREE_ID,
    XLOG_RMGR_HASH_ID,
    XLOG_RMGR_GIN_ID,
    XLOG_RMGR_GIST_ID,
    XLOG_RMGR_SEQ_ID,
    XLOG_RMGR_SPGIST_ID,
    XLOG_RMGR_BRIN_ID,
    XLOG_RMGR_COMMIT_TS_ID,
    XLOG_RMGR_REPLORIGIN_ID,
    XLOG_RMGR_GENERIC_ID,
    XLOG_RMGR_LOGICALMSG_ID
} XLOG_RMGR_ENUM;

#define XLR_INFO_MASK 0x0F
#define XLOG_SWITCH 0x40
#define IsSwitchXlog(rmid, info) (rmid == XLOG_RMGR_XLOG_ID && ((info & ~XLR_INFO_MASK) == XLOG_SWITCH))


#define XLogSegmentOffset(xlogptr, wal_segsz_bytes)	\
    ((xlogptr) & ((wal_segsz_bytes) - 1))

#define XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes) \
    logSegNo = (xlrp) / (wal_segsz_bytes)

#define XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes) \
    (((xlrp) / (wal_segsz_bytes)) == (logSegNo))

#define XLogSegmentsPerXLogId(wal_segsz_bytes)	\
    (UINT64CONST(0x100000000) / (wal_segsz_bytes))

#define XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)	\
    snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
             (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), \
             (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)))

#define XlogGetPagePtr(xlrp, page_size) \
    (xlrp - (xlrp % page_size))

#define IsXlogPageBegin(xlrp, page_size) ((xlrp % page_size) == 0)

#define IsXLogSegmentBegin(xlrp, wal_segsz_bytes) ((xlrp % wal_segsz_bytes) == 0)

#define GetXlogSegmentBegin(xlrp, wal_segsz_bytes) \
        (IsXLogSegmentBegin(xlrp, wal_segsz_bytes) ? xlrp : xlrp - XLogSegmentOffset(xlrp, wal_segsz_bytes))

#define GetNextXLogSegmentBegin(xlrp, wal_segsz_bytes) \
        (IsXLogSegmentBegin(xlrp, wal_segsz_bytes) ? xlrp + wal_segsz_bytes : xlrp + (xlrp % wal_segsz_bytes))

#define GetLastXlogSegmentBegin(xlrp, wal_segsz_bytes) \
        (IsXLogSegmentBegin(xlrp, wal_segsz_bytes) ? (xlrp - 2 * wal_segsz_bytes) : (xlrp - XLogSegmentOffset(xlrp, wal_segsz_bytes) - wal_segsz_bytes))

#endif /* WAL_DEFINE_H*/
