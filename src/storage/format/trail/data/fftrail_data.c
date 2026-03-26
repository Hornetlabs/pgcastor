#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_dbmetadata.h"
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "storage/trail/data/fftrail_txn.h"
#include "storage/trail/data/fftrail_txninsert.h"
#include "storage/trail/data/fftrail_txndelete.h"
#include "storage/trail/data/fftrail_txnupdate.h"
#include "storage/trail/data/fftrail_txnmultiinsert.h"
#include "storage/trail/data/fftrail_txnddl.h"
#include "storage/trail/data/fftrail_txncommit.h"
#include "storage/trail/data/fftrail_txnrefresh.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_begin.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_end.h"
#include "storage/trail/data/fftrail_txnonlinerefresh_increment_end.h"
#include "storage/trail/data/fftrail_txnbigtxn_begin.h"
#include "storage/trail/data/fftrail_txnbigtxn_end.h"
#include "storage/trail/data/fftrail_txnbigtxn_begin.h"
#include "storage/trail/data/fftrail_txnbigtxn_end.h"
#include "storage/trail/data/fftrail_txnonlinerefreshabandon.h"

typedef bool (*serialtoken)(void* data, void* state);

typedef bool (*deserialtoken)(void** data, void* state);

typedef struct FFTRAIL_DATATYPEMGR
{
    ff_data_type  type;
    char*         desc;
    serialtoken   serial;
    deserialtoken deserial;
} fftrail_datatypemgr;

static fftrail_datatypemgr m_datatypemgr[] = {
    {FF_DATA_TYPE_NOP, "NOP", NULL, NULL},
    {FF_DATA_TYPE_DBMETADATA, "DBMETADATA", fftrail_dbmetadata_serial, fftrail_dbmetadata_deserial},
    {FF_DATA_TYPE_TBMETADATA, "TBLEMETADATA", NULL, fftrail_tbmetadata_deserial},
    {FF_DATA_TYPE_TXN, "TXNDATA", fftrail_txn_serial, NULL},
    {FF_DATA_TYPE_DML_INSERT, "TXN INSERT", NULL, fftrail_txninsert_deserial},
    {FF_DATA_TYPE_DML_UPDATE, "TXN UPDATE", NULL, fftrail_txnupdate_deserial},
    {FF_DATA_TYPE_DML_DELETE, "TXN DELETE", NULL, fftrail_txndelete_deserial},
    {FF_DATA_TYPE_DDL_STMT, "TXN DDL STMT", NULL, fftrail_txnddl_deserial},
    {FF_DATA_TYPE_DDL_STRUCT, "TXN DDL STRUCT", NULL, NULL},
    {FF_DATA_TYPE_REC_CONTRECORD, "TXN CONTRECORD", NULL, NULL},
    {FF_DATA_TYPE_DML_MULTIINSERT, "TXN MULTIINSERT", NULL, fftrail_txnmultiinsert_deserial},
    {FF_DATA_TYPE_TXNCOMMIT, "TXN COMMIT", NULL, fftrail_txncommit_deserial},
    {FF_DATA_TYPE_REFRESH, "TXN REFRESH", NULL, fftrail_txnrefresh_deserial},
    {FF_DATA_TYPE_TXNBEGIN, "TXN BEGIN", NULL, NULL},
    {FF_DATA_TYPE_ONLINE_REFRESH_BEGIN,
     "TXN ONLINE REFRESH BEGIN",
     NULL,
     fftrail_txnonlinerefresh_begin_deserial},
    {FF_DATA_TYPE_ONLINE_REFRESH_END,
     "TXN ONLINE REFRESH END",
     NULL,
     fftrail_txnonlinerefresh_end_deserial},
    {FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END,
     "TXN ONLINE REFRESH END",
     NULL,
     fftrail_txnonlinerefresh_increment_end_deserial},
    {FF_DATA_TYPE_BIGTXN_BEGIN, "TXN BIGTRANSACTION BEGIN", NULL, fftrail_txnbigtxn_begin_deserial},
    {FF_DATA_TYPE_BIGTXN_END, "TXN BIGTRANSACTION END", NULL, fftrail_txnbigtxn_end_deserial},
    {FF_DATA_TYPE_ONLINEREFRESH_ABANDON,
     "TXN ONLINEREFRESH ABANDON",
     NULL,
     fftrail_txnonlinerefreshabandon_deserial}};

static int m_datatypmgrcnt = (sizeof(m_datatypemgr) / sizeof(fftrail_datatypemgr));

/* header length */
int fftrail_data_headlen(int compatibility)
{
    /* calculate length */
    /*
     * version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum 4 bytes
     */
    /*
     * token length itself
     * 66
     * data length
     * 42 = 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1 + 2 + 8 + 4
     */

    /* no byte alignment needed */
    return 108;
}

/* get offset of real data relative to record */
uint16 fftrail_data_getrecorddataoffset(int compatibility)
{
    return (uint16)fftrail_data_headlen(compatibility);
}

/* get total length recorded in header */
uint64 fftrail_data_gettotallengthfromhead(int compatibility, uint8* head)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint64 totallength = 0;
    uint8* uptr = NULL;

    uptr = head;

    /*
     * in v1.0 versionmiddle，inNo.5positionplacerecord totallength token
     * front 4 fieldmiddle innercontentlength: 2 + 4 + 8 + 1 = 15
     * front 4 token format length 4*TOKENHDRSIZE
     * addupper reclength itself TOKENHDRSIZE
     * byby offsetamountshould the as : 15+5*TOKENHDRSIZE
     */
    uptr += (5 * TOKENHDRSIZE + 15);

    totallength = CONCAT(get, 64bit)(&uptr);
    return totallength;
}

/* getheadpartmiddlerecord length */
uint16 fftrail_data_getreclengthfromhead(int compatibility, uint8* head)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint16 reclength = 0;
    uint8* uptr = NULL;

    uptr = head;

    /*
     * in v1.0 versionmiddle，inNo.6positionplacerecord reclength token
     * front 5 fieldmiddle innercontentlength: 2 + 4 + 8 + 1 + 8 = 23
     * front 5 token format length 5*TOKENHDRSIZE
     * addupper reclength itself TOKENHDRSIZE
     * byby offsetamountshould the as : 23+6*TOKENHDRSIZE
     */
    uptr += (6 * TOKENHDRSIZE + 23);

    reclength = CONCAT(get, 16bit)(&uptr);
    return reclength;
}

/* set reclength */
void fftrail_data_setreclengthonhead(int compatibility, uint8* head, uint16 reclength)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint8* uptr = NULL;

    uptr = head;
    /*
     * in v1.0 versionmiddle，inNo.6positionplacerecord reclength token
     * front 5 fieldmiddle innercontentlength: 2 + 4 + 8 + 1 + 8 = 23
     * front 5 token format length 5*TOKENHDRSIZE
     * addupper reclength itself TOKENHDRSIZE
     * byby offsetamountshould the as : 23+6*TOKENHDRSIZE
     */
    uptr += (6 * TOKENHDRSIZE + 23);
    CONCAT(put, 16bit)(&uptr, reclength);
}

/* getheadpartmiddlerecord LSN */
uint64 fftrail_data_getorgposfromhead(int compatibility, uint8* head)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint64 orgpos = 0;
    uint8* uptr = NULL;

    uptr = head;

    /*
     * in v1.0 versionmiddle，inNo.10positionplacerecord orgpos token
     * front 9 fieldmiddle innercontentlength: 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1 + 2 = 30
     * front 9 token format length 9*TOKENHDRSIZE
     * addupper orgpos itself TOKENHDRSIZE
     * byby offsetamountshould the as : 23+10*TOKENHDRSIZE
     */
    uptr += (10 * TOKENHDRSIZE + 30);

    orgpos = CONCAT(get, 64bit)(&uptr);
    return orgpos;
}

/* getheadpartmiddlerecord operateworktype */
uint16 fftrail_data_getsubtypefromhead(int compatibility, uint8* head)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint16 reclength = 0;
    uint8* uptr = NULL;

    uptr = head;

    /*
     * in v1.0 versionmiddle，inNo.9positionplacerecord type token
     * front 8 fieldmiddle innercontentlength: 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1= 28
     * front 8 token format length 8*TOKENHDRSIZE
     * addupper type itself TOKENHDRSIZE
     * byby offsetamountshould the as : 28+9*TOKENHDRSIZE
     */
    uptr += (9 * TOKENHDRSIZE + 28);

    reclength = CONCAT(get, 16bit)(&uptr);
    return reclength;
}

/* getheadpartmiddlerecord transind */
uint8 fftrail_data_gettransindfromhead(int compatibility, uint8* head)
{
    /*
     * contents in version 1.0
     * 2024.10.14 9 tokens
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     * dbmdno data length 2 bytes
     * tbmdno data length 4 bytes
     * transid data length 8 bytes
     * transind data length 1 byte
     * totallength total data length 8 bytes
     * reclength current record length 2 bytes
     * reccount number of data items in current record 2 bytes
     * formattype data source 1 byte
     * type operation type 2 bytes
     * orgpos initial offset 8 bytes
     * crc32 checksum
     */
    uint8  transind = 0;
    uint8* uptr = NULL;

    uptr = head;

    /*
     * in v1.0 versionmiddle，inNo.4positionplacerecord transind token
     * front 3 fieldmiddle innercontentlength: 2 + 4 + 8 = 14
     * front 3 token format length 3*TOKENHDRSIZE
     * addupper transind itself TOKENHDRSIZE
     * byby offsetamountshould the as : 14+4*TOKENHDRSIZE
     */
    uptr += (4 * TOKENHDRSIZE + 14);

    transind = CONCAT(get, 8bit)(&uptr);
    return transind;
}

/*
 * will dataaddinto buffermiddle
 * parameterdescription:
 * ffdatahdr headpartinfo，i.e. is input parameteragainis output parameter
 * ffstate writecacheblock info
 * ref_buffer cacheblock
 * dtype waitwritedata type
 * dlen waitwritedata length
 * data waitwritedata
 */
bool fftrail_data_data2buffer(ff_data*        ffdatahdr,
                              ffsmgr_state*   ffstate,
                              file_buffer**   ref_buffer,
                              ftrail_datatype dtype,
                              uint64          dlen,
                              uint8*          data)
{
    bool              shiftfile = false; /* switchswapfile */
    uint32            tlen = 0;
    int               hdrlen = 0;
    int               timeout = 0;
    FullTransactionId xid = InvalidFullTransactionId;
    uint64            nfileid = 0; /* filenumber */
    uint64            blknum = 0;  /* datafastencode */
    uint64            wbytes = 0;  /* can writedata length */
    uint64            freespc = 0; /* can useemptybetween */
    uint8*            uptr = NULL; /* address */
    file_buffer*      fbuffer = NULL;
    file_buffer*      ftmpbuffer = NULL;
    ff_fileinfo*      finfo = NULL;
    file_buffers*     txn2filebuffer = NULL;

    /* getfile_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    fbuffer = *ref_buffer;
    finfo = (ff_fileinfo*)fbuffer->privdata;

    /* dataemptybetween */
    /* checklookis whetheras lastfileblock，lastfileblock，thatcan usememoryemptybetween
     * calculatemethodnot same */
    freespc = fbuffer->maxsize - fbuffer->start;
    if (0 == freespc)
    {
        elog(RLOG_ERROR, "freespc:%lu", freespc);
    }

    /* block inner，recordis complete */
    freespc -= TOKENHDRSIZE; /* rectail */

    /* checklookis whetheras lastfileblock */
    if (finfo->blknum == (ffstate->maxbufid))
    {
        /*
         * as file completeproperty，need needaddfiletailpartidentifier
         */
        freespc -= fftrail_taillen(ffstate->compatibility); /* filetail */
        shiftfile = true;
    }

    /* adddata，emptybetweenenoughputlowerdata */
    uptr = fbuffer->data + fbuffer->start;
    if (freespc >= dlen)
    {
        /* addindata */
        fftrail_body2buffer(dtype, dlen, data, uptr);
        fbuffer->start += dlen;
        ffdatahdr->reclength += dlen;
        return true;
    }

    /* emptybetweennot sufficient，swapcalculatecan add data */
    /* need needfocus is ，whenemptybetweennot sufficientwhen，bywrite data */
    wbytes = dlen;
    if (0 < freespc)
    {
        /* remainsurplus emptybetweenbigin etc in ALIGNOF charsectionnumberwhenwriteinnercontent */
        if (MAXIMUM_ALIGNOF <= freespc)
        {
            /* writepartdividedata */
            uptr = fftrail_body2buffer(dtype, freespc, data, uptr);
            fbuffer->start += freespc;
            wbytes -= freespc;
            data += freespc; /* move */

            /* update state middle length */
            ffdatahdr->reclength += freespc;
        }
    }

    tlen = ffdatahdr->reclength;

    /* add rectail */
    FTRAIL_GROUP2BUFFER(put, TRAIL_TOKENDATA_RECTAIL, FFTRAIL_INFOTYPE_TOKEN, 0, uptr)
    fbuffer->start += TOKENHDRSIZE;
    tlen += TOKENHDRSIZE; /* rectail */

    /* addaddheadpartlengthinfo */
    hdrlen = TOKENHDRSIZE;                                  /* GROUP HEADER */
    hdrlen += fftrail_data_headlen(ffstate->compatibility); /* header length */
    tlen += hdrlen;

    /* totallength,charsectiontoalign */
    tlen = MAXALIGN(tlen);

    /* also need needtoalign */
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* addaddRecord begin identifier */
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_DATA, FFTRAIL_INFOTYPE_GROUP, tlen, ffstate->recptr)

    /* assembleRecordheadpartinfo */
    fftrail_data_hdrserail(ffdatahdr, ffstate);

    /* get current buffer middlerecord fileinfo */
    if (true == shiftfile)
    {
        nfileid = (finfo->fileid + 1);
        xid = finfo->xid;
        blknum = 1;
        ffstate->status = FFSMGR_STATUS_SHIFTFILE;
    }
    else
    {
        nfileid = finfo->fileid;
        xid = finfo->xid;
        blknum = finfo->blknum;
        blknum++;
    }

    /* such as resultswitchswap file，thatneed needsetfileendidentifierinfo */
    if (shiftfile)
    {
        ff_tail fftail = {0}; /* tail info */
        fftail.nexttrailno = nfileid;
        ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

        /* cacheclean */
        fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);
    }

    /* getnew fbuffer */
    ffstate->recptr = NULL;
    while (1)
    {
        ffstate->bufid = file_buffer_get(txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == ffstate->bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* get buffer */
    ftmpbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    rmemcpy1(&ftmpbuffer->extra, 0, &fbuffer->extra, sizeof(file_buffer_extra));

    /* will buffer addto waitbrushlinked listmiddle */
    file_buffer_waitflush_add(txn2filebuffer, fbuffer);
    fbuffer = ftmpbuffer;
    ftmpbuffer->flag = FILE_BUFFER_FLAG_DATA;

    /* set buffer privatehas info */
    if (NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc1(sizeof(ff_fileinfo));
        if (NULL == finfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(finfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ff_fileinfo*)fbuffer->privdata;
    }
    finfo->fileid = nfileid;
    finfo->blknum = blknum;
    finfo->xid = xid;
    *ref_buffer = fbuffer;

    /* checklookis whetheras file openhead，fileopenheadthataddaddfileheadand dbmetadatainfo */
    if (true == shiftfile)
    {
        fftrail_fileinit(ffstate);
    }

    /* reset ffdatahdr innercontent */
    ffdatahdr->totallength = 0;
    ffdatahdr->reccount = 1;
    ffdatahdr->reclength = 0;
    ffdatahdr->subtype = FF_DATA_TYPE_REC_CONTRECORD;

    /* reset record startbeginposition */
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* point toputdata position */
    fbuffer->start += hdrlen;

    /* recursivecall */
    return fftrail_data_data2buffer(ffdatahdr, ffstate, ref_buffer, dtype, wbytes, data);
}

/*
 * will dataslave buffer get
 * parameter:
 * ffdatahdr recordhead innercontent
 * ffstate upperlowertext
 * recoffset foundationin record begin offsetamount,mainneedused forverifychecklookcurrent record
 * is whethercontainhas enough data dtype get datatype dlen reasontheorylength data
 * datastoreput position expandbigrecoffsetand dataoffset，recordconcatenatebacklengthaddadd
 */
bool fftrail_data_buffer2data(ff_data*        ffdatahdr,
                              ffsmgr_state*   ffstate,
                              uint32*         recoffset,
                              uint32*         dataoffset,
                              ftrail_datatype dtype,
                              uint64          dlen,
                              uint8*          data)
{
    uint32 freespc = 0;
    uint8* uptr = NULL;

    /* parsedata innercontent */
    uptr = ffstate->recptr;
    uptr += (*recoffset);

    freespc = ffdatahdr->totallength - *dataoffset;
    if (dlen <= freespc)
    {
        uptr = fftrail_buffer2body(dtype, dlen, data, uptr);
        *recoffset += dlen;
        *dataoffset += dlen;
        return true;
    }

    elog(RLOG_WARNING, "buffer2data error");
    return false;
}

/*
 * assembleheadpartinfo
 */
void fftrail_data_hdrserail(ff_data* ffdatahdr, ffsmgr_state* ffstate)
{
    uint32 len = 0;
    uint8* uptr = NULL;
    uint8* crcuptr = NULL;

    /* toward buffer middleassembleinnercontent */
    crcuptr = uptr = ffstate->recptr;

    /* ffheader innercontentfillfill */
    /* addadd datanameencode */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_DBMDNO,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->dbmdno,
                                &len,
                                uptr);

    /* addadd tableencode */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TBMDNO,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffdatahdr->tbmdno,
                                &len,
                                uptr);

    /* addadd transactionnumber */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TRANSID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->transid,
                                &len,
                                uptr);

    /* addadd transactioninnersequentialorder */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TRANSIND,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_TINYINT,
                                1,
                                (uint8*)&ffdatahdr->transind,
                                &len,
                                uptr);

    /* addadd totallength */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TOTALLENGTH,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->totallength,
                                &len,
                                uptr);

    /* addadd record length */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_RECLENGTH,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->reclength,
                                &len,
                                uptr);

    /* addadd record count */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_RECCOUNT,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->reccount,
                                &len,
                                uptr);

    /* add data source */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_FORMATTYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_TINYINT,
                                1,
                                (uint8*)&ffdatahdr->formattype,
                                &len,
                                uptr);

    /* add type identifier */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->subtype,
                                &len,
                                uptr);

    /* addadd endposition offsetamount */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_ORGPOS,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->orgpos,
                                &len,
                                uptr);

    /*
     * addadd crc innercontent
     * 1、calculate crc code
     * 2、will crc codewriteto filemiddle
     */
    /* calculate crc code */
    /* (1)calculateheadpart */
    INIT_CRC32C(ffdatahdr->crc32);
    COMP_CRC32C(ffdatahdr->crc32, crcuptr, len);

    /* (2)calculatedata */
    crcuptr += fftrail_data_headlen(ffstate->compatibility);
    COMP_CRC32C(ffdatahdr->crc32, crcuptr, ffdatahdr->reclength);
    FIN_CRC32C(ffdatahdr->crc32);

    /* will crc codewriteto filemiddle */
    /* addaddcrc */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_CRC32,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffdatahdr->crc32,
                                &len,
                                uptr);
}

/*
 * reverseserializeheadpartinfo
 *
 */
bool fftrail_data_hdrdeserail(ff_data* ffdatahdr, ffsmgr_state* ffstate)
{
    uint8  tokenid = 0;      /* token identifier */
    uint8  tokeninfo = 0;    /* token details */
    uint32 tokenlen = 0;     /* token length */
    uint8* tokendata = NULL; /* token data area */

    uint8* uptr = NULL;

    /* in buffer middleassembleinnercontent */
    uptr = ffstate->recptr;

    /* getdatalibraryencode */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (TRAIL_TOKENDATAHDR_ID_DBMDNO != tokenid || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR,
             "need type %u, bu current type:%u",
             TRAIL_TOKENDATAHDR_ID_DBMDNO,
             FFTRAIL_INFOTYPE_TOKEN);
    }
    ffdatahdr->dbmdno = CONCAT(get, 16bit)(&tokendata);

    /* gettableencode */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->tbmdno = CONCAT(get, 32bit)(&tokendata);

    /* gettransaction */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->transid = CONCAT(get, 64bit)(&tokendata);

    /* gettransactionposition */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->transind = CONCAT(get, 8bit)(&tokendata);

    /* gettotallength */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->totallength = CONCAT(get, 64bit)(&tokendata);

    /* getrecordlength */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->reclength = CONCAT(get, 16bit)(&tokendata);

    /* getrecordentrynumber */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->reccount = CONCAT(get, 16bit)(&tokendata);

    /* getcomesource */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->formattype = CONCAT(get, 8bit)(&tokendata);

    /* getoperateworktype */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->subtype = CONCAT(get, 16bit)(&tokendata);

    /* getendposition offsetamount */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->orgpos = CONCAT(get, 64bit)(&tokendata);

    /* get crc */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->crc32 = CONCAT(get, 32bit)(&tokendata);

    return true;
}

/* serializedatainfo */
bool fftrail_data_serail(void* data, void* state)
{
    ff_data* ffdata = NULL;

    ffdata = (ff_data*)data;

    if (m_datatypmgrcnt < ffdata->type)
    {
        elog(RLOG_ERROR, "unknown data type:%d", ffdata->type);
    }

    if (NULL == m_datatypemgr[ffdata->type].serial)
    {
        elog(RLOG_ERROR, "%s unsupport", m_datatypemgr[ffdata->type].desc);
    }
    return m_datatypemgr[ffdata->type].serial(data, state);
}

/* determinebreakpassin datatype record is whetheras onetransaction begin */
bool fftrail_data_deserail_check_transind_start(uint8* uptr, int compatibility)
{
    uint16       subtype = 0;
    ffsmgr_state ffstate;
    ff_data      ffdata;

    rmemset1(&ffstate, 0, 0, sizeof(ffsmgr_state));
    rmemset1(&ffdata, 0, 0, sizeof(ff_data));

    /* skiptokenpartdivide */
    uptr += TOKENHDRSIZE;
    ffstate.recptr = uptr;
    subtype = fftrail_data_getsubtypefromhead(compatibility, uptr);

    /* errorchecktest */
    if (m_datatypmgrcnt < subtype)
    {
        elog(RLOG_ERROR, "unknown data type:%d", subtype);
    }

    if (NULL == m_datatypemgr[subtype].deserial)
    {
        return false;
    }

    fftrail_data_hdrdeserail(&ffdata, &ffstate);

    /* transactionbegin */
    if (FF_DATA_TRANSIND_START == (FF_DATA_TRANSIND_START & ffdata.transind))
    {
        return true;
    }

    return false;
}

/* serializedatainfo */
bool fftrail_data_deserail(void** data, void* state)
{
    /* callreverseserializeconcatenateport，parsedata */
    uint16        subtype = 0;
    uint8*        uptr = NULL;
    ffsmgr_state* ffstate = NULL;

    /* get buffer */
    ffstate = (ffsmgr_state*)state;

    uptr = ffstate->recptr;
    uptr += TOKENHDRSIZE;
    subtype = fftrail_data_getsubtypefromhead(ffstate->compatibility, uptr);

    if (m_datatypmgrcnt < subtype)
    {
        elog(RLOG_WARNING, "unknown data type:%d", subtype);
        return false;
    }

    if (NULL == m_datatypemgr[subtype].deserial)
    {
        elog(RLOG_WARNING, "%s unsupport", m_datatypemgr[subtype].desc);
        return false;
    }
    return m_datatypemgr[subtype].deserial(data, state);
}

/*
 * mostsmalllength
 */
int fftrail_data_tokenminsize(int compatibility)
{
    /*
     * token group identifier
     * token header
     * token tail
     */
    int minsize = 0;
    minsize = fftrail_data_headlen(compatibility);
    minsize += (TOKENHDRSIZE + +TOKENHDRSIZE);
    return minsize;
}
