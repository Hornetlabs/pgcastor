#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
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
    ff_data_type                 type;
    char*                               desc;
    serialtoken                         serial;
    deserialtoken                       deserial;
} fftrail_datatypemgr;


static fftrail_datatypemgr       m_datatypemgr[] = 
{
    {
        FF_DATA_TYPE_NOP,
        "NOP",
        NULL,
        NULL
    },
    {
        FF_DATA_TYPE_DBMETADATA,
        "DBMETADATA",
        fftrail_dbmetadata_serial,
        fftrail_dbmetadata_deserial
    },
    {
        FF_DATA_TYPE_TBMETADATA,
        "TBLEMETADATA",
        NULL,
        fftrail_tbmetadata_deserial
    },
    {
        FF_DATA_TYPE_TXN,
        "TXNDATA",
        fftrail_txn_serial,
        NULL
    },
    {
        FF_DATA_TYPE_DML_INSERT,
        "TXN INSERT",
        NULL,
        fftrail_txninsert_deserial
    },
    {
        FF_DATA_TYPE_DML_UPDATE,
        "TXN UPDATE",
        NULL,
        fftrail_txnupdate_deserial
    },
    {
        FF_DATA_TYPE_DML_DELETE,
        "TXN DELETE",
        NULL,
        fftrail_txndelete_deserial
    },
    {
        FF_DATA_TYPE_DDL_STMT,
        "TXN DDL STMT",
        NULL,
        fftrail_txnddl_deserial
    },
    {
        FF_DATA_TYPE_DDL_STRUCT,
        "TXN DDL STRUCT",
        NULL,
        NULL
    },
    {
        FF_DATA_TYPE_REC_CONTRECORD,
        "TXN CONTRECORD",
        NULL,
        NULL
    },
    {
        FF_DATA_TYPE_DML_MULTIINSERT,
        "TXN MULTIINSERT",
        NULL,
        fftrail_txnmultiinsert_deserial
    },
    {
        FF_DATA_TYPE_TXNCOMMIT,
        "TXN COMMIT",
        NULL,
        fftrail_txncommit_deserial
    },
    {
        FF_DATA_TYPE_REFRESH,
        "TXN REFRESH",
        NULL,
        fftrail_txnrefresh_deserial
    },
    {
        FF_DATA_TYPE_TXNBEGIN,
        "TXN BEGIN",
        NULL,
        NULL
    },
    {
        FF_DATA_TYPE_ONLINE_REFRESH_BEGIN,
        "TXN ONLINE REFRESH BEGIN",
        NULL,
        fftrail_txnonlinerefresh_begin_deserial
    },
    {
        FF_DATA_TYPE_ONLINE_REFRESH_END,
        "TXN ONLINE REFRESH END",
        NULL,
        fftrail_txnonlinerefresh_end_deserial
    },
    {
        FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END,
        "TXN ONLINE REFRESH END",
        NULL,
        fftrail_txnonlinerefresh_increment_end_deserial
    },
    {
        FF_DATA_TYPE_BIGTXN_BEGIN,
        "TXN BIGTRANSACTION BEGIN",
        NULL,
        fftrail_txnbigtxn_begin_deserial
    },
    {
        FF_DATA_TYPE_BIGTXN_END,
        "TXN BIGTRANSACTION END",
        NULL,
        fftrail_txnbigtxn_end_deserial
    },
    {
        FF_DATA_TYPE_ONLINEREFRESH_ABANDON,
        "TXN ONLINEREFRESH ABANDON",
        NULL,
        fftrail_txnonlinerefreshabandon_deserial
    }
};


static int      m_datatypmgrcnt = (sizeof(m_datatypemgr) / sizeof(fftrail_datatypemgr));

/* 头部长度 */
int fftrail_data_headlen(int compatibility)
{
    /* 计算长度 */
    /* 
     * version 1.0
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum 校验码 4 字节
    */
    /* 
     * token 本身的长度
     *  66
     * 数据的长度
     *  42 = 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1 + 2 + 8 + 4
     */

    /* 无需字节对齐 */
    return 108;
}

/* 获取真实数据基于 record 的偏移 */
uint16 fftrail_data_getrecorddataoffset(int compatibility)
{
    return (uint16)fftrail_data_headlen(compatibility);
}

/* 获取头部中记录的总长度 */
uint64 fftrail_data_gettotallengthfromhead(int compatibility, uint8* head)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint64  totallength = 0;
    uint8*  uptr = NULL;

    uptr = head;

    /*
     * 在 v1.0 版本中，在第5个位置处记录 totallength 的token
     *  前 4 个字段中的内容长度: 2 + 4 + 8 + 1 = 15
     *  前 4 个 token 格式的长度 4*TOKENHDRSIZE
     *  加上 reclength 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 15+5*TOKENHDRSIZE
     */
    uptr += (5*TOKENHDRSIZE + 15);

    totallength = CONCAT(get, 64bit)(&uptr);
    return totallength;
}

/* 获取头部中记录的长度 */
uint16 fftrail_data_getreclengthfromhead(int compatibility, uint8* head)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint16  reclength = 0;
    uint8*  uptr = NULL;

    uptr = head;

    /*
     * 在 v1.0 版本中，在第6个位置处记录 reclength 的token
     *  前 5 个字段中的内容长度: 2 + 4 + 8 + 1 + 8 = 23
     *  前 5 个 token 格式的长度 5*TOKENHDRSIZE
     *  加上 reclength 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 23+6*TOKENHDRSIZE
     */
    uptr += (6*TOKENHDRSIZE + 23);

    reclength = CONCAT(get, 16bit)(&uptr);
    return reclength;
}

/* 设置 reclength */
void fftrail_data_setreclengthonhead(int compatibility, uint8* head, uint16 reclength)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint8*  uptr = NULL;

    uptr = head;
    /*
     * 在 v1.0 版本中，在第6个位置处记录 reclength 的token
     *  前 5 个字段中的内容长度: 2 + 4 + 8 + 1 + 8 = 23
     *  前 5 个 token 格式的长度 5*TOKENHDRSIZE
     *  加上 reclength 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 23+6*TOKENHDRSIZE
     */
    uptr += (6*TOKENHDRSIZE + 23);
    CONCAT(put, 16bit)(&uptr, reclength);
}

/* 获取头部中记录的LSN */
uint64 fftrail_data_getorgposfromhead(int compatibility, uint8* head)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint64  orgpos = 0;
    uint8*  uptr = NULL;

    uptr = head;

    /*
     * 在 v1.0 版本中，在第10个位置处记录 orgpos 的token
     *  前 9 个字段中的内容长度: 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1 + 2 = 30
     *  前 9 个 token 格式的长度 9*TOKENHDRSIZE
     *  加上 orgpos 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 23+10*TOKENHDRSIZE
     */
    uptr += (10*TOKENHDRSIZE + 30);

    orgpos = CONCAT(get, 64bit)(&uptr);
    return orgpos;
}

/* 获取头部中记录的操作类型 */
uint16 fftrail_data_getsubtypefromhead(int compatibility, uint8* head)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint16  reclength = 0;
    uint8*  uptr = NULL;

    uptr = head;

    /*
     * 在 v1.0 版本中，在第9个位置处记录 type 的token
     *  前 8 个字段中的内容长度: 2 + 4 + 8 + 1 + 8 + 2 + 2 + 1= 28
     *  前 8 个 token 格式的长度 8*TOKENHDRSIZE
     *  加上 type 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 28+9*TOKENHDRSIZE
     */
    uptr += (9*TOKENHDRSIZE + 28);

    reclength = CONCAT(get, 16bit)(&uptr);
    return reclength;
}

/* 获取头部中记录的transind */
uint8 fftrail_data_gettransindfromhead(int compatibility, uint8* head)
{
    /* 
     * version 1.0 中的内容
     * 2024.10.14 9 个token
     * 2024.12.04 crc32 token
     * 2024.12.09 orgpos token
     *  dbmdno                  数据长度 2 字节
     *  tbmdno                  数据长度 4 字节
     *  transid                 数据长度 8 字节
     *  transind                数据长度 1 字节
     *  totallength             数据总长度 8 字节
     *  reclength               当前record的长度 2 字节
     *  reccount                当前 record 中记录的数据的个数 2 字节
     *  formattype              数据的来源  1 字节
     *  type                    操作类型 2 字节
     *  orgpos                  初始偏移量 8 字节
     *  crc32                   checksum
     */
    uint8  transind = 0;
    uint8*  uptr = NULL;

    uptr = head;

    /*
     * 在 v1.0 版本中，在第4个位置处记录 transind 的token
     *  前 3 个字段中的内容长度: 2 + 4 + 8 = 14
     *  前 3 个 token 格式的长度 3*TOKENHDRSIZE
     *  加上 transind 本身的 TOKENHDRSIZE
     *  所以偏移量应该为: 14+4*TOKENHDRSIZE
     */
    uptr += (4*TOKENHDRSIZE + 14);

    transind = CONCAT(get,8bit)(&uptr);
    return transind;
}

/*
 * 将数据加入到buffer中
 * 参数说明:
 *  ffdatahdr               头部信息，即是入参又是出参
 *  ffstate                 写缓存块的信息
 *  ref_buffer              缓存块
 *  dtype                   待写数据的类型
 *  dlen                    待写数据的长度
 *  data                    待写数据
*/
bool fftrail_data_data2buffer(ff_data* ffdatahdr,
                                        ffsmgr_state* ffstate,
                                        file_buffer** ref_buffer,
                                        ftrail_datatype dtype,
                                        uint64 dlen,
                                        uint8* data)
{
    bool shiftfile = false;                         /* 切换文件 */
    uint32 tlen = 0;
    int hdrlen = 0;
    int timeout = 0;
    FullTransactionId xid = InvalidFullTransactionId;
    uint64 nfileid = 0;                             /* 文件号 */
    uint64 blknum = 0;                              /* 数据快编码 */
    uint64 wbytes = 0;                              /* 可写数据的长度 */
    uint64 freespc= 0;                              /* 可用空间 */
    uint8* uptr = NULL;                             /* 地址 */
    file_buffer* fbuffer = NULL;
    file_buffer* ftmpbuffer = NULL;
    ff_fileinfo* finfo = NULL;
    file_buffers* txn2filebuffer = NULL;

    /* 获取file_buffers */
    txn2filebuffer = ffstate->callback.getfilebuffer(ffstate->privdata);

    fbuffer = *ref_buffer;
    finfo = (ff_fileinfo*)fbuffer->privdata;

    /* 数据空间 */
    /* 查看是否为最后一个文件块，最后一个文件块，那么可用内存空间的算法不同 */
    freespc = fbuffer->maxsize - fbuffer->start;
    if(0 == freespc)
    {
        elog(RLOG_ERROR, "freespc:%lu", freespc);
    }

    /* block 内，record是完整的 */
    freespc -= TOKENHDRSIZE; /* rectail */

    /* 查看是否为最后一个文件块 */
    if(finfo->blknum == (ffstate->maxbufid))
    {
        /* 
         * 为了文件的完整性，需要添加文件尾部标识
         */
        freespc -= fftrail_taillen(ffstate->compatibility); /* filetail */
        shiftfile = true;
    }

    /* 添加数据，空间足够放下数据 */
    uptr = fbuffer->data + fbuffer->start;
    if(freespc >= dlen)
    {
        /* 加入数据 */
        fftrail_body2buffer(dtype, dlen, data, uptr);
        fbuffer->start += dlen;
        ffdatahdr->reclength += dlen;
        return true;
    }

    /* 空间不足，换算可添加的数据 */
    /* 需要关注的是，当空间不足时，所写入的数据 */
    wbytes = dlen;
    if(0 < freespc)
    {
        /* 剩余的空间大于等于 ALIGNOF 的字节数时写入内容 */
        if(MAXIMUM_ALIGNOF <= freespc)
        {
            /* 写入部分数据 */
            uptr = fftrail_body2buffer(dtype, freespc, data, uptr);
            fbuffer->start += freespc;
            wbytes -= freespc;
            data += freespc;                        /* 移动 */

            /* 更新 state 中的长度 */
            ffdatahdr->reclength += freespc;
        }
    }

    tlen = ffdatahdr->reclength;

    /* 添加 rectail */
    FTRAIL_GROUP2BUFFER(put,
                                TRAIL_TOKENDATA_RECTAIL,
                                FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)
    fbuffer->start += TOKENHDRSIZE;
    tlen += TOKENHDRSIZE;            /* rectail */

    /* 增加头部长度信息 */
    hdrlen = TOKENHDRSIZE;               /* GROUP HEADER */
    hdrlen += fftrail_data_headlen(ffstate->compatibility);       /* 头部长度 */
    tlen += hdrlen;

    /* 总长度,字节对齐 */
    tlen = MAXALIGN(tlen);

    /* 也需要对齐 */
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* 增加Record 开始的标识 */
    FTRAIL_GROUP2BUFFER(put,
                                FFTRAIL_GROUPTYPE_DATA,
                                FFTRAIL_INFOTYPE_GROUP,
                                tlen,
                                ffstate->recptr)

    /* 组装Record头部信息 */
    fftrail_data_hdrserail(ffdatahdr, ffstate);

    /* 获取 当前 buffer 中记录的文件信息 */
    if(true == shiftfile)
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

    /* 如果切换了文件，那么需要设置文件结束标识信息 */
    if(shiftfile)
    {
        ff_tail fftail = { 0 };                  /* tail 信息 */
        fftail.nexttrailno = nfileid;
        ffstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_FTAIL, &fftail, ffstate);

        /* 缓存清理 */
        fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_SERIAL, ffstate->fdata->ffdata);
    }

    /* 获取新的 fbuffer */
    ffstate->recptr = NULL;
    while(1)
    {
        ffstate->bufid = file_buffer_get(txn2filebuffer, &timeout);
        if(INVALID_BUFFERID == ffstate->bufid)
        {
            if(ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    /* 获取 buffer */
    ftmpbuffer = file_buffer_getbybufid(txn2filebuffer, ffstate->bufid);
    rmemcpy1(&ftmpbuffer->extra, 0, &fbuffer->extra, sizeof(file_buffer_extra));

    /* 将 buffer 添加到待刷链表中 */
    file_buffer_waitflush_add(txn2filebuffer, fbuffer);
    fbuffer = ftmpbuffer;
    ftmpbuffer->flag = FILE_BUFFER_FLAG_DATA;

    /* 设置 buffer 私有信息 */
    if(NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc1(sizeof(ff_fileinfo));
        if(NULL == finfo)
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

    /* 查看是否为文件的开头，文件开头那么增加文件头和dbmetadata信息 */
    if(true == shiftfile)
    {
        fftrail_fileinit(ffstate);
    }

    /* 重置 ffdatahdr 内容 */
    ffdatahdr->totallength = 0;
    ffdatahdr->reccount = 1;
    ffdatahdr->reclength = 0;
    ffdatahdr->subtype = FF_DATA_TYPE_REC_CONTRECORD;

    /* 重置 record 起始位置 */
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 指向放数据的位置 */
    fbuffer->start += hdrlen;

    /* 递归调用 */
    return fftrail_data_data2buffer(ffdatahdr, ffstate, ref_buffer, dtype, wbytes, data);
}

/*
 * 将数据从 buffer 获取
 * 参数:
 *  ffdatahdr               recordhead 的内容
 *  ffstate                 上下文
 *  recoffset               基于 record 开始的偏移量,主要用于校验查看当前的 record 是否含有足够的数据
 *  dtype                   获取的数据类型
 *  dlen                    理论长度
 *  data                    数据存放的位置
 *  扩大recoffset和dataoffset，record拼接后长度增加
*/
bool fftrail_data_buffer2data(ff_data* ffdatahdr, 
                                        ffsmgr_state* ffstate,
                                        uint32* recoffset,
                                        uint32* dataoffset,
                                        ftrail_datatype dtype,
                                        uint64 dlen,
                                        uint8* data)
{
    uint32  freespc = 0;
    uint8*  uptr = NULL;

    /* 解析数据的内容 */
    uptr = ffstate->recptr;
    uptr += (*recoffset);

    freespc = ffdatahdr->totallength - *dataoffset;
    if(dlen <= freespc)
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
 * 组装头部信息
*/
void fftrail_data_hdrserail(ff_data* ffdatahdr, ffsmgr_state* ffstate)
{
    uint32 len = 0;
    uint8*  uptr = NULL;
    uint8*  crcuptr = NULL;

    /* 向 buffer 中组装内容 */
    crcuptr = uptr = ffstate->recptr;

    /* ffheader 内容填充 */
    /* 增加 数据名称编码 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_DBMDNO,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->dbmdno,
                                &len,
                                uptr);

    /* 增加 表编码 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TBMDNO,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffdatahdr->tbmdno,
                                &len,
                                uptr);

    /* 增加 事务号 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TRANSID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->transid,
                                &len,
                                uptr);

    /* 增加 事务内顺序 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TRANSIND,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_TINYINT,
                                1,
                                (uint8*)&ffdatahdr->transind,
                                &len,
                                uptr);

    /* 增加 总长度 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TOTALLENGTH,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->totallength,
                                &len,
                                uptr);

    /* 增加 record 长度 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_RECLENGTH,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->reclength,
                                &len,
                                uptr);

    /* 增加 record 数量 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_RECCOUNT,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->reccount,
                                &len,
                                uptr);

    /* 增加 数据涞源 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_FORMATTYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_TINYINT,
                                1,
                                (uint8*)&ffdatahdr->formattype,
                                &len,
                                uptr);

    /* 增加 类型标识 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_TYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_SMALLINT,
                                2,
                                (uint8*)&ffdatahdr->subtype,
                                &len,
                                uptr);

    /* 增加 结束位置的偏移量 */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_ORGPOS,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                8,
                                (uint8*)&ffdatahdr->orgpos,
                                &len,
                                uptr);

    /* 
     * 增加 crc 内容
     *  1、计算 crc 码
     *  2、将 crc 码写入到文件中
     */
    /* 计算 crc 码 */
    /* (1)计算头部 */
    INIT_CRC32C(ffdatahdr->crc32);
    COMP_CRC32C(ffdatahdr->crc32, crcuptr, len);

    /* (2)计算数据 */
    crcuptr += fftrail_data_headlen(ffstate->compatibility);
    COMP_CRC32C(ffdatahdr->crc32, crcuptr, ffdatahdr->reclength);
    FIN_CRC32C(ffdatahdr->crc32);

    /* 将 crc 码写入到文件中 */
    /* 增加crc */
    uptr = fftrail_token2buffer(TRAIL_TOKENDATAHDR_ID_CRC32,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffdatahdr->crc32,
                                &len,
                                uptr);
}

/*
 * 反序列化头部信息
 * 
*/
bool fftrail_data_hdrdeserail(ff_data* ffdatahdr, ffsmgr_state* ffstate)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  tokenlen = 0;                       /* token 长度 */
    uint8*  tokendata = NULL;                   /* token 数据区 */

    uint8*  uptr = NULL;

    /* 在 buffer 中组装内容 */
    uptr = ffstate->recptr;

    /* 获取数据库编码 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(TRAIL_TOKENDATAHDR_ID_DBMDNO != tokenid
        || FFTRAIL_INFOTYPE_TOKEN != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "need type %u, bu current type:%u",
                            TRAIL_TOKENDATAHDR_ID_DBMDNO,
                            FFTRAIL_INFOTYPE_TOKEN);
    }
    ffdatahdr->dbmdno = CONCAT(get,16bit)(&tokendata);

    /* 获取表编码 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->tbmdno = CONCAT(get,32bit)(&tokendata);

    /* 获取事务 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->transid = CONCAT(get,64bit)(&tokendata);

    /* 获取事务位置 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->transind = CONCAT(get,8bit)(&tokendata);

    /* 获取总长度 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->totallength = CONCAT(get,64bit)(&tokendata);

    /* 获取记录长度 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->reclength = CONCAT(get,16bit)(&tokendata);

    /* 获取记录条数 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->reccount = CONCAT(get,16bit)(&tokendata);

    /* 获取来源 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->formattype = CONCAT(get,8bit)(&tokendata);

    /* 获取操作类型 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->subtype = CONCAT(get,16bit)(&tokendata);

    /* 获取结束位置的偏移量 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->orgpos = CONCAT(get,64bit)(&tokendata);

    /* 获取 crc */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffdatahdr->crc32 = CONCAT(get,32bit)(&tokendata);

    return true;
}

/* 序列化数据信息 */
bool fftrail_data_serail(void* data, void* state)
{
    ff_data* ffdata = NULL;

    ffdata = (ff_data*)data;

    if(m_datatypmgrcnt < ffdata->type)
    {
        elog(RLOG_ERROR, "unknown data type:%d", ffdata->type);
    }

    if(NULL == m_datatypemgr[ffdata->type].serial)
    {
        elog(RLOG_ERROR, "%s unsupport", m_datatypemgr[ffdata->type].desc);
    }
    return m_datatypemgr[ffdata->type].serial(data, state);
}

/* 判断传入的data类型的record的是否为一个事务的开始 */
bool fftrail_data_deserail_check_transind_start(uint8 *uptr, int compatibility)
{
    uint16 subtype = 0;
    ffsmgr_state ffstate;
    ff_data ffdata;

    rmemset1(&ffstate, 0, 0, sizeof(ffsmgr_state));
    rmemset1(&ffdata, 0, 0, sizeof(ff_data));

    /* 跳过token部分 */
    uptr += TOKENHDRSIZE;
    ffstate.recptr = uptr;
    subtype = fftrail_data_getsubtypefromhead(compatibility, uptr);

    /* 错误检测 */
    if(m_datatypmgrcnt < subtype)
    {
        elog(RLOG_ERROR, "unknown data type:%d", subtype);
    }

    if(NULL == m_datatypemgr[subtype].deserial)
    {
        return false;
    }

    fftrail_data_hdrdeserail(&ffdata, &ffstate);

    /* 事务开始 */
    if (FF_DATA_TRANSIND_START == (FF_DATA_TRANSIND_START & ffdata.transind))
    {
        return true;
    }

    return false;
}

/* 序列化数据信息 */
bool fftrail_data_deserail(void** data, void* state)
{
    /* 调用反序列化接口，解析数据 */
    uint16 subtype = 0;
    uint8* uptr = NULL;
    ffsmgr_state* ffstate = NULL;

    /* 获取 buffer */
    ffstate = (ffsmgr_state*)state;

    uptr = ffstate->recptr;
    uptr += TOKENHDRSIZE;
    subtype = fftrail_data_getsubtypefromhead(ffstate->compatibility, uptr);

    if(m_datatypmgrcnt < subtype)
    {
        elog(RLOG_WARNING, "unknown data type:%d", subtype);
        return false;
    }

    if(NULL == m_datatypemgr[subtype].deserial)
    {
        elog(RLOG_WARNING, "%s unsupport", m_datatypemgr[subtype].desc);
        return false;
    }
    return m_datatypemgr[subtype].deserial(data, state);
}

/*
 * 最小长度
*/
int fftrail_data_tokenminsize(int compatibility)
{
    /*
     * token group 标识
     * token header
     * token tail
    */
   int minsize = 0;
   minsize = fftrail_data_headlen(compatibility);
   minsize += (TOKENHDRSIZE +  + TOKENHDRSIZE);
   return minsize;
}

