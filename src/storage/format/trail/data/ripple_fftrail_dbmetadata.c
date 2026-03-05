#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "storage/trail/data/ripple_fftrail_dbmetadata.h"

/* 数据库信息序列化 */
bool ripple_fftrail_dbmetadata_serial(void* data, void* state)
{
    /*
     * 1、首先将数据部分写入到 buffer 中
     * 2、增加 rectail 尾部
     * 3、写GROUP信息
     * 4、写头部
    */
    uint16 reclen = 0;                          /* 数据长度 */
    int hdrlen = 0;                             /* 头长度 */
    int tmplen = 0;                             /* 计算过程中使用的临时变量 */

    uint8* uptr = NULL;
    ripple_ff_dbmetadata* ffdbmd = NULL;
    ripple_ffsmgr_state* ffstate = NULL;
    ripple_file_buffer* rfbuffer = NULL;

    /* 类型转换 */
    ffdbmd = (ripple_ff_dbmetadata*)data;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取 buffer */
    rfbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /* 向 buffer 中设置数据 */
    ffstate->recptr = uptr = rfbuffer->data + rfbuffer->start;

    /* 增加偏移 */
    hdrlen = RIPPLE_TOKENHDRSIZE;
    hdrlen += ripple_fftrail_data_headlen(ffstate->compatibility);

    /* 数据偏移 */
    uptr += hdrlen;

    /* 填充数据 */
    /* 数据库编号 */
    RIPPLE_CONCAT(put,16bit)(&uptr, ffdbmd->dbmdno);
    reclen += 2;

    /* 数据库唯一标识 */
    RIPPLE_CONCAT(put,32bit)(&uptr, ffdbmd->oid);
    reclen += 4;

    /* 数据库名称 */
    tmplen = strlen(ffdbmd->dbname);
    RIPPLE_CONCAT(put,16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->dbname, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* 数据库编码 */
    tmplen = strlen(ffdbmd->charset);
    RIPPLE_CONCAT(put,16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->charset, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* 时区 */
    tmplen = strlen(ffdbmd->timezone);
    RIPPLE_CONCAT(put,16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->timezone, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    /* 币种 */
    tmplen = strlen(ffdbmd->money);
    RIPPLE_CONCAT(put,16bit)(&uptr, tmplen);
    reclen += 2;
    rmemcpy1(uptr, 0, ffdbmd->money, tmplen);
    uptr += tmplen;
    reclen += tmplen;

    ffdbmd->header.reclength = reclen;
    ffdbmd->header.totallength = reclen;

    /* 增加rectail */
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_TRAIL_TOKENDATA_RECTAIL,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)
    reclen += RIPPLE_TOKENHDRSIZE;

    /* 总长度信息，包含 group 长度 */
    reclen += hdrlen;

    /* record 总长度 */
    reclen = RIPPLE_MAXALIGN(reclen);

    rfbuffer->start += reclen;

    /* 增加GROUP信息 */
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_FFTRAIL_GROUPTYPE_DATA,
                                RIPPLE_FFTRAIL_INFOTYPE_GROUP,
                                reclen,
                                ffstate->recptr)

    /* 增加头信息 */
    ripple_fftrail_data_hdrserail(&ffdbmd->header, ffstate);

    ffstate->recptr = NULL;
    return true;
}

/* 数据库信息反序列化 */
bool ripple_fftrail_dbmetadata_deserial(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint16  blkoffset = 0;
    uint16  tmplen = 0;                         /* 数据临时长度 */
    uint32  tokenlen = 0;                       /* token 长度 */
    uint8*  tokendata = NULL;                   /* token 数据区 */

    uint8*  uptr = NULL;
    ripple_ff_dbmetadata* ffdbmd = NULL;
    ripple_ffsmgr_state* ffstate = NULL;

    /* 类型强转 */
    ffstate = (ripple_ffsmgr_state*)state;

    /* 在 buffer 中组装内容 */
    uptr = ffstate->recptr;

    /* 申请空间 */
    ffdbmd = (ripple_ff_dbmetadata*)rmalloc0(sizeof(ripple_ff_dbmetadata));
    if(NULL == ffdbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd, 0, '\0', sizeof(ripple_ff_dbmetadata));
    *data = ffdbmd;

    /* 获取头部标识 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_DATA != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file format error");
    }

    /* 解析头部数据 */
    blkoffset = RIPPLE_TOKENHDRSIZE;

    /* 解析头部数据 */
    uptr = ffstate->recptr;
    ffstate->recptr += blkoffset;
    ripple_fftrail_data_hdrdeserail(&ffdbmd->header, ffstate);
    ffstate->recptr = uptr;
    blkoffset += ripple_fftrail_data_headlen(ffstate->compatibility);

    /* 解析真实数据 */
    uptr += blkoffset;

    /* 获取数据库编号 */
    ffdbmd->dbmdno = RIPPLE_CONCAT(get,16bit)(&uptr);

    /* 数据库唯一标识 */
    ffdbmd->oid = RIPPLE_CONCAT(get,32bit)(&uptr);

    /* 获取数据库名称 */
    tmplen = RIPPLE_CONCAT(get,16bit)(&uptr);
    ffdbmd->dbname = (char*)rmalloc0(tmplen + 1);
    if(NULL == ffdbmd->dbname)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->dbname, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->dbname, 0, uptr, tmplen);
    ffdbmd->dbname[tmplen] = '\0';
    uptr += tmplen;

    /* 获取数据库编码 */
    tmplen = RIPPLE_CONCAT(get,16bit)(&uptr);
    ffdbmd->charset = (char*)rmalloc0(tmplen + 1);
    if(NULL == ffdbmd->charset)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->charset, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->charset, 0, uptr, tmplen);
    ffdbmd->charset[tmplen] = '\0';
    uptr += tmplen;

    /* 获取数据库时区 */
    tmplen = RIPPLE_CONCAT(get,16bit)(&uptr);
    ffdbmd->timezone = (char*)rmalloc0(tmplen + 1);
    if(NULL == ffdbmd->timezone)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->timezone, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->timezone, 0, uptr, tmplen);
    ffdbmd->timezone[tmplen] = '\0';
    uptr += tmplen;

    /* 获取数据库币种 */
    tmplen = RIPPLE_CONCAT(get,16bit)(&uptr);
    ffdbmd->money = (char*)rmalloc0(tmplen + 1);
    if(NULL == ffdbmd->money)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd->money, 0, '\0', (tmplen + 1));
    rmemcpy0(ffdbmd->money, 0, uptr, tmplen);
    ffdbmd->money[tmplen] = '\0';
    uptr += tmplen;

    /* 获取结尾 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)

    /* 日志级别为 debug */
    if(RLOG_DEBUG == g_loglevel)
    {
        /* 输出调试日志 */
        elog(RLOG_DEBUG, "----------Trail MetaDB Begin----------------");
        elog(RLOG_DEBUG, "dbmdno:           %u", ffdbmd->header.dbmdno);
        elog(RLOG_DEBUG, "tbmdno:           %u", ffdbmd->header.tbmdno);
        elog(RLOG_DEBUG, "transid:          %lu", ffdbmd->header.transid);
        elog(RLOG_DEBUG, "transind:         %u", ffdbmd->header.transind);
        elog(RLOG_DEBUG, "totallength:      %lu", ffdbmd->header.totallength);
        elog(RLOG_DEBUG, "reclength:        %u", ffdbmd->header.reclength);
        elog(RLOG_DEBUG, "reccount:         %u", ffdbmd->header.reccount);
        elog(RLOG_DEBUG, "formattype:       %u", ffdbmd->header.formattype);
        elog(RLOG_DEBUG, "subtype:          %u", ffdbmd->header.subtype);
        elog(RLOG_DEBUG, "dbmdno:           %u", ffdbmd->dbmdno);
        elog(RLOG_DEBUG, "oid:              %u", ffdbmd->oid);
        elog(RLOG_DEBUG, "dbname:           %s", ffdbmd->dbname);
        elog(RLOG_DEBUG, "charset:          %s", ffdbmd->charset);
        elog(RLOG_DEBUG, "timezone:         %s", ffdbmd->timezone);
        elog(RLOG_DEBUG, "money:            %s", ffdbmd->money);
        elog(RLOG_DEBUG, "----------Trail MetaDB   End----------------");
    }

    return true;
}
