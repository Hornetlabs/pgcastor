#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netclient.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"
#include "fastcompare/ripple_fastcompare_tableslicetask.h"
#include "fastcompare/ripple_fastcompare_tableslice.h"


ripple_fastcompare_beginslice *ripple_fastcompare_beginslice_init(void)
{
    ripple_fastcompare_beginslice *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_beginslice));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_beginslice));

    result->base.type = RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINSLICE;

    return result;
}

void ripple_fastcompare_beginslice_clean(ripple_fastcompare_beginslice *beginslice)
{
    if (beginslice)
    {
        if (beginslice->schema)
        {
            rfree(beginslice->schema);
        }
        if (beginslice->table)
        {
            rfree(beginslice->table);
        }
        if (beginslice->condition)
        {
            rfree(beginslice->condition);
        }
        rfree(beginslice);
    }
}

uint8 *ripple_fastcompare_beginslice_serial(ripple_fastcompare_beginslice *beginslice, uint32 *size)
{
    uint32 len = 0;
    uint32 mlen = 0;
    ListCell *cell = NULL;
    uint8 *result = NULL;
    uint8 *curptr = NULL;
    int temp_len = 0;
    uint32 crc = 0;

    /* 计算长度 */
    len = 4 + 4 + 4                         /* type, length, crc32 */
        + 1 + 4                             /* flag, slicenum */
        + 2 + strlen(beginslice->schema)    /* schemalen, schema */
        + 2 + strlen(beginslice->table)     /* tablelen, table */
        + 2 + strlen(beginslice->condition) /* condition len, condition */
        + 2;                                /* columncnt */

    foreach(cell, beginslice->columns)
    {
        ripple_fastcompare_columndefine *coldef = (ripple_fastcompare_columndefine *)lfirst(cell);
        len += 2 + 2 + strlen(coldef->colname); /* columnid, column name len, column name */
    }

    mlen = RIPPLE_MAXALIGN(len);

    result = rmalloc0(sizeof(uint8) * mlen);
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(uint8) * mlen);

    curptr = result;

    /* type */
    RIPPLE_CONCAT(put, 32bit)(&curptr, beginslice->base.type);
    /* len */
    RIPPLE_CONCAT(put, 32bit)(&curptr, len);

    /* 先跳过crc */
    curptr += 4;

    /* flag */
    RIPPLE_CONCAT(put, 8bit)(&curptr, beginslice->flag);
    /* num */
    RIPPLE_CONCAT(put, 32bit)(&curptr, beginslice->num);

    temp_len = strlen(beginslice->schema);
    /* schema len */
    RIPPLE_CONCAT(put, 16bit)(&curptr, temp_len);
    /* schema */
    memcpy(curptr, beginslice->schema, temp_len);
    curptr += temp_len;

    temp_len = strlen(beginslice->table);
    /* table len */
    RIPPLE_CONCAT(put, 16bit)(&curptr, temp_len);
    /* table */
    memcpy(curptr, beginslice->table, temp_len);
    curptr += temp_len;

    temp_len = strlen(beginslice->condition);
    /* condition len */
    RIPPLE_CONCAT(put, 16bit)(&curptr, temp_len);
    /* condition */
    memcpy(curptr, beginslice->condition, temp_len);
    curptr += temp_len;

    /* column cnt */
    RIPPLE_CONCAT(put, 16bit)(&curptr, beginslice->columns->length);

    foreach(cell, beginslice->columns)
    {
        ripple_fastcompare_columndefine *coldef = (ripple_fastcompare_columndefine *)lfirst(cell);
        /* column id */
        RIPPLE_CONCAT(put, 16bit)(&curptr, coldef->colid);

        temp_len = strlen(coldef->colname);
        /* column name len */
        RIPPLE_CONCAT(put, 16bit)(&curptr, temp_len);
        memcpy(curptr, coldef->colname, temp_len);
        curptr += temp_len;
    }

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, result, 8);
    COMP_CRC32C(crc, result + 12, len - 12);
    FIN_CRC32C(crc);

    /* crc最后放置 */
    curptr = result + 8;
    RIPPLE_CONCAT(put, 32bit)(&curptr, crc);

    *size = len;
    return result;
}

bool ripple_fastcompare_beginslice_send(void* netclient_in, ripple_fastcompare_beginslice *beginslice)
{
    ripple_netclient* netclient = (ripple_netclient*) netclient_in;
    ripple_netpacket *netpacket = NULL;
    uint8 *data = NULL;
    uint32 size = 0;

    /* 序列化数据 */
    data = ripple_fastcompare_beginslice_serial(beginslice, &size);

    netpacket = ripple_netpacket_init();
    netpacket->used = size;
    netpacket->max = RIPPLE_MAXALIGN(size);
    netpacket->data = data;
    ripple_netclient_addwpacket(netclient, (void*)netpacket);

    /* 消息处理 */
    if(false == ripple_netclient_desc(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

        elog(RLOG_ERROR, "can't send begin slice");
    }
    return true;
}

/* 按照beginslice协议获取数据 */
ripple_fastcompare_beginslice* ripple_fastcompare_beginslice_fetchdata(void* privdata, uint8* buffer)
{
    uint16 mlen = 0;
    uint32 colcnt = 0;
    uint32 colindex = 0;
    uint32 checkcrc = 0;
    uint32 msgcrc = 0;
    uint32 msglen = 0;
    uint32 msgtype = 0;
    uint8* uptr = NULL;
    ripple_fastcompare_beginslice* beginslice = NULL;
    ripple_fastcompare_columndefine* column = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    uptr = buffer;
    /* 计算crc */
    INIT_CRC32C(checkcrc);
    COMP_CRC32C(checkcrc, uptr, 8);

    /* 读取协议头部信息 */
    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);
    msglen = RIPPLE_CONCAT(get, 32bit)(&uptr);
    msgcrc = RIPPLE_CONCAT(get, 32bit)(&uptr);

    COMP_CRC32C(checkcrc, uptr, (msglen - 12));
    FIN_CRC32C(checkcrc);

    if (checkcrc != msgcrc)
    {
        elog(RLOG_ERROR, "Beginslice CRC check failed");
    }

    elog(RLOG_DEBUG,"Get beginslice :%u msglen: %u ", msgtype, msglen);

    beginslice = ripple_fastcompare_beginslice_init();
    beginslice->base.type = msgtype;

    manager->correctslice->sliceflag = RIPPLE_CONCAT(get, 8bit)(&uptr);
    manager->correctslice->no = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /* 读取tablename */
    mlen = RIPPLE_CONCAT(get, 16bit)(&uptr);
    beginslice->schema = (char*)rmalloc0(mlen + 1);
    if(NULL == beginslice->schema)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(beginslice->schema, 0, '\0', mlen + 1);
    rmemcpy0(beginslice->schema, 0, uptr, mlen);
    uptr += mlen;

    /* 读取schemaname */
    mlen = RIPPLE_CONCAT(get, 16bit)(&uptr);
    beginslice->table = (char*)rmalloc0(mlen + 1);
    if(NULL == beginslice->table)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(beginslice->table, 0, '\0', mlen + 1);
    rmemcpy0(beginslice->table, 0, uptr, mlen);
    uptr += mlen;

    /* 读取condition */
    mlen = RIPPLE_CONCAT(get, 16bit)(&uptr);
    beginslice->condition = (char*)rmalloc0(mlen + 1);
    if(NULL == beginslice->condition)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(beginslice->condition, 0, '\0', mlen + 1);
    rmemcpy0(beginslice->condition, 0, uptr, mlen);
    uptr += mlen;

    /* 读取主键列信息 */
    colcnt = RIPPLE_CONCAT(get, 16bit)(&uptr);

    for (colindex = 0; colindex < colcnt; colindex++)
    {
        column = ripple_fastcompare_columndefine_init();

        column->colid = RIPPLE_CONCAT(get, 16bit)(&uptr);

        mlen = RIPPLE_CONCAT(get, 16bit)(&uptr);
        column->colname = (char*)rmalloc0(mlen + 1);
        if(NULL == column->colname)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(column->colname, 0, '\0', mlen + 1);
        rmemcpy0(column->colname, 0, uptr, mlen);
        uptr += mlen;
        beginslice->columns = lappend(beginslice->columns, column);
    }
    return beginslice;
}

