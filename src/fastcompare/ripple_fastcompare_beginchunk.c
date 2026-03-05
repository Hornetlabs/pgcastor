#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/algorithm/crc/crc_check.h"
#include "queue/ripple_queue.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netclient.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"

ripple_fastcompare_beginchunk *ripple_fastcompare_beginchunk_init(void)
{
    ripple_fastcompare_beginchunk *result = NULL;
    result = rmalloc0(sizeof(ripple_fastcompare_beginchunk));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_beginchunk));
    result->base.type = RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINCHNUK;
    return result;
}

void ripple_fastcompare_beginchunk_clean(ripple_fastcompare_beginchunk *chunk)
{
    /* min和max链表在simpledatachunk发送后删除 */
    if (chunk)
    {
        rfree(chunk);
    }
}

uint8 *ripple_fastcompare_beginchunk_serial(ripple_fastcompare_beginchunk *beginchunk,
                                          uint32 *size)
{
    uint32 len = 0;
    uint32 mlen = 0;
    ListCell *cell = NULL;
    uint8 *result = NULL;
    uint8 *curptr = NULL;
    uint32 crc = 0;

    /* 计算长度 */
    len = 4 + 4 + 4     /* type, length, crc32 */
        + 1 + 2;        /* flag, pkeycnt */

    foreach(cell, beginchunk->minprivalue)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);
        len += 4 + 4 + 2 + 4 + col->len;
    }

    foreach(cell, beginchunk->maxprivalue)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);
        len += 4 + 4 + 2 + 4 + col->len;
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
    RIPPLE_CONCAT(put, 32bit)(&curptr, beginchunk->base.type);
    /* len */
    RIPPLE_CONCAT(put, 32bit)(&curptr, len);

    /* 先跳过crc */
    curptr += 4;

    /* flag */
    RIPPLE_CONCAT(put, 8bit)(&curptr, beginchunk->flag);

    /* pkeycnt */
    RIPPLE_CONCAT(put, 16bit)(&curptr, beginchunk->maxprivalue->length);

    foreach(cell, beginchunk->minprivalue)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->type);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->colid);
        RIPPLE_CONCAT(put, 16bit)(&curptr, col->flag);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->len);
        memcpy(curptr, col->value, col->len);
        curptr += col->len;
    }

    foreach(cell, beginchunk->maxprivalue)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->type);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->colid);
        RIPPLE_CONCAT(put, 16bit)(&curptr, col->flag);
        RIPPLE_CONCAT(put, 32bit)(&curptr, col->len);
        memcpy(curptr, col->value, col->len);
        curptr += col->len;
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

bool ripple_fastcompare_beginchunk_send(void* netclient_in, ripple_fastcompare_beginchunk *beginchunk)
{
    ripple_netclient* netclient = (ripple_netclient*) netclient_in;
    ripple_netpacket *netpacket = NULL;
    uint8 *data = NULL;
    uint32 size = 0;

    /* 序列化数据 */
    data = ripple_fastcompare_beginchunk_serial(beginchunk, &size);

    netpacket = ripple_netpacket_init();
    netpacket->used = size;
    netpacket->max = RIPPLE_MAXALIGN(size);
    netpacket->data = data;
    ripple_netclient_addwpacket(netclient, (void*)netpacket);

    /* 消息处理 */
    if(false == ripple_netclient_desc(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

        elog(RLOG_ERROR, "can't send begin chunk");
    }
    return true;
}

/* 根据消息生成beginchunk */
ripple_fastcompare_beginchunk* ripple_fastcompare_beginchunk_fetchdata(void* privdata, uint8* buffer)
{
    uint32 colindex = 0;
    uint32 msgcrc = 0;
    uint32 checkcrc = 0;
    uint32 msglen = 0;
    uint32 msgtype = 0;
    uint8* uptr = NULL;
    ripple_fastcompare_columnvalue* column = NULL;
    ripple_fastcompare_beginchunk* beginchunk = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    uptr = buffer;
    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

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
        elog(RLOG_ERROR, "Beginchunk CRC check failed");
    }

    elog(RLOG_DEBUG,"Get beginchunk :%u msglen: %u ", msgtype, msglen);

    beginchunk = ripple_fastcompare_beginchunk_init();
    beginchunk->base.type = msgtype;
    manager->correctslice->chunkflag = RIPPLE_CONCAT(get, 8bit)(&uptr);

    manager->correctslice->prikeycnt = RIPPLE_CONCAT(get, 16bit)(&uptr);

    /* 获取最小主键 */
    for (colindex = 0; colindex < manager->correctslice->prikeycnt; colindex++)
    {
        column = ripple_fastcompare_columnvalue_init();

        column->type = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->colid = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->flag = RIPPLE_CONCAT(get, 16bit)(&uptr);
        column->len = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->value = (char*)rmalloc0(column->len + 1);
        if(NULL == column->value)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(column->value, 0, '\0', column->len + 1);
        rmemcpy0(column->value, 0, uptr, column->len);
        uptr += column->len;
        beginchunk->minprivalue = lappend(beginchunk->minprivalue, column);
    }

    /* 获取最大主键 */
    for (colindex = 0; colindex < manager->correctslice->prikeycnt; colindex++)
    {
        column = ripple_fastcompare_columnvalue_init();

        column->type = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->colid = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->flag = RIPPLE_CONCAT(get, 16bit)(&uptr);
        column->len = RIPPLE_CONCAT(get, 32bit)(&uptr);
        column->value = (char*)rmalloc0(column->len + 1);
        if(NULL == column->value)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(column->value, 0, '\0', column->len + 1);
        rmemcpy0(column->value, 0, uptr, column->len);
        uptr += column->len;
        beginchunk->maxprivalue = lappend(beginchunk->maxprivalue, column);
    }

    return beginchunk;
}
