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
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_row.h"
#include "fastcompare/ripple_fastcompare_datachunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_datacmpresultitem.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"

ripple_fastcompare_datachunk *ripple_fastcompare_datachunk_init(void)
{
    ripple_fastcompare_datachunk *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_datachunk));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_datachunk));

    result->base.type = RIPPLE_NETMSG_TYPE_FASTCOMPARE_S2DCORRECTDATACHUNK;

    return result;
}

void ripple_fastcompare_datachunk_clean(ripple_fastcompare_datachunk *datachunk)
{
    if (datachunk)
    {
        if (datachunk->rows)
        {
            ripple_fastcompare_row_lsit_clean(datachunk->rows);
        }
        rfree(datachunk);
    }
}

uint8 *ripple_fastcompare_datachunk_serial(ripple_fastcompare_datachunk *chunk,
                                                 uint32 *size)
{
    uint32 len = 0;
    uint32 mlen = 0;
    ListCell *rowcell = NULL;
    uint8 *result = NULL;
    uint8 *curptr = NULL;
    uint32 crc = 0;
    int index_col = 0;

    /* 计算长度 */
    len = 4 + 4 + 4     /* type, length, crc32 */
        + 4;            /* rowcnt */
    foreach(rowcell, chunk->rows)
    {

        ripple_fastcompare_row *row = (ripple_fastcompare_row *)lfirst(rowcell);

        len += 1 + 4;

        for (index_col = 0; index_col < row->cnt; index_col++)
        {
            ripple_fastcompare_columnvalue *colvalue = &row->column[index_col];
            len += 4 + 4 + 2 + 4 + colvalue->len;
        }
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
    RIPPLE_CONCAT(put, 32bit)(&curptr, chunk->base.type);
    /* len */
    RIPPLE_CONCAT(put, 32bit)(&curptr, len);

    /* 先跳过crc */
    curptr += 4;

    /* rowcnt */
    RIPPLE_CONCAT(put, 32bit)(&curptr, chunk->rows->length);

    //elog(RLOG_DEBUG, "row cnt: %u", chunk->rows->length);

    foreach(rowcell, chunk->rows)
    {

        ripple_fastcompare_row *row = (ripple_fastcompare_row *)lfirst(rowcell);

        RIPPLE_CONCAT(put, 8bit)(&curptr, row->op);
        RIPPLE_CONCAT(put, 32bit)(&curptr, row->cnt);

        // elog(RLOG_DEBUG, "op: %hhu, value cnt: %u", row->op, row->cnt);

        for (index_col = 0; index_col < row->cnt; index_col++)
        {
            ripple_fastcompare_columnvalue *colvalue = &row->column[index_col];
            RIPPLE_CONCAT(put, 32bit)(&curptr, colvalue->type);
            RIPPLE_CONCAT(put, 32bit)(&curptr, colvalue->colid);
            RIPPLE_CONCAT(put, 16bit)(&curptr, colvalue->flag);
            RIPPLE_CONCAT(put, 32bit)(&curptr, colvalue->len);
            memcpy(curptr, colvalue->value, colvalue->len);

            // elog(RLOG_DEBUG, "value[%d] %s", index_col + 1, colvalue->value);
            curptr += colvalue->len;
        }
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

ripple_fastcompare_datachunk *ripple_fastcompare_datachunk_deserial(uint8 *chunk)
{
    ripple_fastcompare_datachunk *result = NULL;
    uint8 *curptr = NULL;
    uint32 total_len = 0;
    uint32 crc = 0;
    uint32 crc_result = 0;
    int index_row = 0;
    int index_col = 0;
    uint32 rowcnt = 0;

    result = ripple_fastcompare_datachunk_init();
    curptr = chunk;

    /* type */
    result->base.type = RIPPLE_CONCAT(get, 32bit)(&curptr);

    /* len */
    total_len = RIPPLE_CONCAT(get, 32bit)(&curptr);

    /* crc */
    crc = RIPPLE_CONCAT(get, 32bit)(&curptr);

    INIT_CRC32C(crc_result);
    COMP_CRC32C(crc_result, chunk, 8);
    COMP_CRC32C(crc_result, chunk + 12, total_len - 12);
    FIN_CRC32C(crc_result);

    if (crc_result != crc)
    {
        elog(RLOG_ERROR, "crc check failed!");
    }

    /* rowcnt */
    rowcnt = RIPPLE_CONCAT(get, 32bit)(&curptr);
    //elog(RLOG_DEBUG, "datachunk deserial, total_len %u, rowcnt: %u", total_len, rowcnt);

    for (index_row = 0; index_row < rowcnt; index_row++)
    {
        ripple_fastcompare_row *row = ripple_fastcompare_row_init();

        row->op = RIPPLE_CONCAT(get, 8bit)(&curptr);
        row->cnt = RIPPLE_CONCAT(get, 32bit)(&curptr);

        //elog(RLOG_DEBUG, "datachunk deserial, row->op: %u, cnt: %u", row->op, row->cnt);

        ripple_fastcompare_row_column_init(row, row->cnt);
        for (index_col = 0; index_col < row->cnt; index_col++)
        {
            ripple_fastcompare_columnvalue *col = &row->column[index_col];
            col->type = RIPPLE_CONCAT(get, 32bit)(&curptr);
            col->colid = RIPPLE_CONCAT(get, 32bit)(&curptr);
            col->flag = RIPPLE_CONCAT(get, 16bit)(&curptr);
            col->len = RIPPLE_CONCAT(get, 32bit)(&curptr);

            col->value = rmalloc0(col->len + 1);
            if (!col->value)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(col->value, 0, 0, col->len + 1);
            memcpy(col->value, curptr, col->len);
            curptr += col->len;
        }
        result->rows = lappend(result->rows, row);
    }

    return result;
}

bool ripple_fastcompare_datachunk_send(void* netclient_in, ripple_fastcompare_datachunk *datachunk)
{
    ripple_netclient* netclient = (ripple_netclient*) netclient_in;
    ripple_netpacket *netpacket = NULL;
    uint8 *data = NULL;
    uint32 size = 0;

    /* 序列化数据 */
    data = ripple_fastcompare_datachunk_serial(datachunk, &size);

    netpacket = ripple_netpacket_init();
    netpacket->used = size;
    netpacket->max = RIPPLE_MAXALIGN(size);
    netpacket->data = data;
    ripple_netclient_addwpacket(netclient, (void*)netpacket);

    /* 消息处理 */
    if(false == ripple_netclient_desc(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

        elog(RLOG_ERROR, "can't send data chunk");
    }
    return true;
}


/* 按照S2DcorrectData协议获取数据 */
ripple_fastcompare_datachunk* ripple_fastcompare_datachunk_s2dcorrectdata_fetchdata(void* privdata, uint8* buffer)
{
    uint32 colindex = 0;
    uint32 rowindex = 0;
    uint32 rowcnt = 0;
    uint32 msglen = 0;
    uint32 msgcrc = 0;
    uint32 msgtype = 0;
    uint32 checkcrc = 0;
    uint8* uptr = NULL;
    List* rows = NULL;
    ripple_fastcompare_row* row = NULL;
    ripple_fastcompare_columnvalue* column = NULL;
    ripple_fastcompare_datachunk* datachunk = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    uptr = buffer;
    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;
    RIPPLE_UNUSED(manager);

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
        elog(RLOG_ERROR, "Datachunk CRC check failed");
    }

    elog(RLOG_DEBUG,"Get s2dcorrectdata :%u msglen:%u ", msgtype, msglen);

    datachunk = ripple_fastcompare_datachunk_init();
    datachunk->base.type = msgtype;

    /* 读取行数 */
    rowcnt = RIPPLE_CONCAT(get, 32bit)(&uptr);
    //elog(RLOG_DEBUG, "row cnt: %u", rowcnt);

    /* 组装行数据 */
    for (rowindex = 0; rowindex < rowcnt; rowindex++)
    {
        row = ripple_fastcompare_row_init();

        /* 读取列数 */
        row->op = RIPPLE_CONCAT(get, 8bit)(&uptr);
        row->cnt = RIPPLE_CONCAT(get, 32bit)(&uptr);

        //elog(RLOG_DEBUG, "op: %hhu, cnt: %u", row->op, row->cnt);

        /* 组装列数据 */
        ripple_fastcompare_row_column_init(row, row->cnt);

        for (colindex = 0; colindex < row->cnt; colindex++)
        {
            column = &row->column[colindex];
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
            //elog(RLOG_WARNING, "column->len: %u, column->value:%s", column->len, column->value);
            uptr += column->len;
        }
        rows = lappend(rows, row);
    }
    datachunk->rows = rows;

    return datachunk;
}

/* 构建d2scorrectdata消息 */
uint8* ripple_fastcompare_datachunk_d2scorrectdata_build(void* privdata, uint32* totallen)
{
    uint32 rowcnt = 0;
    uint32 crc = 0;
    uint8* uptr = NULL;
    uint8* buffer = NULL;
    ListCell* resultcell = NULL;
    ListCell* colcell = NULL;
    ripple_fastcompare_columnvalue* column = NULL;
    ripple_fastcompare_datacmpresultitem* resultitem = NULL;
    ripple_fastcompare_tableslicecorrectmanager* manager = NULL;

    manager = (ripple_fastcompare_tableslicecorrectmanager*)privdata;

    *totallen = 0;
    *totallen += 16;

    /* 计算总长度 */
    foreach(resultcell, manager->datacompare->result->checkresult)
    {
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(resultcell);
        *totallen += 5;
        foreach(colcell, resultitem->privalues)
        {
            column = (ripple_fastcompare_columnvalue *)lfirst(colcell);
            *totallen += 14 + column->len;
        }
        rowcnt++;
    }

    /* 写入要删除的内容 */
    foreach(resultcell, manager->datacompare->result->corrresult)
    {
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(resultcell);

        *totallen += 5;
        foreach(colcell, resultitem->privalues)
        {
            column = (ripple_fastcompare_columnvalue *)lfirst(colcell);
            *totallen += 14 + column->len;
        }
        rowcnt++;
    }

    buffer = rmalloc0(*totallen);
    if (!buffer)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(buffer , 0, 0, *totallen);

    uptr = buffer;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_FASTCOMPARE_D2SCORRECTDATACHUNK);
    RIPPLE_CONCAT(put, 32bit)(&uptr, *totallen);
    uptr += 4;
    RIPPLE_CONCAT(put, 32bit)(&uptr, rowcnt);

    /* 遍历链表, 写入（I/U）内容 */
    foreach(resultcell, manager->datacompare->result->checkresult)
    {
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(resultcell);

        RIPPLE_CONCAT(put, 8bit)(&uptr, resultitem->op);
        RIPPLE_CONCAT(put, 32bit)(&uptr, resultitem->privalues->length);

        foreach(colcell, resultitem->privalues)
        {
            column = (ripple_fastcompare_columnvalue *)lfirst(colcell);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->type);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->colid);
            RIPPLE_CONCAT(put, 16bit)(&uptr, column->flag);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->len);
            rmemcpy1(uptr, 0, column->value, column->len);
            uptr += column->len;
        }
    }

    /* 写入要删除的内容 */
    foreach(resultcell, manager->datacompare->result->corrresult)
    {
        resultitem = (ripple_fastcompare_datacmpresultitem *)lfirst(resultcell);

        RIPPLE_CONCAT(put, 8bit)(&uptr, resultitem->op);
        RIPPLE_CONCAT(put, 32bit)(&uptr, resultitem->privalues->length);

        foreach(colcell, resultitem->privalues)
        {
            column = (ripple_fastcompare_columnvalue *)lfirst(colcell);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->type);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->colid);
            RIPPLE_CONCAT(put, 16bit)(&uptr, column->flag);
            RIPPLE_CONCAT(put, 32bit)(&uptr, column->len);
            rmemcpy1(uptr, 0, column->value, column->len);
            uptr += column->len;
        }
    }

    uptr = buffer;
    /* 计算crc */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, uptr, 8);
    uptr += 12;
    COMP_CRC32C(crc, uptr, (*totallen - 12));
    FIN_CRC32C(crc);

    /* 写入crc */
    uptr = buffer;
    uptr += 8;
    RIPPLE_CONCAT(put, 32bit)(&uptr, crc);

    return buffer;

}

