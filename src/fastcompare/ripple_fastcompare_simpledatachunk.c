#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/algorithm/crc/crc_check.h"
#include "queue/ripple_queue.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_simpledatachunk.h"
#include "fastcompare/ripple_fastcompare_simplerow.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_datacmpresult.h"
#include "fastcompare/ripple_fastcompare_datacompare.h"
#include "fastcompare/ripple_fastcompare_beginchunk.h"
#include "fastcompare/ripple_fastcompare_beginslice.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"
#include "fastcompare/ripple_fastcompare_tablecorrectslice.h"
#include "fastcompare/ripple_fastcompare_tableslicecorrectmanager.h"

ripple_fastcompare_simpledatachunk *ripple_fastcompare_simpledatachunk_init(void)
{
    ripple_fastcompare_simpledatachunk *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_simpledatachunk));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_simpledatachunk));

    result->base.type = RIPPLE_NETMSG_TYPE_FASTCOMPARE_SIMPLEDATACHUNK;
    INIT_CRC32C(result->crc);

    return result;
}

void ripple_fastcompare_simpledatachunk_clean(ripple_fastcompare_simpledatachunk *chunk)
{
    if (chunk)
    {
        if (chunk->data)
        {
            ripple_fastcompare_simplerow_list_clean(chunk->data);
        }
        /* min和max是引用, 不用释放 */

        rfree(chunk);
    }
}

static void ripple_fastcompare_simpledatachunk_crcComp(ripple_fastcompare_simpledatachunk *chunk,
                                                void *data,
                                                uint32 len)
{
    COMP_CRC32C(chunk->crc, data, len);
}

static void ripple_fastcompare_simpledatachunk_crcFin(ripple_fastcompare_simpledatachunk *chunk)
{
    FIN_CRC32C(chunk->crc);
}

void ripple_fastcompare_simpledatachunk_appendFin(ripple_fastcompare_simpledatachunk *chunk)
{
    return ripple_fastcompare_simpledatachunk_crcFin(chunk);
}

bool ripple_fastcompare_simpledatachunk_append(ripple_fastcompare_simpledatachunk *chunk,
                                               PGresult *res,
                                               List *pkey_define_list,
                                               int tuple_num)
{
    int nfield_cnt = PQnfields(res);
    int nfield_index = 0;
    ripple_fastcompare_simplerow *simple_row = NULL;
    ListCell *pkey_cell = NULL;
    int md5_col_num = pkey_define_list->length + 1 - 1;
    char *md5 = NULL;

    /* md5列应该是最后一列 */
    if (md5_col_num != nfield_cnt - 1)
    {
        elog(RLOG_ERROR, "column check false!");
    }

    /* 初始化SimpleRow */
    simple_row = ripple_fastcompare_simplerow_init();

    /* pkey_cell取head */
    pkey_cell = list_head(pkey_define_list);

    /* 遍历获取pkey值 */
    for (nfield_index = 0; nfield_index < md5_col_num; nfield_index++)
    {
        ripple_fastcompare_columndefine *pkey_define = NULL;
        ripple_fastcompare_columnvalue *colvalue = NULL;

        if (!pkey_cell)
        {
            elog(RLOG_ERROR, "pkey column cnt error");
        }

        pkey_define = (ripple_fastcompare_columndefine *)lfirst(pkey_cell);
        colvalue = ripple_fastcompare_columnvalue_init();
        colvalue->colid = pkey_define->colid;
        colvalue->value = rstrdup(PQgetvalue(res, tuple_num, nfield_index));
        colvalue->len = strlen(colvalue->value);

        /* 附加到simplerow中 */
        simple_row->privalues = lappend(simple_row->privalues, colvalue);

        /* 处理simpledatachunk的总crc */
        ripple_fastcompare_simpledatachunk_crcComp(chunk, colvalue->value, colvalue->len);
        chunk->size += 4 + 4 + 2 + 4;
        chunk->size += colvalue->len;
        pkey_cell = lnext(pkey_cell);
    }

    /* 获取列md5值 */
    md5 = PQgetvalue(res, tuple_num, md5_col_num);

    /* md5转换为crc */
    INIT_CRC32C(simple_row->crc);
    COMP_CRC32C(simple_row->crc, md5, strlen(md5));
    FIN_CRC32C(simple_row->crc);

    /* 处理simpledatachunk的总crc */
    ripple_fastcompare_simpledatachunk_crcComp(chunk, &simple_row->crc, sizeof(uint32));
    chunk->size += sizeof(uint32);

    if (!chunk->minprivalues)
    {
        chunk->minprivalues = simple_row->privalues;
    }
    chunk->maxprivalues = simple_row->privalues;
    chunk->data = lappend(chunk->data, simple_row);
    chunk->datacnt++;

    return true;
}

uint8 *ripple_fastcompare_simpledatachunk_serial(ripple_fastcompare_simpledatachunk *chunk,
                                                 uint32 *size)
{
    uint32 len = 0;
    uint32 mlen = 0;
    ListCell *rowcell = NULL;
    ListCell *keycell = NULL;
    uint8 *result = NULL;
    uint8 *curptr = NULL;
    uint32 crc = 0;

    /* 计算长度 */
    len = 4 + 4 + 4     /* type, length, crc32 */
        + 4 + 4;        /* crc, rowcnt */
    foreach(rowcell, chunk->data)
    {
        ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(rowcell);

        len += 4;

        foreach(keycell, row->privalues)
        {
            ripple_fastcompare_columnvalue *key = (ripple_fastcompare_columnvalue *)lfirst(keycell);
            len += 4 + 4 + 2 + 4 + key->len;
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

    /* 数据crc */
    RIPPLE_CONCAT(put, 32bit)(&curptr, chunk->crc);

    /* rowcnt */
    RIPPLE_CONCAT(put, 32bit)(&curptr, chunk->data->length);

    foreach(rowcell, chunk->data)
    {
        ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(rowcell);

        /* rowcrc */
        RIPPLE_CONCAT(put, 32bit)(&curptr, row->crc);

        foreach(keycell, row->privalues)
        {
            ripple_fastcompare_columnvalue *key = (ripple_fastcompare_columnvalue *)lfirst(keycell);
            RIPPLE_CONCAT(put, 32bit)(&curptr, key->type);
            RIPPLE_CONCAT(put, 32bit)(&curptr, key->colid);
            RIPPLE_CONCAT(put, 16bit)(&curptr, key->flag);
            RIPPLE_CONCAT(put, 32bit)(&curptr, key->len);
            memcpy(curptr, key->value, key->len);
            curptr += key->len;
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

bool ripple_fastcompare_simpledatachunk_send(void* netclient_in, ripple_fastcompare_simpledatachunk *simpledatachunk)
{
    ripple_netclient* netclient = (ripple_netclient*) netclient_in;
    ripple_netpacket *netpacket = NULL;
    uint8 *data = NULL;
    uint32 size = 0;

    /* 序列化数据 */
    data = ripple_fastcompare_simpledatachunk_serial(simpledatachunk, &size);

    netpacket = ripple_netpacket_init();
    netpacket->used = size;
    netpacket->max = RIPPLE_MAXALIGN(size);
    netpacket->data = data;
    ripple_netclient_addwpacket(netclient, (void*)netpacket);

    /* 消息处理 */
    if(false == ripple_netclient_desc(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

        elog(RLOG_ERROR, "can't send simpledata chunk");
    }
    return true;
}

/* 按照SimpleDataChunk协议获取数据 */
ripple_fastcompare_simpledatachunk* ripple_fastcompare_simpledatachunk_fetchdata(void* privdata, uint8* buffer)
{
    uint32 rowindex = 0;
    uint32 colindex = 0;
    uint32 msglen = 0;
    uint32 msgcrc = 0;
    uint32 checkcrc = 0;
    uint32 msgtype = 0;
    uint8* uptr = NULL;
    ripple_fastcompare_simplerow* row = NULL;
    ripple_fastcompare_simpledatachunk* simpledatachunk = NULL;
    ripple_fastcompare_columnvalue* column = NULL;
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
        elog(RLOG_ERROR, "Simpledatachunk CRC check failed");
    }

    elog(RLOG_DEBUG,"Get simpledatachunk :%u msglen:%u ", msgtype, msglen);

    simpledatachunk = ripple_fastcompare_simpledatachunk_init();

    simpledatachunk->base.type = msgtype;

    /* 读取所有行crc */
    simpledatachunk->crc = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /* 读取行数 */
    simpledatachunk->datacnt = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /* 组装行数据 */
    for (rowindex = 0; rowindex < simpledatachunk->datacnt; rowindex++)
    {
        row = ripple_fastcompare_simplerow_init();

        /*读取每行的crc*/
        row->crc = RIPPLE_CONCAT(get, 32bit)(&uptr);

        /* 组装列数据 */
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
            row->privalues = lappend(row->privalues, column);
        }
        simpledatachunk->data = lappend(simpledatachunk->data, row);
    }
    return simpledatachunk;
}

