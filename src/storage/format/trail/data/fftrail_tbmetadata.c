#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"

/* 
 * 表信息序列化
 * 参数:
 *  force               强制将表结构写入到 trail 文件中，代表表结构发生了变化
 *  dbid                表对应的库标识
 *  tbid                表标识
 *  *dbmdno             库标识在 trail 文件中的编号             出参
 *  *tbmdno             表标识在 trail 文件中的编号             出参
 *  state               关键信息
 */
bool fftrail_tbmetadata_serial(bool force,
                                      Oid dbid,
                                      Oid tbid,
                                      FullTransactionId xid,
                                      uint32* dbmdno,
                                      uint32* tbmdno,
                                      void* state)
{
    /*
     * 查看剩余空间是否满足最小化要求
     *  头部+尾部
     */
    bool found = false;
    uint16 tmpcollen = 0;
    int index = 0;
    int hdrlen = 0;
    uint32 typid = 0;
    uint32 tlen = 0;                            /* record 长度     */
    uint8* uptr = NULL;
    List* attrs = NULL;
    ListCell* lc = NULL;
    ff_tbmetadata* fftbmd = NULL;
    ffsmgr_state* ffstate = NULL;
    file_buffer* fbuffer = NULL;                 /* 缓存信息 */
    fftrail_privdata* ffprivdata = NULL;
    fftrail_table_serialentry* fftbentry = NULL;
    fftrail_database_serialentry* ffdbentry = NULL;
    xk_pg_sysdict_Form_pg_class class = NULL;
    xk_pg_sysdict_Form_pg_type type = NULL;
    xk_pg_sysdict_Form_pg_namespace namespace = NULL;
    fftrail_table_serialkey   fftbkey = { 0 };
    List* index_list = NULL;
    uint32 indexnum = 0;

    /* 获取表序列化所需的信息 */
    ffstate = (ffsmgr_state*)state;
    ffprivdata = (fftrail_privdata*)ffstate->fdata->ffdata;

    if (dbid == InvalidOid)
    {
        dbid = ffstate->callback.getdboid(ffstate->privdata);
        if(NULL != ffstate->callback.setdboid)
        {
            ffstate->callback.setdboid(ffstate->privdata, dbid);
        }
    }

    /* 查看是否存在，不存在则添加 */
    if(false == force)
    {
        fftbkey.dbid = dbid;
        fftbkey.tbid = tbid;
        fftbentry = hash_search(ffprivdata->tables, &fftbkey, HASH_FIND, &found);
        if(true == found)
        {
            *tbmdno = fftbentry->tbno;
            *dbmdno = fftbentry->dbno;
            return true;
        }
    }

    /* 生成表信息 */
    fftbmd = (ff_tbmetadata*)rmalloc0(sizeof(ff_tbmetadata));
    if(NULL == fftbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd, 0, '\0', sizeof(ff_tbmetadata));
    fftbmd->oid = tbid;
    fftbmd->header.formattype = FF_DATA_FORMATTYPE_SQL;
    fftbmd->header.reccount = 1;
    fftbmd->header.subtype = FF_DATA_TYPE_TBMETADATA;
    fftbmd->header.transid = xid;
    fftbmd->flag = FF_TBMETADATA_FLAG_NOP;

    /* 组装列信息 */
    /*
     * 1、获取列数
     * 2、组装tbmd数据
     */
    /* 根据 oid 获取列 */
    attrs = (List*)ffstate->callback.getattributes(ffstate->privdata, tbid);
    fftbmd->colcnt = list_length(attrs);

    fftbmd->columns = (ff_column*)rmalloc0(sizeof(ff_column)*fftbmd->colcnt);
    if(NULL == fftbmd->columns)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->columns, 0, '\0', (sizeof(ff_column)*fftbmd->colcnt));

    foreach(lc, attrs)
    {
        xk_pg_parser_sysdict_pgattributes* pgattrs = NULL;
        pgattrs = (xk_pg_parser_sysdict_pgattributes*)lfirst(lc);

        /* 在 pg 中小于 0 的字段为系统列，不关注 */
        if(0 >= pgattrs->attnum)
        {
            continue;
        }

        fftbmd->columns[pgattrs->attnum - 1].typid = pgattrs->atttypid;
        fftbmd->columns[pgattrs->attnum - 1].flag = 0;
        fftbmd->columns[pgattrs->attnum - 1].num = pgattrs->attnum;
        rmemcpy1(fftbmd->columns[pgattrs->attnum - 1].column, 0, pgattrs->attname.data, strlen(pgattrs->attname.data));
        if (0 < pgattrs->atttypid)
        {
            type = (xk_pg_sysdict_Form_pg_type)ffstate->callback.gettype(ffstate->privdata, pgattrs->atttypid);
            rmemcpy1(fftbmd->columns[pgattrs->attnum - 1].typename, 0, type->typname.data, strlen(type->typname.data));
        }


        typid = pgattrs->atttypid;
        if (0 <= pgattrs->atttypmod)
        {
            if (XK_PG_SYSDICT_BPCHAROID == typid || XK_PG_SYSDICT_VARCHAROID == typid)
            {
                pgattrs->atttypmod -= (int32_t) sizeof(int32_t);
                fftbmd->columns[pgattrs->attnum - 1].length = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].precision = -1;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else if (XK_PG_SYSDICT_TIMEOID == typid || XK_PG_SYSDICT_TIMETZOID == typid
                || XK_PG_SYSDICT_TIMESTAMPOID == typid || XK_PG_SYSDICT_TIMESTAMPTZOID == typid)
            {
                fftbmd->columns[pgattrs->attnum - 1].length = -1;
                fftbmd->columns[pgattrs->attnum - 1].precision = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else if (XK_PG_SYSDICT_NUMERICOID == typid)
            {
                pgattrs->atttypmod -= (int32_t) sizeof(int32_t);
                fftbmd->columns[pgattrs->attnum - 1].length = -1;
                fftbmd->columns[pgattrs->attnum - 1].precision = (pgattrs->atttypmod >> 16) & 0xffff;
                fftbmd->columns[pgattrs->attnum - 1].scale = pgattrs->atttypmod & 0xffff;
            }
            else if (XK_PG_SYSDICT_BITOID == typid|| XK_PG_SYSDICT_VARBITOID == typid)
            {
                fftbmd->columns[pgattrs->attnum - 1].length = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].precision = -1;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else
            {
                /* 其他情况下, 确保3个值为-1 */
                fftbmd->columns[pgattrs->attnum - 1].length = -1;
                fftbmd->columns[pgattrs->attnum - 1].precision = -1;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
        }
        else
        {
            fftbmd->columns[pgattrs->attnum - 1].length = -1;
            fftbmd->columns[pgattrs->attnum - 1].precision = -1;
            fftbmd->columns[pgattrs->attnum - 1].scale = -1;
        }

        fftbmd->header.totallength += 8; /* typeid(4) + flag(2) + attnum(2) */
        fftbmd->header.totallength += 12; /* length(4) + precision(4) + scale(4) */
        fftbmd->header.totallength += 2; /* attname.data 占用的字节 */
        fftbmd->header.totallength += strlen(fftbmd->columns[pgattrs->attnum - 1].column);
        fftbmd->header.totallength += 2; /* typname.data 占用的字节 */
        if (NULL != fftbmd->columns[pgattrs->attnum - 1].typename )
        {
            fftbmd->header.totallength += strlen(fftbmd->columns[pgattrs->attnum - 1].typename);
        }

    }

    /* 释放大事务拼凑的attribute链表 */
    if (ffstate->callback.freeattributes)
    {
        ffstate->callback.freeattributes(attrs);
    }

    /* 根据 oid 获取表信息 */
    class = (xk_pg_sysdict_Form_pg_class)ffstate->callback.getclass(ffstate->privdata, tbid);

    /* 根据 oid 获取索引信息 */
    index_list = (List *)ffstate->callback.getindex(ffstate->privdata, tbid);

    indexnum = index_list ? index_list->length : 0;

    fftbmd->table = class->relname.data;
    fftbmd->header.totallength += 2;
    fftbmd->header.totallength += strlen(fftbmd->table);

    fftbmd->identify = class->relreplident;
    fftbmd->header.totallength += 1;

    if('\0' == class->nspname.data[0])
    {
        /* 根据 nspoid 获取模式信息 */
        namespace = (xk_pg_sysdict_Form_pg_namespace)ffstate->callback.getnamespace(ffstate->privdata, class->relnamespace);
        fftbmd->schema = namespace->nspname.data;
    }
    else
    {
        fftbmd->schema = class->nspname.data;
    }

    fftbmd->header.totallength += 2;
    fftbmd->header.totallength += strlen(fftbmd->schema);

    /* 检验并切换block */
    fftrail_serialpreshiftblock(state);
    if(FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* 根据 bufid 获取 fbuffer */
    fbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /*
     * 在私有中添加表信息
     */
    /* 获取数据库标识信息 */
    ffdbentry = hash_search(ffprivdata->databases, &dbid, HASH_FIND, &found);
    if(false == found)
    {
        elog(RLOG_ERROR, "xsynch trail database logical error");
    }
    fftbmd->header.dbmdno = ffdbentry->no;
    *dbmdno = ffdbentry->no;

    /* 向文件中添加表信息 */
    fftbentry = hash_search(ffprivdata->tables, &fftbkey, HASH_ENTER, &found);
    if(false == found)
    {
        fftbentry->key.dbid = dbid;
        fftbentry->key.tbid = fftbmd->oid;
        fftbentry->dbno = ffdbentry->no;
        fftbentry->tbno = ffprivdata->tbnum++;
        rmemcpy1(fftbentry->schema, 0, fftbmd->schema, strlen(fftbmd->schema));
        rmemcpy1(fftbentry->table, 0, fftbmd->table, strlen(fftbmd->table));
        ffprivdata->tbentrys = lappend(ffprivdata->tbentrys, fftbentry);
    }
    fftbmd->tbmdno = fftbentry->tbno;
    *tbmdno = fftbmd->tbmdno;

    /* 向文件中写入表数据 */
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* 增加偏移 */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);

    /* 数据偏移 */
    fbuffer->start += hdrlen;

    /* 填充表信息 */
    /* 计算长度 */
    fftbmd->header.totallength += 12;            /* tbmdno(4) + (table)4 + (flag)2 + (colcnt)2) */

    /* 填充index信息 */
    /* 计算固定长度 */
    fftbmd->header.totallength += 4;        /* indexnum(4) */

    if (0 < indexnum)
    {
        /* 计算每个index的长度 */
        foreach(lc, index_list)
        {
            catalog_index_value *index_value = (catalog_index_value*)lfirst(lc);

            /* 计算固定长度 */
            fftbmd->header.totallength += 10;       /* indexrelid(4) + indisprimary(1) + indidentify(1) + indnatts(4) */

            /* 计算变长长度 */
            fftbmd->header.totallength += (index_value->index->indnatts * sizeof(uint32));
        }
    }

    /* 填充数据 */
    /* 表编号 */
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_INT,
                                    4,
                                    (uint8*)&fftbmd->tbmdno);

    /* 表oid */
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_INT,
                                    4,
                                    (uint8*)&fftbmd->oid);

    /* 表标识 */
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&fftbmd->flag);

    /* identity */
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_TINYINT,
                                    1,
                                    (uint8*)&fftbmd->identify);

    /* 表中列的个数 */
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&fftbmd->colcnt);

    /* 表的模式信息 */
    tmpcollen = (uint16)strlen(fftbmd->schema);
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&tmpcollen);

    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_STR,
                                    tmpcollen,
                                    (uint8*)fftbmd->schema);

    /* 表名 */
    tmpcollen = (uint16)strlen(fftbmd->table);
    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                    2,
                                    (uint8*)&tmpcollen);

    fftrail_data_data2buffer(&fftbmd->header,
                                    ffstate,
                                    &fbuffer,
                                    FTRAIL_TOKENDATATYPE_STR,
                                    tmpcollen,
                                    (uint8*)fftbmd->table);

    /* 列信息 */
    for(index = 0; index < fftbmd->colcnt; index++)
    {
        /* 列类型 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&fftbmd->columns[index].typid);

        /* 列标识 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&fftbmd->columns[index].flag);

        /* 列在表中的顺序 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&fftbmd->columns[index].num);

        /* 列类型长度 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&fftbmd->columns[index].length);

        /* 列类型精度 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&fftbmd->columns[index].precision);

        /* 列类型刻度 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&fftbmd->columns[index].scale);

         /* 列类型名称的长度信息 */
        if (NULL == fftbmd->columns[index].typename )
        {
            tmpcollen = 0;
        }
        else
        {
            tmpcollen = (uint16)strlen(fftbmd->columns[index].typename);
        }
        
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&tmpcollen);

        /* 已删除的列atttypid为0 */
        if (tmpcollen > 0)
        {
            /* 列类型名 */
            fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_STR,
                                            tmpcollen,
                                            (uint8*)fftbmd->columns[index].typename);
        }

        /* 列名的长度信息 */
        tmpcollen = (uint16)strlen(fftbmd->columns[index].column);
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                        2,
                                        (uint8*)&tmpcollen);

        /* 列名 */
        fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_STR,
                                        tmpcollen,
                                        (uint8*)fftbmd->columns[index].column);
    }

    /* indexnum */
    fftrail_data_data2buffer(&fftbmd->header,
                                        ffstate,
                                        &fbuffer,
                                        FTRAIL_TOKENDATATYPE_INT,
                                        4,
                                        (uint8*)&indexnum);

    /* 在有索引的情况下放置索引内容 */
    if (0 < indexnum)
    {
        foreach(lc, index_list)
        {
            int key_index = 0;
            catalog_index_value *index_value = (catalog_index_value*)lfirst(lc);
            xk_pg_sysdict_Form_pg_index index_catalog = index_value->index;

            /* indexrelid 4 */
            fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_INT,
                                            4,
                                            (uint8*)&index_catalog->indexrelid);

            /* indisprimary 1 */
            fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_TINYINT,
                                            1,
                                            (uint8*)&index_catalog->indisprimary);

            /* indisreplident 1 */
            fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_TINYINT,
                                            1,
                                            (uint8*)&index_catalog->indisreplident);

            /* indnatts 4 */
            fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_INT,
                                            4,
                                            (uint8*)&index_catalog->indnatts);

            /* indkey 变长 */
            for (key_index = 0; key_index < index_catalog->indnatts; key_index++)
            {
                uint32 key_num = index_catalog->indkey[key_index];

                fftrail_data_data2buffer(&fftbmd->header,
                                            ffstate,
                                            &fbuffer,
                                            FTRAIL_TOKENDATATYPE_INT,
                                            4,
                                            (uint8*)&key_num);
            }
        }
    }

    /* 写在 Record token 中的长度 */
    tlen = fftbmd->header.reclength;

    /* 增加rectail */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put,
                                TRAIL_TOKENDATA_RECTAIL,
                                FFTRAIL_INFOTYPE_TOKEN,
                                0,
                                uptr)
    fbuffer->start += TOKENHDRSIZE;
    tlen += TOKENHDRSIZE;

    /* record 总长度 */
    tlen += hdrlen;

    /* 字节对齐 */
    tlen = MAXALIGN(tlen);
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* 组装头部信息 */
    /* 增加GROUP信息 */
    FTRAIL_GROUP2BUFFER(put,
                                FFTRAIL_GROUPTYPE_DATA,
                                FFTRAIL_INFOTYPE_GROUP,
                                tlen,
                                ffstate->recptr)

    /* 增加头部信息 */
    fftrail_data_hdrserail(&fftbmd->header, ffstate);
    ffstate->recptr = NULL;

    /* 内存释放 */
    if(NULL != fftbmd->columns)
    {
        rfree(fftbmd->columns);
    }
    if (index_list)
    {
        list_free(index_list);
    }
    rfree(fftbmd);
    return true;
}

/* 表信息反序列化 */
bool fftrail_tbmetadata_deserial(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  recoffset = 0;                      /* 基于 record 开始的偏移，用于指向需要解析的数据 */
    uint32  dataoffset = 0;                     /* 基于 数据 的偏移，用于计算当前 record 数据部分的剩余空间 */
    uint16  tmpcollen = 0;
    uint16  index = 0;
    uint16  subtype = FF_DATA_TYPE_NOP;
    uint32  tokenlen = 0;                       /* token 长度 */

    uint8*  uptr = NULL;
    uint8*  tokendata = NULL;                   /* token 数据区 */
    ff_tbmetadata* fftbmd = NULL;
    ffsmgr_state* ffstate = NULL;
    uint32 indexnum = 0;

    /* 类型强转 */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* 申请空间 */
    fftbmd = (ff_tbmetadata*)rmalloc0(sizeof(ff_tbmetadata));
    if(NULL == fftbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd, 0, '\0', sizeof(ff_tbmetadata));
    *data = fftbmd;

    /* 获取头部标识 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(FFTRAIL_GROUPTYPE_DATA != tokenid
        || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    recoffset = TOKENHDRSIZE;

    /* 解析头部数据 */
    uptr = ffstate->recptr;
    ffstate->recptr += recoffset;
    fftrail_data_hdrdeserail(&fftbmd->header, ffstate);

    /* 保留信息，因为在后续的处理逻辑中，这些数据可能会被清理 */
    subtype = fftbmd->header.subtype;

    /* 重新指向头部 */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    /* 
     * 解析真实数据
     *  1、查看是否为空的 record
     *  2、数据拼装
     */
    /* 获取表编号 */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&fftbmd->tbmdno))
    {
        return false;
    }

    /* 获取表的 oid */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&fftbmd->oid))
    {
        return false;
    }

    /* 获取表的 flag */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&fftbmd->flag))
    {
        return false;
    }

    /* 获取表的 identify */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_TINYINT,
                                                    1,
                                                    (uint8*)&fftbmd->identify))
    {
        return false;
    }

    /* 获取表的 colcnt */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&fftbmd->colcnt))
    {
        return false;
    }

    /* 获取表的 模式 */
    /* 获取长度 */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&tmpcollen))
    {
        return false;
    }

    /* 获取模式名称 */
    /* 申请空间 */
    fftbmd->schema = (char*)rmalloc0(tmpcollen + 1);
    if(NULL == fftbmd->schema)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->schema, 0, '\0', (tmpcollen + 1));
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_STR,
                                                    tmpcollen,
                                                    (uint8*)fftbmd->schema))
    {
        return false;
    }

    /* 获取表的 表名 */
    /* 获取长度 */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&tmpcollen))
    {
        return false;
    }

    /* 获取模式名称 */
    fftbmd->table = (char*)rmalloc0(tmpcollen + 1);
    if(NULL == fftbmd->table)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->table, 0, '\0', (tmpcollen + 1));
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_STR,
                                                    tmpcollen,
                                                    (uint8*)fftbmd->table))
    {
        return false;
    }

    /* 获取列信息 */
    fftbmd->columns = (ff_column*)rmalloc0(sizeof(ff_column)*fftbmd->colcnt);
    if(NULL == fftbmd->columns)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->columns, 0, '\0', (sizeof(ff_column)*fftbmd->colcnt));

    for(index = 0; index < fftbmd->colcnt; index++)
    {
        /* 获取列类型 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_INT,
                                                        4,
                                                        (uint8*)&fftbmd->columns[index].typid))
        {
            return false;
        }

        /* 获取列标识 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                                        2,
                                                        (uint8*)&fftbmd->columns[index].flag))
        {
            return false;
        }

        /* 列在表中的顺序 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_SMALLINT,
                                                        2,
                                                        (uint8*)&fftbmd->columns[index].num))
        {
            return false;
        }

        /* 列类型长度 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                     ffstate,
                                                     &recoffset,
                                                     &dataoffset,
                                                     FTRAIL_TOKENDATATYPE_INT,
                                                     4,
                                                     (uint8*)&fftbmd->columns[index].length))
        {
            return false;
        }

        /* 列类型精度*/
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                     ffstate,
                                                     &recoffset,
                                                     &dataoffset,
                                                     FTRAIL_TOKENDATATYPE_INT,
                                                     4,
                                                     (uint8*)&fftbmd->columns[index].precision))
        {
            return false;
        }

        /* 列类型刻度 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                     ffstate,
                                                     &recoffset,
                                                     &dataoffset,
                                                     FTRAIL_TOKENDATATYPE_INT,
                                                     4,
                                                     (uint8*)&fftbmd->columns[index].scale))
        {
            return false;
        }

        /* 列类型名的长度信息 */
        /* 列类型长度 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&tmpcollen))
        {
            return false;
        }

        /* 列类型名 */
        /* 申请空间 */
        if (tmpcollen > 0)
        {
            rmemset1(fftbmd->columns[index].typename, 0, '\0', NAMEDATALEN);
            if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                            ffstate,
                                                            &recoffset,
                                                            &dataoffset,
                                                            FTRAIL_TOKENDATATYPE_STR,
                                                            tmpcollen,
                                                            (uint8*)fftbmd->columns[index].typename))
            {
                return false;
            }
        }

        /* 列名的长度信息 */
        /* 列长度 */
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_SMALLINT,
                                                    2,
                                                    (uint8*)&tmpcollen))
        {
            return false;
        }

        /* 列名 */
        /* 申请空间 */
        rmemset1(fftbmd->columns[index].column, 0, '\0', NAMEDATALEN);
        if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_STR,
                                                        tmpcollen,
                                                        (uint8*)fftbmd->columns[index].column))
        {
            return false;
        }
    }

    /* 获取索引标识 */
    if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&indexnum))
    {
        return false;
    }

    /* 存在索引 */
    if (0 < indexnum)
    {
        int index_index = 0;
        List *index_list = NULL;

        for (index_index = 0; index_index < indexnum; index_index++)
        {
            uint32 indexrelid = 0;
            bool indisprimary = false;
            bool indisidentify = false;
            uint32 indnatts = 0;
            ff_tbindex* fftbindex = NULL;
            ff_tbindex_type fftbindex_type = FF_TBINDEX_TYPE_NOP;
            uint32* key = NULL;
            int index_key = 0;

            /* 获取indexrelid */
            if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&indexrelid))
            {
                return false;
            }

            /* 获取indisprimary */
            if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_TINYINT,
                                                    1,
                                                    (uint8*)&indisprimary))
            {
                return false;
            }

            /* 获取indisidentify */
            if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_TINYINT,
                                                    1,
                                                    (uint8*)&indisidentify))
            {
                return false;
            }

            /* 获取indnatts */
            if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                    ffstate,
                                                    &recoffset,
                                                    &dataoffset,
                                                    FTRAIL_TOKENDATATYPE_INT,
                                                    4,
                                                    (uint8*)&indnatts))
            {
                return false;
            }

            /* key 初始化 */
            key = rmalloc0(sizeof(uint32) * indnatts);
            if (!key)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(key, 0, 0, sizeof(uint32) * indnatts);

            for (index_key = 0; index_key < indnatts; index_key++)
            {
                /* 获取key */
                if(false  == fftrail_data_buffer2data(&fftbmd->header,
                                                        ffstate,
                                                        &recoffset,
                                                        &dataoffset,
                                                        FTRAIL_TOKENDATATYPE_INT,
                                                        4,
                                                        (uint8*)&key[index_key]))
                {
                    return false;
                }
            }

            /* 目前只有 primary 和 unique */
            fftbindex_type = indisprimary ? FF_TBINDEX_TYPE_PKEY : FF_TBINDEX_TYPE_UNIQUE;

            /* 初始化 */
            fftbindex = ff_tbindex_init(fftbindex_type, indnatts);

            /* 设置key */
            fftbindex->index_key = key;
            fftbindex->index_identify = indisidentify;
            fftbindex->index_oid = indexrelid;

            index_list = lappend(index_list, fftbindex);
        }

        fftbmd->index = index_list;
    }

    /* 重设，因为在切换block或file时，subtype的值为:FF_DATA_TYPE_REC_CONTRECORD */
    fftbmd->header.subtype = subtype;
    return true;
}
