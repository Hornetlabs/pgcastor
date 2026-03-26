#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "storage/trail/data/fftrail_tbmetadata.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"

/*
 * tableinfoserialize
 * parameter:
 * force strongmakewill tableendrefactorwriteto trail
 * filemiddle，replacetabletableendrefactorsendlive changeoptimize dbid tabletoshould
 * libraryidentifier tbid tableidentifier *dbmdno libraryidentifierin trail filemiddle number output
 * parameter *tbmdno tableidentifierin trail filemiddle number output parameter state closekeyinfo
 */
bool fftrail_tbmetadata_serial(bool              force,
                               Oid               dbid,
                               Oid               tbid,
                               FullTransactionId xid,
                               uint32*           dbmdno,
                               uint32*           tbmdno,
                               void*             state)
{
    /*
     * checklookremainsurplusemptybetweenis whetherfullsufficientmostsmalloptimizeneedrequest
     * headpart+tailpart
     */
    bool                          found = false;
    uint16                        tmpcollen = 0;
    int                           index = 0;
    int                           hdrlen = 0;
    uint32                        typid = 0;
    uint32                        tlen = 0; /* record length */
    uint8*                        uptr = NULL;
    List*                         attrs = NULL;
    ListCell*                     lc = NULL;
    ff_tbmetadata*                fftbmd = NULL;
    ffsmgr_state*                 ffstate = NULL;
    file_buffer*                  fbuffer = NULL; /* cacheinfo */
    fftrail_privdata*             ffprivdata = NULL;
    fftrail_table_serialentry*    fftbentry = NULL;
    fftrail_database_serialentry* ffdbentry = NULL;
    pg_sysdict_Form_pg_class      class = NULL;
    pg_sysdict_Form_pg_type       type = NULL;
    pg_sysdict_Form_pg_namespace  namespace = NULL;
    fftrail_table_serialkey       fftbkey = {0};
    List*                         index_list = NULL;
    uint32                        indexnum = 0;

    /* gettableserializebyneed info */
    ffstate = (ffsmgr_state*)state;
    ffprivdata = (fftrail_privdata*)ffstate->fdata->ffdata;

    if (dbid == INVALIDOID)
    {
        dbid = ffstate->callback.getdboid(ffstate->privdata);
        if (NULL != ffstate->callback.setdboid)
        {
            ffstate->callback.setdboid(ffstate->privdata, dbid);
        }
    }

    /* checklookwhether exists，not storeinthen add */
    if (false == force)
    {
        fftbkey.dbid = dbid;
        fftbkey.tbid = tbid;
        fftbentry = hash_search(ffprivdata->tables, &fftbkey, HASH_FIND, &found);
        if (true == found)
        {
            *tbmdno = fftbentry->tbno;
            *dbmdno = fftbentry->dbno;
            return true;
        }
    }

    /* generatetableinfo */
    fftbmd = (ff_tbmetadata*)rmalloc0(sizeof(ff_tbmetadata));
    if (NULL == fftbmd)
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

    /* assemblelistinfo */
    /*
     * 1、getcolumn count
     * 2、assembletbmddata
     */
    /* according to oid getlist */
    attrs = (List*)ffstate->callback.getattributes(ffstate->privdata, tbid);
    fftbmd->colcnt = list_length(attrs);

    fftbmd->columns = (ff_column*)rmalloc0(sizeof(ff_column) * fftbmd->colcnt);
    if (NULL == fftbmd->columns)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->columns, 0, '\0', (sizeof(ff_column) * fftbmd->colcnt));

    foreach (lc, attrs)
    {
        pg_parser_sysdict_pgattributes* pgattrs = NULL;
        pgattrs = (pg_parser_sysdict_pgattributes*)lfirst(lc);

        /* in pg middlesmallin 0 fieldas systemsystemlist，not focus */
        if (0 >= pgattrs->attnum)
        {
            continue;
        }

        fftbmd->columns[pgattrs->attnum - 1].typid = pgattrs->atttypid;
        fftbmd->columns[pgattrs->attnum - 1].flag = 0;
        fftbmd->columns[pgattrs->attnum - 1].num = pgattrs->attnum;
        rmemcpy1(fftbmd->columns[pgattrs->attnum - 1].column,
                 0,
                 pgattrs->attname.data,
                 strlen(pgattrs->attname.data));
        if (0 < pgattrs->atttypid)
        {
            type = (pg_sysdict_Form_pg_type)ffstate->callback.gettype(ffstate->privdata,
                                                                      pgattrs->atttypid);
            rmemcpy1(fftbmd->columns[pgattrs->attnum - 1].typename,
                     0,
                     type->typname.data,
                     strlen(type->typname.data));
        }

        typid = pgattrs->atttypid;
        if (0 <= pgattrs->atttypmod)
        {
            if (PG_SYSDICT_BPCHAROID == typid || PG_SYSDICT_VARCHAROID == typid)
            {
                pgattrs->atttypmod -= (int32_t)sizeof(int32_t);
                fftbmd->columns[pgattrs->attnum - 1].length = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].precision = -1;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else if (PG_SYSDICT_TIMEOID == typid || PG_SYSDICT_TIMETZOID == typid ||
                     PG_SYSDICT_TIMESTAMPOID == typid || PG_SYSDICT_TIMESTAMPTZOID == typid)
            {
                fftbmd->columns[pgattrs->attnum - 1].length = -1;
                fftbmd->columns[pgattrs->attnum - 1].precision = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else if (PG_SYSDICT_NUMERICOID == typid)
            {
                pgattrs->atttypmod -= (int32_t)sizeof(int32_t);
                fftbmd->columns[pgattrs->attnum - 1].length = -1;
                fftbmd->columns[pgattrs->attnum - 1].precision =
                    (pgattrs->atttypmod >> 16) & 0xffff;
                fftbmd->columns[pgattrs->attnum - 1].scale = pgattrs->atttypmod & 0xffff;
            }
            else if (PG_SYSDICT_BITOID == typid || PG_SYSDICT_VARBITOID == typid)
            {
                fftbmd->columns[pgattrs->attnum - 1].length = pgattrs->atttypmod;
                fftbmd->columns[pgattrs->attnum - 1].precision = -1;
                fftbmd->columns[pgattrs->attnum - 1].scale = -1;
            }
            else
            {
                /* its othersituationlower, confirmkeep3valueas -1 */
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

        fftbmd->header.totallength += 8;  /* typeid(4) + flag(2) + attnum(2) */
        fftbmd->header.totallength += 12; /* length(4) + precision(4) + scale(4) */
        fftbmd->header.totallength += 2;  /* attname.data occupyuse charsection */
        fftbmd->header.totallength += strlen(fftbmd->columns[pgattrs->attnum - 1].column);
        fftbmd->header.totallength += 2; /* typname.data occupyuse charsection */
        if ('\0' != fftbmd->columns[pgattrs->attnum - 1].typename[0])
        {
            fftbmd->header.totallength += strlen(fftbmd->columns[pgattrs->attnum - 1].typename);
        }
    }

    /* freebigtransaction concatenate attribute linked list */
    if (ffstate->callback.freeattributes)
    {
        ffstate->callback.freeattributes(attrs);
    }

    /* according to oid gettableinfo */
    class = (pg_sysdict_Form_pg_class)ffstate->callback.getclass(ffstate->privdata, tbid);

    /* according to oid getindexinfo */
    index_list = (List*)ffstate->callback.getindex(ffstate->privdata, tbid);

    indexnum = index_list ? index_list->length : 0;

    fftbmd->table = class->relname.data;
    fftbmd->header.totallength += 2;
    fftbmd->header.totallength += strlen(fftbmd->table);

    fftbmd->identify = class->relreplident;
    fftbmd->header.totallength += 1;

    if ('\0' == class->nspname.data[0])
    {
        /* according to nspoid getschemainfo */
        namespace = (pg_sysdict_Form_pg_namespace)ffstate->callback.getnamespace(
            ffstate->privdata, class->relnamespace);
        fftbmd->schema = namespace->nspname.data;
    }
    else
    {
        fftbmd->schema = class->nspname.data;
    }

    fftbmd->header.totallength += 2;
    fftbmd->header.totallength += strlen(fftbmd->schema);

    /* checkvalidateandswitchswapblock */
    fftrail_serialpreshiftblock(state);
    if (FFSMGR_STATUS_SHIFTFILE == ffstate->status)
    {
        ffstate->status = FFSMGR_STATUS_USED;
    }

    /* according to bufid get fbuffer */
    fbuffer =
        file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /*
     * inprivatehas middleaddtableinfo
     */
    /* getdatalibraryidentifierinfo */
    ffdbentry = hash_search(ffprivdata->databases, &dbid, HASH_FIND, &found);
    if (false == found)
    {
        elog(RLOG_ERROR, "xsynch trail database logical error");
    }
    fftbmd->header.dbmdno = ffdbentry->no;
    *dbmdno = ffdbentry->no;

    /* towardfilemiddleaddtableinfo */
    fftbentry = hash_search(ffprivdata->tables, &fftbkey, HASH_ENTER, &found);
    if (false == found)
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

    /* towardfilemiddlewritetabledata */
    ffstate->recptr = fbuffer->data + fbuffer->start;

    /* addaddoffset */
    hdrlen = TOKENHDRSIZE;
    hdrlen += fftrail_data_headlen(ffstate->compatibility);

    /* dataoffset */
    fbuffer->start += hdrlen;

    /* fillfilltableinfo */
    /* calculatelength */
    fftbmd->header.totallength += 12; /* tbmdno(4) + (table)4 + (flag)2 + (colcnt)2) */

    /* fillfillindexinfo */
    /* calculatefixeddeterminelength */
    fftbmd->header.totallength += 4; /* indexnum(4) */

    if (0 < indexnum)
    {
        /* calculateeachindex length */
        foreach (lc, index_list)
        {
            catalog_index_value* index_value = (catalog_index_value*)lfirst(lc);

            /* calculatefixeddeterminelength */
            fftbmd->header.totallength +=
                10; /* indexrelid(4) + indisprimary(1) + indidentify(1) + indnatts(4) */

            /* calculatechangelonglength */
            fftbmd->header.totallength += (index_value->index->indnatts * sizeof(uint32));
        }
    }

    /* fillfilldata */
    /* tablenumber */
    fftrail_data_data2buffer(
        &fftbmd->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_INT, 4, (uint8*)&fftbmd->tbmdno);

    /* table oid */
    fftrail_data_data2buffer(
        &fftbmd->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_INT, 4, (uint8*)&fftbmd->oid);

    /* tableidentifier */
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

    /* tablemiddlelist number */
    fftrail_data_data2buffer(&fftbmd->header,
                             ffstate,
                             &fbuffer,
                             FTRAIL_TOKENDATATYPE_SMALLINT,
                             2,
                             (uint8*)&fftbmd->colcnt);

    /* table schemainfo */
    tmpcollen = (uint16)strlen(fftbmd->schema);
    fftrail_data_data2buffer(
        &fftbmd->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&tmpcollen);

    fftrail_data_data2buffer(&fftbmd->header,
                             ffstate,
                             &fbuffer,
                             FTRAIL_TOKENDATATYPE_STR,
                             tmpcollen,
                             (uint8*)fftbmd->schema);

    /* table name */
    tmpcollen = (uint16)strlen(fftbmd->table);
    fftrail_data_data2buffer(
        &fftbmd->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_SMALLINT, 2, (uint8*)&tmpcollen);

    fftrail_data_data2buffer(&fftbmd->header,
                             ffstate,
                             &fbuffer,
                             FTRAIL_TOKENDATATYPE_STR,
                             tmpcollen,
                             (uint8*)fftbmd->table);

    /* listinfo */
    for (index = 0; index < fftbmd->colcnt; index++)
    {
        /* listtype */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_INT,
                                 4,
                                 (uint8*)&fftbmd->columns[index].typid);

        /* listidentifier */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2,
                                 (uint8*)&fftbmd->columns[index].flag);

        /* listintablemiddle sequentialorder */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2,
                                 (uint8*)&fftbmd->columns[index].num);

        /* listtypelength */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_INT,
                                 4,
                                 (uint8*)&fftbmd->columns[index].length);

        /* listtyperefineddegree */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_INT,
                                 4,
                                 (uint8*)&fftbmd->columns[index].precision);

        /* listtypecarvedegree */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_INT,
                                 4,
                                 (uint8*)&fftbmd->columns[index].scale);

        /* listtypename lengthinfo */
        if ('\0' == fftbmd->columns[index].typename[0])
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

        /* already delete listatttypidas 0 */
        if (tmpcollen > 0)
        {
            /* listtypename */
            fftrail_data_data2buffer(&fftbmd->header,
                                     ffstate,
                                     &fbuffer,
                                     FTRAIL_TOKENDATATYPE_STR,
                                     tmpcollen,
                                     (uint8*)fftbmd->columns[index].typename);
        }

        /* column name lengthinfo */
        tmpcollen = (uint16)strlen(fftbmd->columns[index].column);
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_SMALLINT,
                                 2,
                                 (uint8*)&tmpcollen);

        /* column name */
        fftrail_data_data2buffer(&fftbmd->header,
                                 ffstate,
                                 &fbuffer,
                                 FTRAIL_TOKENDATATYPE_STR,
                                 tmpcollen,
                                 (uint8*)fftbmd->columns[index].column);
    }

    /* indexnum */
    fftrail_data_data2buffer(
        &fftbmd->header, ffstate, &fbuffer, FTRAIL_TOKENDATATYPE_INT, 4, (uint8*)&indexnum);

    /* inhas index situationlowerputsetindexinnercontent */
    if (0 < indexnum)
    {
        foreach (lc, index_list)
        {
            int                      key_index = 0;
            catalog_index_value*     index_value = (catalog_index_value*)lfirst(lc);
            pg_sysdict_Form_pg_index index_catalog = index_value->index;

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

            /* indkey changelong */
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

    /* writein Record token middle length */
    tlen = fftbmd->header.reclength;

    /* addaddrectail */
    uptr = fbuffer->data + fbuffer->start;
    FTRAIL_GROUP2BUFFER(put, TRAIL_TOKENDATA_RECTAIL, FFTRAIL_INFOTYPE_TOKEN, 0, uptr)
    fbuffer->start += TOKENHDRSIZE;
    tlen += TOKENHDRSIZE;

    /* record totallength */
    tlen += hdrlen;

    /* charsectiontoalign */
    tlen = MAXALIGN(tlen);
    fbuffer->start = MAXALIGN(fbuffer->start);

    /* assembleheadpartinfo */
    /* addaddGROUPinfo */
    FTRAIL_GROUP2BUFFER(put, FFTRAIL_GROUPTYPE_DATA, FFTRAIL_INFOTYPE_GROUP, tlen, ffstate->recptr)

    /* addaddheadpartinfo */
    fftrail_data_hdrserail(&fftbmd->header, ffstate);
    ffstate->recptr = NULL;

    /* memoryfree */
    if (NULL != fftbmd->columns)
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

/* tableinforeverseserialize */
bool fftrail_tbmetadata_deserial(void** data, void* state)
{
    uint8  tokenid = 0;   /* token identifier */
    uint8  tokeninfo = 0; /* token details */
    uint32 recoffset =
        0; /* foundationin record begin offset，used forpoint toneed needparse data */
    uint32         dataoffset = 0; /* foundationin data offset，used forcalculatecurrent record
                                      datapartdivide remainsurplusemptybetween */
    uint16         tmpcollen = 0;
    uint16         index = 0;
    uint16         subtype = FF_DATA_TYPE_NOP;
    uint32         tokenlen = 0; /* token length */

    uint8*         uptr = NULL;
    uint8*         tokendata = NULL; /* token data area */
    ff_tbmetadata* fftbmd = NULL;
    ffsmgr_state*  ffstate = NULL;
    uint32         indexnum = 0;

    /* typestrongturn */
    ffstate = (ffsmgr_state*)state;
    uptr = ffstate->recptr;

    /* requestemptybetween */
    fftbmd = (ff_tbmetadata*)rmalloc0(sizeof(ff_tbmetadata));
    if (NULL == fftbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd, 0, '\0', sizeof(ff_tbmetadata));
    *data = fftbmd;

    /* getheadpartidentifier */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if (FFTRAIL_GROUPTYPE_DATA != tokenid || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        /* make gcc happy */
        uptr = tokendata;
        elog(RLOG_ERROR, "trail file data format error");
    }
    recoffset = TOKENHDRSIZE;

    /* parseheadpartdata */
    uptr = ffstate->recptr;
    ffstate->recptr += recoffset;
    fftrail_data_hdrdeserail(&fftbmd->header, ffstate);

    /* keepinfo，because as insubsequent processlogiclogicmiddle，thissomedatacan can will is clean
     */
    subtype = fftbmd->header.subtype;

    /* heavynewpoint toheadpart */
    ffstate->recptr = uptr;
    recoffset += (uint16)fftrail_data_headlen(ffstate->compatibility);

    /*
     * parsetruerealdata
     * 1、checklookwhether empty record
     * 2、dataconcatinstall
     */
    /* gettablenumber */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_INT,
                                          4,
                                          (uint8*)&fftbmd->tbmdno))
    {
        return false;
    }

    /* gettable oid */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_INT,
                                          4,
                                          (uint8*)&fftbmd->oid))
    {
        return false;
    }

    /* gettable flag */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_SMALLINT,
                                          2,
                                          (uint8*)&fftbmd->flag))
    {
        return false;
    }

    /* gettable identify */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_TINYINT,
                                          1,
                                          (uint8*)&fftbmd->identify))
    {
        return false;
    }

    /* gettable colcnt */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_SMALLINT,
                                          2,
                                          (uint8*)&fftbmd->colcnt))
    {
        return false;
    }

    /* gettable schema */
    /* getlength */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_SMALLINT,
                                          2,
                                          (uint8*)&tmpcollen))
    {
        return false;
    }

    /* getschemaname */
    /* requestemptybetween */
    fftbmd->schema = (char*)rmalloc0(tmpcollen + 1);
    if (NULL == fftbmd->schema)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->schema, 0, '\0', (tmpcollen + 1));
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_STR,
                                          tmpcollen,
                                          (uint8*)fftbmd->schema))
    {
        return false;
    }

    /* gettable table name */
    /* getlength */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_SMALLINT,
                                          2,
                                          (uint8*)&tmpcollen))
    {
        return false;
    }

    /* getschemaname */
    fftbmd->table = (char*)rmalloc0(tmpcollen + 1);
    if (NULL == fftbmd->table)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->table, 0, '\0', (tmpcollen + 1));
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_STR,
                                          tmpcollen,
                                          (uint8*)fftbmd->table))
    {
        return false;
    }

    /* getlistinfo */
    fftbmd->columns = (ff_column*)rmalloc0(sizeof(ff_column) * fftbmd->colcnt);
    if (NULL == fftbmd->columns)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(fftbmd->columns, 0, '\0', (sizeof(ff_column) * fftbmd->colcnt));

    for (index = 0; index < fftbmd->colcnt; index++)
    {
        /* getlisttype */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT,
                                              4,
                                              (uint8*)&fftbmd->columns[index].typid))
        {
            return false;
        }

        /* getlistidentifier */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT,
                                              2,
                                              (uint8*)&fftbmd->columns[index].flag))
        {
            return false;
        }

        /* listintablemiddle sequentialorder */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT,
                                              2,
                                              (uint8*)&fftbmd->columns[index].num))
        {
            return false;
        }

        /* listtypelength */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT,
                                              4,
                                              (uint8*)&fftbmd->columns[index].length))
        {
            return false;
        }

        /* listtyperefineddegree*/
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT,
                                              4,
                                              (uint8*)&fftbmd->columns[index].precision))
        {
            return false;
        }

        /* listtypecarvedegree */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_INT,
                                              4,
                                              (uint8*)&fftbmd->columns[index].scale))
        {
            return false;
        }

        /* listtypename lengthinfo */
        /* listtypelength */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT,
                                              2,
                                              (uint8*)&tmpcollen))
        {
            return false;
        }

        /* listtypename */
        /* requestemptybetween */
        if (tmpcollen > 0)
        {
            rmemset1(fftbmd->columns[index].typename, 0, '\0', NAMEDATALEN);
            if (false == fftrail_data_buffer2data(&fftbmd->header,
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

        /* column name lengthinfo */
        /* listlength */
        if (false == fftrail_data_buffer2data(&fftbmd->header,
                                              ffstate,
                                              &recoffset,
                                              &dataoffset,
                                              FTRAIL_TOKENDATATYPE_SMALLINT,
                                              2,
                                              (uint8*)&tmpcollen))
        {
            return false;
        }

        /* column name */
        /* requestemptybetween */
        rmemset1(fftbmd->columns[index].column, 0, '\0', NAMEDATALEN);
        if (false == fftrail_data_buffer2data(&fftbmd->header,
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

    /* getindexidentifier */
    if (false == fftrail_data_buffer2data(&fftbmd->header,
                                          ffstate,
                                          &recoffset,
                                          &dataoffset,
                                          FTRAIL_TOKENDATATYPE_INT,
                                          4,
                                          (uint8*)&indexnum))
    {
        return false;
    }

    /* storeinindex */
    if (0 < indexnum)
    {
        int   index_index = 0;
        List* index_list = NULL;

        for (index_index = 0; index_index < indexnum; index_index++)
        {
            uint32          indexrelid = 0;
            bool            indisprimary = false;
            bool            indisidentify = false;
            uint32          indnatts = 0;
            ff_tbindex*     fftbindex = NULL;
            ff_tbindex_type fftbindex_type = FF_TBINDEX_TYPE_NOP;
            uint32*         key = NULL;
            int             index_key = 0;

            /* getindexrelid */
            if (false == fftrail_data_buffer2data(&fftbmd->header,
                                                  ffstate,
                                                  &recoffset,
                                                  &dataoffset,
                                                  FTRAIL_TOKENDATATYPE_INT,
                                                  4,
                                                  (uint8*)&indexrelid))
            {
                return false;
            }

            /* getindisprimary */
            if (false == fftrail_data_buffer2data(&fftbmd->header,
                                                  ffstate,
                                                  &recoffset,
                                                  &dataoffset,
                                                  FTRAIL_TOKENDATATYPE_TINYINT,
                                                  1,
                                                  (uint8*)&indisprimary))
            {
                return false;
            }

            /* getindisidentify */
            if (false == fftrail_data_buffer2data(&fftbmd->header,
                                                  ffstate,
                                                  &recoffset,
                                                  &dataoffset,
                                                  FTRAIL_TOKENDATATYPE_TINYINT,
                                                  1,
                                                  (uint8*)&indisidentify))
            {
                return false;
            }

            /* getindnatts */
            if (false == fftrail_data_buffer2data(&fftbmd->header,
                                                  ffstate,
                                                  &recoffset,
                                                  &dataoffset,
                                                  FTRAIL_TOKENDATATYPE_INT,
                                                  4,
                                                  (uint8*)&indnatts))
            {
                return false;
            }

            /* key initialize */
            key = rmalloc0(sizeof(uint32) * indnatts);
            if (!key)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(key, 0, 0, sizeof(uint32) * indnatts);

            for (index_key = 0; index_key < indnatts; index_key++)
            {
                /* getkey */
                if (false == fftrail_data_buffer2data(&fftbmd->header,
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

            /* itemfrontonly has primary and unique */
            fftbindex_type = indisprimary ? FF_TBINDEX_TYPE_PKEY : FF_TBINDEX_TYPE_UNIQUE;

            /* initialize */
            fftbindex = ff_tbindex_init(fftbindex_type, indnatts);

            /* setkey */
            fftbindex->index_key = key;
            fftbindex->index_identify = indisidentify;
            fftbindex->index_oid = indexrelid;

            index_list = lappend(index_list, fftbindex);
        }

        fftbmd->index = index_list;
    }

    /* heavyset，because as inswitchswapblockor filewhen，subtype valueas
     * :FF_DATA_TYPE_REC_CONTRECORD */
    fftbmd->header.subtype = subtype;
    return true;
}
