#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/conn/conn.h"
#include "utils/hash/hash_utils.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "catalog/constraint.h"

static int compare_int16(const void* a, const void* b)
{
    return (*(int16_t*)a - *(int16_t*)b);
}

static void constraint_splitconkey(const char* input, int16_t** conkey, int16_t* conkeycnt)
{
    int    count;
    int    index = 0;
    size_t len = 0;
    char*  token = NULL;
    char*  pindex = NULL;
    char*  content = NULL;

    *conkeycnt = 0;
    *conkey = NULL;
    if (NULL == input)
    {
        return;
    }

    len = strlen(input);

    if (len < 2 || input[0] != '{' || input[len - 1] != '}')
    {
        elog(RLOG_WARNING, "Invalid conkey", strerror(errno));
        return;
    }

    /* Content in the middle */
    content = rstrndup(input + 1, len - 2);

    /* Initial count is 1 */
    count = 1;
    for (pindex = content; *pindex; pindex++)
    {
        if (*pindex == ',')
        {
            count++;
        }
    }

    /* Allocate space */
    *conkey = (int16_t*)rmalloc0(count * sizeof(int16_t));
    if (NULL == *conkey)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        rfree(content);
        *conkey = NULL;
        return;
    }

    /* Store conkey */
    token = strtok(content, ",");
    for (index = 0; index < count; index++)
    {
        sscanf(token, "%hd", &(*conkey)[index]);
        token = strtok(NULL, ",");
    }

    *conkeycnt = count;

    qsort(*conkey, count, sizeof(int16_t), compare_int16);

    rfree(content);

    return;
}

void constraint_getfromdb(PGconn* conn, cache_sysdicts* sysdicts)
{
    int                           i, j;
    HASHCTL                       hash_ctl;
    bool                          found = false;
    PGresult*                     res = NULL;
    pg_sysdict_Form_pg_constraint constraint;
    catalog_constraint_value*     entry = NULL;
    const char*                   query =
        "SELECT rel.oid , rel.conname, rel.connamespace, rel.contype, rel.conrelid, rel.conkey "
        "FROM pg_constraint rel where contype = 'p';";

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(catalog_constraint_value);
    sysdicts->by_constraint =
        hash_create("catalog_sysdict_constraint", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    res = conn_exec(conn, query);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "pg_constraint query failed");
    }

    for (i = 0; i < PQntuples(res); i++)
    {
        constraint =
            (pg_sysdict_Form_pg_constraint)rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));
        if (NULL == constraint)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(constraint, 0, '\0', sizeof(pg_parser_sysdict_pgconstraint));
        j = 0;
        constraint->oid = strtoul(PQgetvalue(res, i, j++), NULL, 10);
        strcpy(constraint->conname.data, PQgetvalue(res, i, j++));
        sscanf(PQgetvalue(res, i, j++), "%u", &constraint->connamespace);
        sscanf(PQgetvalue(res, i, j++), "%c", &constraint->contype);
        sscanf(PQgetvalue(res, i, j++), "%u", &constraint->conrelid);
        constraint_splitconkey(
            PQgetvalue(res, i, j++), &constraint->conkey, &constraint->conkeycnt);

        entry = (catalog_constraint_value*)hash_search(
            sysdicts->by_constraint, &constraint->conrelid, HASH_ENTER, &found);
        if (false)
        {
            elog(RLOG_ERROR,
                 "authid_oid:%u already exist in by_constraint",
                 entry->constraint->conrelid);
        }

        entry->conrelid = constraint->conrelid;
        entry->constraint = constraint;
    }

    PQclear(res);

    return;
}

/* colvalue2constraint */
catalogdata* constraint_colvalue2constraint(void* in_colvalue)
{
    catalogdata*                    catalogconstraint = NULL;
    pg_parser_translog_tbcol_value* colvalue = NULL;
    pg_sysdict_Form_pg_constraint   pgconstraint = NULL;
    catalog_constraint_value*       constraintvalue = NULL;

    colvalue = (pg_parser_translog_tbcol_value*)in_colvalue;

    /* Value conversion */
    catalogconstraint = (catalogdata*)rmalloc0(sizeof(catalogdata));
    if (NULL == catalogconstraint)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(catalogconstraint, 0, '\0', sizeof(catalogdata));

    constraintvalue = (catalog_constraint_value*)rmalloc0(sizeof(catalog_constraint_value));
    if (NULL == constraintvalue)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(constraintvalue, 0, '\0', sizeof(catalog_constraint_value));
    catalogconstraint->catalog = constraintvalue;
    catalogconstraint->type = CATALOG_TYPE_CONSTRAINT;

    pgconstraint = (pg_sysdict_Form_pg_constraint)rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));
    if (NULL == pgconstraint)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pgconstraint, 0, '\0', sizeof(pg_parser_sysdict_pgconstraint));
    constraintvalue->constraint = pgconstraint;

    /* oid  0*/
    sscanf((char*)(colvalue->m_value), "%u", &pgconstraint->oid);

    /*
     * postgres conrelid 7 conkey 18
     */
    sscanf((char*)((colvalue + 7)->m_value), "%u", &pgconstraint->conrelid);
    /* Split conkey */
    constraint_splitconkey(
        (char*)((colvalue + 18)->m_value), &pgconstraint->conkey, &pgconstraint->conkeycnt);

    constraintvalue->conrelid = pgconstraint->conrelid;

    /* contype 3 */
    pgconstraint->contype = ((char*)((colvalue + 3)->m_value))[0];

    /* conname 1 */
    rmemcpy1(pgconstraint->conname.data,
             0,
             (char*)((colvalue + 1)->m_value),
             (colvalue + 1)->m_valueLen);

    /* connamespace 2 */
    sscanf((char*)((colvalue + 2)->m_value), "%u", &pgconstraint->connamespace);

    return catalogconstraint;
}

/* catalogdata2transcache */
void constraint_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata)
{
    bool                      found = false;
    catalog_class_value*      classInHash = NULL;
    catalog_constraint_value* newcatalog = NULL;
    catalog_constraint_value* catalogInHash = NULL;

    if (NULL == catalogdata || NULL == catalogdata->catalog)
    {
        return;
    }

    newcatalog = (catalog_constraint_value*)catalogdata->catalog;

    if ('p' != newcatalog->constraint->contype)
    {
        return;
    }

    /* Find in hash */
    classInHash =
        hash_search(sysdicts->by_class, &newcatalog->constraint->conrelid, HASH_FIND, &found);
    if (false == found)
    {
        elog(RLOG_WARNING, "by_class can not found, %u", newcatalog->constraint->conrelid);
        return;
    }

    if (CATALOG_OP_INSERT == catalogdata->op)
    {
        classInHash->class->relhaspk = true;
        catalogInHash =
            hash_search(sysdicts->by_constraint, &newcatalog->conrelid, HASH_ENTER, &found);
        if (true == found)
        {
            elog(RLOG_WARNING,
                 "by_constraint hash duplicate conrelid, %u, %s",
                 catalogInHash->constraint->conrelid,
                 catalogInHash->constraint->conname.data);

            if (NULL != catalogInHash->constraint)
            {
                if (0 != catalogInHash->constraint->conkeycnt)
                {
                    rfree(catalogInHash->constraint->conkey);
                }
                rfree(catalogInHash->constraint);
            }
        }
        catalogInHash->conrelid = newcatalog->conrelid;
        catalogInHash->constraint =
            (pg_sysdict_Form_pg_constraint)rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));
        if (NULL == catalogInHash->constraint)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }

        if (newcatalog->constraint->conkeycnt == 0)
        {
            rmemcpy0(catalogInHash->constraint,
                     0,
                     newcatalog->constraint,
                     sizeof(pg_parser_sysdict_pgconstraint));
        }
        else
        {
            rmemcpy0(catalogInHash->constraint,
                     0,
                     newcatalog->constraint,
                     offsetof(pg_parser_sysdict_pgconstraint, conkey));
            catalogInHash->constraint->conkey =
                (int16_t*)rmalloc0(newcatalog->constraint->conkeycnt * sizeof(int16_t));
            if (NULL == catalogInHash->constraint->conkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(catalogInHash->constraint->conkey,
                     0,
                     '\0',
                     newcatalog->constraint->conkeycnt * sizeof(int16_t));
            rmemcpy0(catalogInHash->constraint->conkey,
                     0,
                     newcatalog->constraint->conkey,
                     newcatalog->constraint->conkeycnt * sizeof(int16_t));
        }
    }
    else if (CATALOG_OP_DELETE == catalogdata->op)
    {
        elog(RLOG_DEBUG,
             "CATALOG_OP_DELETE == catalogdata->op %s",
             newcatalog->constraint->conname.data);
        classInHash->class->relhaspk = false;
        catalogInHash =
            hash_search(sysdicts->by_constraint, &newcatalog->conrelid, HASH_REMOVE, &found);
        if (NULL != catalogInHash)
        {
            if (NULL != catalogInHash->constraint)
            {
                if (0 != catalogInHash->constraint->conkeycnt)
                {
                    rfree(catalogInHash->constraint->conkey);
                }
                rfree(catalogInHash->constraint);
            }
        }
    }
    else if (CATALOG_OP_UPDATE == catalogdata->op)
    {
        catalogInHash =
            hash_search(sysdicts->by_constraint, &newcatalog->conrelid, HASH_FIND, &found);
        if (NULL == catalogInHash)
        {
            elog(RLOG_WARNING,
                 "by_constraint not found conrelid, %u, %s",
                 newcatalog->constraint->conrelid,
                 newcatalog->constraint->conname.data);
            return;
        }
        if (0 != catalogInHash->constraint->conkeycnt)
        {
            rfree(catalogInHash->constraint->conkey);
        }
        rfree(catalogInHash->constraint);

        catalogInHash->constraint =
            (pg_sysdict_Form_pg_constraint)rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));
        if (NULL == catalogInHash->constraint)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }

        if (newcatalog->constraint->conkeycnt == 0)
        {
            rmemcpy0(catalogInHash->constraint,
                     0,
                     newcatalog->constraint,
                     sizeof(pg_parser_sysdict_pgconstraint));
        }
        else
        {
            rmemcpy0(catalogInHash->constraint,
                     0,
                     newcatalog->constraint,
                     offsetof(pg_parser_sysdict_pgconstraint, conkey));
            catalogInHash->constraint->conkey =
                (int16_t*)rmalloc0(newcatalog->constraint->conkeycnt * sizeof(int16_t));
            if (NULL == catalogInHash->constraint->conkey)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(catalogInHash->constraint->conkey,
                     0,
                     '\0',
                     newcatalog->constraint->conkeycnt * sizeof(int16_t));
            rmemcpy0(catalogInHash->constraint->conkey,
                     0,
                     newcatalog->constraint->conkey,
                     newcatalog->constraint->conkeycnt * sizeof(int16_t));
        }
    }
    else
    {
        elog(RLOG_ERROR, "unknown op:%d", catalogdata->op);
    }
}

void constraint_catalogdatafree(catalogdata* catalogdata)
{
    catalog_constraint_value* catalog = NULL;
    if (NULL == catalogdata)
    {
        return;
    }

    if (NULL == catalogdata->catalog)
    {
        rfree(catalogdata);
        return;
    }

    /* Catalog memory release */
    catalog = (catalog_constraint_value*)catalogdata->catalog;
    if (NULL != catalog->constraint)
    {
        if (0 != catalog->constraint->conkeycnt)
        {
            rfree(catalog->constraint->conkey);
        }
        rfree(catalog->constraint);
    }
    rfree(catalogdata->catalog);
    rfree(catalogdata);
}

void constraintdata_write(List* constraint_list, uint64* offset, sysdict_header_array* array)
{
    int                           fd;

    uint64                        page_num = 0;
    char                          buffer[FILE_BLK_SIZE];
    size_t                        page_offset = PAGE_HEADER_SIZE;
    size_t                        constraint_size = 0;
    ListCell*                     cell = NULL;
    pg_sysdict_Form_pg_constraint rippleconstraint = NULL;

    array->type = CATALOG_TYPE_CONSTRAINT;
    array->offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }
    foreach (cell, constraint_list)
    {
        rippleconstraint = (pg_sysdict_Form_pg_constraint)lfirst(cell);
        constraint_size = offsetof(pg_parser_sysdict_pgconstraint, conkey);
        constraint_size += rippleconstraint->conkeycnt * sizeof(int16_t);

        if (page_offset + constraint_size > FILE_BLK_SIZE)
        {
            rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
            {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = PAGE_HEADER_SIZE;
        }
        rmemcpy1(buffer,
                 page_offset,
                 rippleconstraint,
                 offsetof(pg_parser_sysdict_pgconstraint, conkey));
        page_offset += offsetof(pg_parser_sysdict_pgconstraint, conkey);

        if (rippleconstraint->conkeycnt > 0 && rippleconstraint->conkey != NULL)
        {
            rmemcpy1(buffer,
                     page_offset,
                     rippleconstraint->conkey,
                     rippleconstraint->conkeycnt * sizeof(int16_t));
            page_offset += (rippleconstraint->conkeycnt * sizeof(int16_t));
        }
    }
    if (page_offset > PAGE_HEADER_SIZE)
    {
        rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
        {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }
    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", CONSTRAINT_FILE);
    }
    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", CONSTRAINT_FILE);
    }

    array->len = page_num;
}

HTAB* constraintcache_load(sysdict_header_array* array)
{
    int                           r = 0;
    int                           fd = -1;
    HTAB*                         constrainthtab;
    HASHCTL                       hash_ctl;
    bool                          found = false;
    uint32                        datasize = 0;
    uint64                        offset = 0;
    uint64                        fileoffset = 0;

    char                          buffer[FILE_BLK_SIZE];
    pg_sysdict_Form_pg_constraint rippleconstraint;
    catalog_constraint_value*     entry = NULL;

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint32_t);
    hash_ctl.entrysize = sizeof(catalog_constraint_value);
    constrainthtab =
        hash_create("catalog_constraint_value", 2048, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    if (array[CATALOG_TYPE_CONSTRAINT - 1].len == array[CATALOG_TYPE_CONSTRAINT - 1].offset)
    {
        return constrainthtab;
    }

    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }

    fileoffset = array[CATALOG_TYPE_CONSTRAINT - 1].offset;
    while ((r = osal_file_pread(fd, buffer, FILE_BLK_SIZE, fileoffset)) > 0)
    {
        rmemcpy1(&datasize, 0, buffer, PAGE_HEADER_SIZE);
        offset = PAGE_HEADER_SIZE;

        while (offset < datasize)
        {
            rippleconstraint =
                (pg_parser_sysdict_pgconstraint*)rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));

            if (rippleconstraint == NULL)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(rippleconstraint, 0, '\0', sizeof(pg_parser_sysdict_pgconstraint));

            rmemcpy0(rippleconstraint,
                     0,
                     buffer + offset,
                     offsetof(pg_parser_sysdict_pgconstraint, conkey));
            offset += offsetof(pg_parser_sysdict_pgconstraint, conkey);

            if (rippleconstraint->conkeycnt > 0)
            {
                rippleconstraint->conkey =
                    (int16_t*)rmalloc0(rippleconstraint->conkeycnt * sizeof(int16_t));
                if (rippleconstraint->conkey == NULL)
                {
                    rfree(rippleconstraint);
                    elog(RLOG_ERROR, "out of memory conkey");
                }
                rmemset0(rippleconstraint->conkey,
                         0,
                         '\0',
                         rippleconstraint->conkeycnt * sizeof(int16_t));
                rmemcpy0(rippleconstraint->conkey,
                         0,
                         buffer + offset,
                         rippleconstraint->conkeycnt * sizeof(int16_t));
                offset += rippleconstraint->conkeycnt * sizeof(int16_t);
            }
            else
            {
                rippleconstraint->conkey = NULL;
            }

            entry = (catalog_constraint_value*)hash_search(
                constrainthtab, &rippleconstraint->conrelid, HASH_ENTER, &found);
            entry->conrelid = rippleconstraint->conrelid;
            entry->constraint = rippleconstraint;

            if (fileoffset + offset == array[CATALOG_TYPE_CONSTRAINT - 1].len)
            {
                if (osal_file_close(fd))
                {
                    elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
                }
                return constrainthtab;
            }
        }
        fileoffset += FILE_BLK_SIZE;
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }
    return constrainthtab;
}

void constraintcache_write(HTAB* constraintcache, uint64* offset, sysdict_header_array* array)
{
    int                           fd;
    int                           page_num = 0;
    uint64                        constraint_size = 0;
    size_t                        page_offset = PAGE_HEADER_SIZE;
    HASH_SEQ_STATUS               status;
    char                          buffer[FILE_BLK_SIZE];
    catalog_constraint_value*     entry = NULL;
    pg_sysdict_Form_pg_constraint constraint = NULL;

    array[CATALOG_TYPE_CONSTRAINT - 1].type = CATALOG_TYPE_CONSTRAINT;
    array[CATALOG_TYPE_CONSTRAINT - 1].offset = *offset;
    page_num = *offset;

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_FILE);
    }

    hash_seq_init(&status, constraintcache);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        constraint = entry->constraint;
        constraint_size = offsetof(pg_parser_sysdict_pgconstraint, conkey);
        constraint_size += constraint->conkeycnt * sizeof(int16_t);

        if (page_offset + constraint_size > FILE_BLK_SIZE)
        {
            rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);
            if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
            {
                elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_FILE);
                osal_file_close(fd);
                return;
            }
            rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);
            page_num = *offset + page_offset;
            *offset += FILE_BLK_SIZE;
            page_offset = PAGE_HEADER_SIZE;
        }
        rmemcpy1(buffer, page_offset, constraint, offsetof(pg_parser_sysdict_pgconstraint, conkey));
        page_offset += offsetof(pg_parser_sysdict_pgconstraint, conkey);

        if (constraint->conkeycnt > 0 && constraint->conkey != NULL)
        {
            rmemcpy1(
                buffer, page_offset, constraint->conkey, constraint->conkeycnt * sizeof(int16_t));
            page_offset += constraint->conkeycnt * sizeof(int16_t);
        }
    }
    if (page_offset > PAGE_HEADER_SIZE)
    {
        rmemcpy1(buffer, 0, &page_offset, PAGE_HEADER_SIZE);
        if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, *offset) != FILE_BLK_SIZE)
        {
            elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
            osal_file_close(fd);
            return;
        }
        page_num = page_offset + *offset;
        *offset += FILE_BLK_SIZE;
    }
    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_TMP_FILE);
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_TMP_FILE);
    }

    array[CATALOG_TYPE_CONSTRAINT - 1].len = page_num;
}