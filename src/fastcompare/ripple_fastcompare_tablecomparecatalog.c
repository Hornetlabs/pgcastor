#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"
#include "fastcompare/ripple_fastcompare_tablecomparecatalog.h"

/* 设置constraint中conkey */
static void ripple_fastcompare_tablecomparecatalog_setconkey(char *input, HTAB* attribute, ripple_fastcompare_tablecomparecatalog_pgconstraint* constraint)
{
    bool find = false;
    int16 conkeycnt = 0;
    size_t len = 0;
    char *uptr = NULL;
    ListCell* lc = NULL;
    ripple_fastcompare_columndefine* columdefine = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute* attr = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute_value* entry = NULL;

    if (NULL == input)
    {
        return;
    }

    len = strlen(input);
    
    if (len < 2 || input[0] != '{' || input[len - 1] != '}')
    {
        elog(RLOG_WARNING, "Invalid conkey");
        return;
    }

    entry = (ripple_fastcompare_tablecomparecatalog_pgattribute_value*)hash_search(attribute, &constraint->conrelid, HASH_FIND, &find);
    if(NULL == entry || NIL == entry->attrs)
    {
        elog(RLOG_WARNING, "Invalid constraint->conrelid not find in attribute");
        return;
    }

    uptr = input;
    /* 跳过开头'{' */
    uptr++;
    
    while (*uptr && *uptr != '}')
    {
        columdefine = (ripple_fastcompare_columndefine*)rmalloc0(sizeof(ripple_fastcompare_columndefine));
        if(NULL == columdefine)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(columdefine, 0, '\0', sizeof(ripple_fastcompare_columndefine));

        sscanf(uptr, "%u", &columdefine->colid);
        conkeycnt ++;
        foreach(lc, entry->attrs)
        {
            attr = (ripple_fastcompare_tablecomparecatalog_pgattribute*)lfirst(lc);
            if (columdefine->colid == attr->attnum)
            {
                columdefine->colname = rstrdup(attr->attname);
                lc = NULL;
                constraint->conkey = lappend(constraint->conkey, (void*)columdefine);
                uptr++;
                break;
            }
        }
        if (*uptr == ',')
        {
            uptr++;
        }

    }

    constraint->conkeycnt = conkeycnt;

    return;
}

static void ripple_fastcompare_tablecomparecatalog_freeconkey(List* conkey)
{ 
    ListCell* lc = NULL;
    ripple_fastcompare_columndefine* columdefine = NULL;
    foreach(lc, conkey)
    {
        columdefine = (ripple_fastcompare_columndefine*)lfirst(lc);

        elog(RLOG_DEBUG,"columdefine:id:%u, colname :%s ",columdefine->colid, columdefine->colname);

        rfree(columdefine->colname);
        rfree(columdefine);
    }
}

/* 生成table2oid*/
static void ripple_fastcompare_tablecomparecatalog_table2oid_set(ripple_fastcompare_tablecomparecatalog_pgclass* class, HTAB* table2oid)
{
    bool found = false;
    ripple_fastcompare_tablecomparecatalog_table2oid_key key;
    ripple_fastcompare_tablecomparecatalog_table2oid_value* entry;

    rmemset1(key.tablename, 0, '\0', NAMEDATALEN);
    rmemset1(key.schemaname, 0, '\0', NAMEDATALEN);

    rmemcpy1(key.tablename, 0, class->relname, NAMEDATALEN);
    rmemcpy1(key.schemaname, 0, class->nspname, NAMEDATALEN);

    entry = (ripple_fastcompare_tablecomparecatalog_table2oid_value*)hash_search(table2oid, &key, HASH_ENTER, &found);
    if (found)
    {
        elog(RLOG_ERROR, "table:%s.%s already exist in table2oid", class->nspname, class->relname);
    }

    rmemcpy1(entry->key.tablename, 0, class->relname, NAMEDATALEN);
    rmemcpy1(entry->key.schemaname, 0, class->nspname, NAMEDATALEN);
    entry->oid = class->oid;

    return;
}

/* 从数据库获取constraint表信息 */
static void ripple_fastcompare_tablecomparecatalog_constraint_getfromdb(PGconn *conn, ripple_fastcompare_tablecomparecatalog* sysdicts)
{
    int i, j;
    Oid oid = InvalidOid;
    HASHCTL hash_ctl;
    bool found = false;
    PGresult *res = NULL;
    ripple_fastcompare_tablecomparecatalog_pgconstraint* entry = NULL;

    const char *query = "SELECT conrelid, conkey FROM pg_constraint WHERE contype = 'p';";

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ripple_fastcompare_tablecomparecatalog_pgconstraint);
    sysdicts->pg_constraint = hash_create("ripple_catalog_sysdict_constraint", 1024, &hash_ctl,
                                            HASH_ELEM | HASH_BLOBS);

    res = ripple_conn_exec(conn, query);
    if (NULL == res)
    {
        conn = NULL;
        elog(RLOG_ERROR, "pg_constraint query failed");
    }

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++)
    {
        j=0;
        oid = strtoul(PQgetvalue(res, i, j++), NULL, 10);

        entry = (ripple_fastcompare_tablecomparecatalog_pgconstraint*)hash_search(sysdicts->pg_constraint, &oid, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR, "class:%u already exist in pg_constraint", entry->conrelid);
        }
        entry->conrelid = oid;
        entry->conkey = NULL;
        entry->conkeycnt = 0;
        ripple_fastcompare_tablecomparecatalog_setconkey(PQgetvalue(res, i, j++), sysdicts->pg_attribute, entry);
    }

    PQclear(res);

    return;
}

/* 从数据库获取class、attribute表信息 */
static void ripple_fastcompare_tablecomparecatalog_class_attribute_getfromdb(PGconn *conn, ripple_fastcompare_tablecomparecatalog* sysdicts)
{
    int i, j;
    bool found = false;
    PGresult    *res = NULL;
    ListCell*   cell = NULL;
    List*   classlist = NIL;
    ripple_fastcompare_tablecomparecatalog_pgclass* class = NULL;
    ripple_fastcompare_tablecomparecatalog_pgclass* classtoast = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute* attribute = NULL;
    ripple_fastcompare_tablecomparecatalog_pgclass* class_entry = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute_value* attr_entry = NULL;
    char sql_exec[RIPPLE_MAX_EXEC_SQL_LEN] = {'\0'};

    HASHCTL class_hash_ctl;
    HASHCTL attr_hash_ctl;
    HASHCTL table2oid_ctl;

    rmemset1(&class_hash_ctl, 0, '\0', sizeof(class_hash_ctl));
    class_hash_ctl.keysize = sizeof(Oid);
    class_hash_ctl.entrysize = sizeof(ripple_fastcompare_tablecomparecatalog_pgclass);
    sysdicts->pg_class = hash_create("ripple_fastcompare_tablecomparecatalog_class", 2048, &class_hash_ctl,
                        HASH_ELEM | HASH_BLOBS);

    rmemset1(&attr_hash_ctl, 0, '\0', sizeof(attr_hash_ctl));
    attr_hash_ctl.keysize = sizeof(Oid);
    attr_hash_ctl.entrysize = sizeof(ripple_fastcompare_tablecomparecatalog_pgattribute);
    sysdicts->pg_attribute = hash_create("ripple_fastcompare_tablecomparecatalog_attribute", 2048, &attr_hash_ctl,
                        HASH_ELEM | HASH_BLOBS);

    rmemset1(&table2oid_ctl, 0, '\0', sizeof(table2oid_ctl));
    table2oid_ctl.keysize = sizeof(ripple_fastcompare_tablecomparecatalog_table2oid_key);
    table2oid_ctl.entrysize = sizeof(ripple_fastcompare_tablecomparecatalog_table2oid_value);
    sysdicts->table2oid = hash_create("ripple_fastcompare_tablecomparecatalog_table2oid", 2048, &table2oid_ctl,
                        HASH_ELEM | HASH_BLOBS);

    sprintf(sql_exec, "SELECT rel.oid, \n"
                            "rel.relname, \n"
                            "nsp.nspname \n"
                            "FROM pg_class rel \n"
                            "JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid \n"
                            "WHERE rel.relkind not in ('v', 'i', 'c', 'I') \n"
                            "AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') \n"
                            "AND EXISTS (SELECT 1 \n"
                            "FROM pg_constraint con \n"
                            "WHERE con.contype = 'p' \n"
                            "AND con.conrelid = rel.oid);");

    res = ripple_conn_exec(conn, sql_exec);
    if (NULL == res)
    {
        conn = NULL;
        elog(RLOG_ERROR, "pg_class query failed");
    }

    // 打印行数据
    for (i = 0; i < PQntuples(res); i++)
    {
        class = (ripple_fastcompare_tablecomparecatalog_pgclass*)rmalloc0(sizeof(ripple_fastcompare_tablecomparecatalog_pgclass));
        if(NULL == class)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(class, 0, '\0', sizeof(ripple_fastcompare_tablecomparecatalog_pgclass));
        j=0;
        sscanf(PQgetvalue(res, i, j++), "%u", &class->oid);
        strcpy(class->relname ,PQgetvalue(res, i, j++));
        strcpy(class->nspname ,PQgetvalue(res, i, j++));

        classlist = lappend(classlist, class);
    }
    PQclear(res);

    foreach(cell, classlist)
    {
        classtoast = (ripple_fastcompare_tablecomparecatalog_pgclass*) lfirst(cell);

        class_entry = hash_search(sysdicts->pg_class, &classtoast->oid, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "class_oid:%u already exist in pg_class", class_entry->oid);
        }
        class_entry->oid = classtoast->oid;
        rmemcpy1(class_entry->relname, 0, classtoast->relname, NAMEDATALEN);
        rmemcpy1(class_entry->nspname, 0, classtoast->nspname, NAMEDATALEN);

        ripple_fastcompare_tablecomparecatalog_table2oid_set(classtoast, sysdicts->table2oid);

        rmemset1(sql_exec, 0, '\0', RIPPLE_MAX_EXEC_SQL_LEN);
        sprintf(sql_exec, "SELECT rel.attrelid, \n"
                                    "rel.attname, \n"
                                    "rel.atttypid, \n"
                                    "rel.attnum \n"
                                    "FROM pg_attribute rel \n"
                                    "where rel.attrelid = '%u' and rel.attnum > 0 and rel.attisdropped = false;",classtoast->oid );
        res = ripple_conn_exec(conn, sql_exec);
        if (NULL == res)
        {
            conn = NULL;
            elog(RLOG_ERROR, "pg_attribute query failed");
        }

        for (i = 0; i < PQntuples(res); i++) 
        {
            attribute = (ripple_fastcompare_tablecomparecatalog_pgattribute*)rmalloc0(sizeof(ripple_fastcompare_tablecomparecatalog_pgattribute));
            if(NULL == attribute)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(attribute, 0, '\0', sizeof(ripple_fastcompare_tablecomparecatalog_pgattribute));
            j=0;

            sscanf(PQgetvalue(res, i, j++), "%u", &attribute->attrelid);
            strcpy(attribute->attname, PQgetvalue(res, i, j++));
            sscanf(PQgetvalue(res, i, j++), "%u", &attribute->atttypid);
            sscanf(PQgetvalue(res, i, j++), "%hd", &attribute->attnum);

            attr_entry = (ripple_fastcompare_tablecomparecatalog_pgattribute_value *)hash_search(sysdicts->pg_attribute, &attribute->attrelid, HASH_ENTER, &found);
            if (!found)
            {
                attr_entry->attrs = NIL;
            }
            attr_entry->attrelid = attribute->attrelid;
            attr_entry->attrs = lappend(attr_entry->attrs, attribute);
        }
        PQclear(res);
    }
    list_free_deep(classlist);

    return;
}

/* 加载数据字典 */
ripple_fastcompare_tablecomparecatalog* ripple_fastcompare_tablecomparecatalog_init(void)
{
    PGconn* conn = NULL;
    const char*    url = NULL;
    ripple_fastcompare_tablecomparecatalog* sysdicts = NULL;

    /*获取连接信息*/
    url = guc_getConfigOption("url");

    /*连接数据库*/
    conn = ripple_conn_get(url);

    /* 连接错误退出 */
    if(NULL == conn)
    {
        return sysdicts;
    }

    sysdicts = (ripple_fastcompare_tablecomparecatalog*)rmalloc0(sizeof(ripple_fastcompare_tablecomparecatalog));
    if(NULL == sysdicts)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(sysdicts, 0, '\0', sizeof(ripple_fastcompare_tablecomparecatalog));

    ripple_fastcompare_tablecomparecatalog_class_attribute_getfromdb(conn, sysdicts);
    ripple_fastcompare_tablecomparecatalog_constraint_getfromdb(conn, sysdicts);

    PQfinish(conn);
    conn = 0;

    elog(RLOG_DEBUG, " get tablecomparecatalog ");

    return sysdicts;
}

/* 根据oid获取表名、模式名 */
bool ripple_fastcompare_tablecomparecatalog_gettablebyoid(HTAB* class, Oid oid, char* table, char* schema)
{
    bool find = false;
    ripple_fastcompare_tablecomparecatalog_pgclass* entry;

    entry = (ripple_fastcompare_tablecomparecatalog_pgclass*)hash_search(class, &oid, HASH_FIND, &find);
    if (true == find)
    {
        rmemcpy1(table, 0, entry->relname, NAMEDATALEN);
        rmemcpy1(schema, 0, entry->nspname, NAMEDATALEN);
    }
    return find;
}

/* 根据表名 模式名获取表oid */
Oid ripple_fastcompare_tablecomparecatalog_getoidbytable(HTAB* table2oid, char* table, char* schema)
{
    bool find = false;
    Oid oid = InvalidXLogRecPtr;
    ripple_fastcompare_tablecomparecatalog_table2oid_key key;
    ripple_fastcompare_tablecomparecatalog_table2oid_value* entry;

    rmemset1(&key, 0, '\0', sizeof(ripple_fastcompare_tablecomparecatalog_table2oid_key));
    rmemcpy1(key.tablename, 0, table, strlen(table));
    rmemcpy1(key.schemaname, 0, schema, strlen(schema));

    entry = (ripple_fastcompare_tablecomparecatalog_table2oid_value*)hash_search(table2oid, &key, HASH_FIND, &find);
    if (true == find)
    {
        oid = entry->oid;
    }

    return oid;
}

/* 根据oid获取表列信息 list *columnvalue, 按attribute中顺序排列 */
List* ripple_fastcompare_tablecomparecatalog_getcoldefinebyoid(HTAB* attrhash, Oid oid)
{
    bool find = false;
    ListCell* lc = NULL;
    List* col_list = NULL;
    ripple_fastcompare_columndefine* columdefine = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute* attribute = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute_value* attr_entry = NULL;

    attr_entry = (ripple_fastcompare_tablecomparecatalog_pgattribute_value*)hash_search(attrhash, &oid, HASH_FIND, &find);
    if (false == find)
    {
        return NULL;
    }

    foreach(lc, attr_entry->attrs)
    {
        attribute = (ripple_fastcompare_tablecomparecatalog_pgattribute*)lfirst(lc);
        columdefine = (ripple_fastcompare_columndefine*)rmalloc0(sizeof(ripple_fastcompare_columndefine));
        if(NULL == columdefine)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(columdefine, 0, '\0', sizeof(ripple_fastcompare_columndefine));

        columdefine->colid = attribute->attnum;
        columdefine->colname = rstrdup(attribute->attname);

        col_list = lappend(col_list, columdefine);
    }

    return col_list;
}

/* 根据oid获取表主键信息 list *columnvalue，按照主键排序规则排列 */
List* ripple_fastcompare_tablecomparecatalog_getpkcoldefinebyoid(HTAB* conhash, Oid oid)
{
    bool find = false;
    ListCell* lc = NULL;
    List* col_list = NULL;
    ripple_fastcompare_columndefine* conkeycol = NULL;
    ripple_fastcompare_columndefine* columdefine = NULL;
    ripple_fastcompare_tablecomparecatalog_pgconstraint* constraint = NULL;

    constraint = (ripple_fastcompare_tablecomparecatalog_pgconstraint*)hash_search(conhash, &oid, HASH_FIND, &find);
    if (false == find)
    {
        return NULL;
    }

    foreach(lc, constraint->conkey)
    {
        conkeycol = (ripple_fastcompare_columndefine*)lfirst(lc);
        columdefine = (ripple_fastcompare_columndefine*)rmalloc0(sizeof(ripple_fastcompare_columndefine));
        if(NULL == columdefine)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(columdefine, 0, '\0', sizeof(ripple_fastcompare_columndefine));

        columdefine->colid = conkeycol->colid;
        columdefine->colname = rstrdup(conkeycol->colname);

        col_list = lappend(col_list, columdefine);
    }

    return col_list;
}

/* 资源释放 */
void ripple_fastcompare_tablecomparecatalog_destroy(ripple_fastcompare_tablecomparecatalog* sysdicts)
{
    HASH_SEQ_STATUS status;
    ripple_fastcompare_tablecomparecatalog_pgconstraint* con_entry = NULL;
    ripple_fastcompare_tablecomparecatalog_pgattribute_value* attr_entry = NULL;

    if (NULL == sysdicts)
    {
        return;
    }

    if (sysdicts->pg_class)
    {
        hash_destroy(sysdicts->pg_class);
    }

    if (sysdicts->pg_attribute)
    {
        rmemset1(&status, 0, '\0', sizeof(status));
        hash_seq_init(&status,sysdicts->pg_attribute);
        while ((attr_entry = hash_seq_search(&status)) != NULL)
        {
            list_free_deep(attr_entry->attrs);
        }
        hash_destroy(sysdicts->pg_attribute);
    }
    
    if (sysdicts->pg_constraint)
    {
        rmemset1(&status, 0, '\0', sizeof(status));
        hash_seq_init(&status,sysdicts->pg_constraint);
        while ((con_entry = hash_seq_search(&status)) != NULL)
        {
            elog(RLOG_DEBUG,"crelid :%u ",con_entry->conrelid);
            ripple_fastcompare_tablecomparecatalog_freeconkey(con_entry->conkey);
            list_free(con_entry->conkey);
        }
        hash_destroy(sysdicts->pg_constraint);
    }

    if (sysdicts->table2oid)
    {
        hash_destroy(sysdicts->table2oid);
    }
    
    rfree(sysdicts);
    
}
