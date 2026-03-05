#include "ripple_app_incl.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "refresh/ripple_refresh_tables.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"

HTAB* ripple_onlinerefresh_integratefilterdataset_init(void)
{
    HTAB* integratefilterdataset = NULL;
    HASHCTL hctl = {'\0'};

    /* pg_class初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_onlinerefresh_integratefilterdataset);
    integratefilterdataset = hash_create("integratefilterdataset",
                                        128,
                                        &hctl,
                                        HASH_ELEM | HASH_BLOBS);

    return integratefilterdataset;
    
}

bool ripple_onlinerefresh_integratefilterdataset_add(HTAB* filterdataset, void* in_tables, FullTransactionId txid)
{
    bool find = false;
    ripple_refresh_tables* tables = NULL;
    ripple_refresh_table* current_table = NULL;
    ripple_onlinerefresh_integratefilterdataset* entry = NULL;

    tables = ( ripple_refresh_tables*)(in_tables);

    /* refresh已完成 */
    if (NULL == tables)
    {
        return true;
    }

    current_table = tables->tables;

    while (current_table)
    {
        entry = hash_search(filterdataset, &current_table->oid, HASH_ENTER, &find);
        if(!find)
        {
            entry->oid = current_table->oid;
            rmemset1(entry->schema, 0, '\0', NAMEDATALEN);
            rmemset1(entry->table, 0, '\0', NAMEDATALEN);
            rmemcpy1(entry->schema, 0, current_table->schema, strlen(current_table->schema));
            rmemcpy1(entry->table, 0, current_table->table, strlen(current_table->table));
        }
        entry->txid = txid;
        current_table = current_table->next;
    }
    return true;
}

HTAB* ripple_onlinerefresh_integratefilterdataset_copy(HTAB* filterdataset)
{
    bool find = false;
    HASH_SEQ_STATUS status;
    HTAB* result = NULL;
    ripple_onlinerefresh_integratefilterdataset* entry = NULL;
    ripple_onlinerefresh_integratefilterdataset* new_entry = NULL;

    result = ripple_onlinerefresh_integratefilterdataset_init();

    hash_seq_init(&status, filterdataset);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        new_entry = hash_search(result, &entry->oid, HASH_ENTER, &find);
        if(!find)
        {
            new_entry->oid = entry->oid;
            rmemset1(new_entry->schema, 0, '\0', NAMEDATALEN);
            rmemset1(new_entry->table, 0, '\0', NAMEDATALEN);
            rmemcpy1(new_entry->schema, 0, entry->schema, strlen(entry->schema));
            rmemcpy1(new_entry->table, 0, entry->table, strlen(entry->table));
        }
        new_entry->txid = entry->txid;
    }
    return result;
}

bool ripple_onlinerefresh_integratefilterdataset_delete(HTAB* filterdataset, void* in_tables, FullTransactionId txid)
{
    bool find = false;
    ripple_refresh_tables* tables = NULL;
    ripple_refresh_table* current_table = NULL;
    ripple_onlinerefresh_integratefilterdataset* entry = NULL;

    tables = (ripple_refresh_tables*)(in_tables);

    current_table = tables->tables;

    while (current_table)
    {
        entry = hash_search(filterdataset, &current_table->oid, HASH_FIND, &find);
        if (find)
        {
            if (entry->txid == txid)
            {
                hash_search(filterdataset, &current_table->oid, HASH_REMOVE, NULL);
            }
        }
        current_table = current_table->next;
    }

    return true;
}
