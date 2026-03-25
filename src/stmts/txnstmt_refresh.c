#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "stmts/txnstmt.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"

void txnstmt_refreshfree(void* data)
{
    refresh_tables* refreshtables = NULL;

    if (NULL == data)
    {
        return;
    }

    refreshtables = (refresh_tables*)data;

    if (NULL != refreshtables)
    {
        refresh_freetables(refreshtables);
    }
    return;
}
