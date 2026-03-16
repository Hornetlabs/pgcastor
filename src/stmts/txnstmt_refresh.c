#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "stmts/ripple_txnstmt.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"

void ripple_txnstmt_refreshfree(void* data)
{
    ripple_refresh_tables* refreshtables = NULL;

    if(NULL == data)
    {
        return;
    }

    refreshtables = (ripple_refresh_tables*)data;

    if(NULL != refreshtables)
    {
        ripple_refresh_freetables(refreshtables);
    }
    return;
}
