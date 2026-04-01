#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "common/pg_parser_errnodef.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_xact.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_heap.h"

typedef void (*decode_prefunc_onlinerefresh)(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase);

typedef struct DECODE_PREMGR_ONLINEREFRESH
{
    int                          type;
    char*                        name;
    decode_prefunc_onlinerefresh func;
} decode_premgr_onlinerefresh;

static decode_premgr_onlinerefresh m_decodepremgr_onlinerefresh[] = {
    {PG_PARSER_TRANSLOG_INVALID,             "INVALID",        NULL                            },
    {PG_PARSER_TRANSLOG_HEAP_INSERT,         "INSERT",         onlinerefresh_decode_heap       },
    {PG_PARSER_TRANSLOG_HEAP_UPDATE,         "UPDATE",         onlinerefresh_decode_heap       },
    {PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,     "HOT UPDATE",     onlinerefresh_decode_heap       },
    {PG_PARSER_TRANSLOG_HEAP_DELETE,         "DELETE",         onlinerefresh_decode_heap       },
    {PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,  "MULTI INSERT",   onlinerefresh_decode_heap       },
    {PG_PARSER_TRANSLOG_XACT_COMMIT,         "COMMIT",         onlinerefresh_decode_xact_commit},
    {PG_PARSER_TRANSLOG_XACT_ABORT,          "ABORT",          onlinerefresh_decode_xact_abort },
    {PG_PARSER_TRANSLOG_XLOG_SWITCH,         "SWITCH",         NULL                            },
    {PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,     "ONLINE",         decode_chkpt                    },
    {PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,   "SHUTDOWN",       decode_chkpt                    },
    {PG_PARSER_TRANSLOG_FPW_TUPLE,           "FPW_TUPLE",      heap_fpw_tuples                 },
    {PG_PARSER_TRANSLOG_RELMAP,              "RELMAP",         decode_relmap                   },
    {PG_PARSER_TRANSLOG_RUNNING_XACTS,       "RUNNING_XACTS",  NULL                            },
    {PG_PARSER_TRANSLOG_XLOG_RECOVERY,       "RECOVERY",       NULL                            },
    {PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE, "COMMIT_PREPARE", NULL                            },
    {PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,  "ABORT_PREPARE",  NULL                            },
    {PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,     "ASSIGNMENT",     NULL                            },
    {PG_PARSER_TRANSLOG_XACT_PREPARE,        "PREPARE",        NULL                            },
    {PG_PARSER_TRANSLOG_HEAP_TRUNCATE,       "TRUNCATE",       NULL                            },
    {PG_PARSER_TRANSLOG_SEQ,                 "SEQUENCE",       NULL                            }
};

static int m_precnt_onlinerefresh = (sizeof(m_decodepremgr_onlinerefresh)) / (sizeof(decode_premgr_onlinerefresh));
static XLogRecPtr m_parserlsn = 0;

void parserwork_waldecode_onlinerefresh(decodingcontext* decodingctx)
{
    int32                        castorerrno = 0;
    pg_parser_translog_pre_base* preparserresutl = NULL;
    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    m_parserlsn = decodingctx->decode_record->start.wal.lsn;
    /* Call pre-parser, dispatch processing based on pre-parser content */
    if (false == pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &castorerrno))
    {
        elog(RLOG_ERROR,
             "pg_parser_trans_preTrans error, %08X, %s",
             castorerrno,
             pg_parser_errno_getErrInfo(castorerrno));
    }

    /* Call dispatch function */
    if (m_precnt_onlinerefresh <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
    }

    if (NULL == m_decodepremgr_onlinerefresh[preparserresutl->m_type].func)
    {
        /* Check if transaction number is valid, if invalid, manually add a transaction number and
         * put it in cache for processing */
        pg_parser_trans_preTrans_free(preparserresutl);
        return;
    }

    m_decodepremgr_onlinerefresh[preparserresutl->m_type].func(decodingctx, preparserresutl);
    pg_parser_trans_preTrans_free(preparserresutl);
}
