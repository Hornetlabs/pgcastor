#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/common/xk_pg_parser_thirdparty_pgconvert.h"

typedef struct XK_PG_PARSER_OID2TYPE_CONVERT
{
    uint32_t oid;
    uint8_t  type;
}xk_pg_parser_oid2type_convert;

/* index type */
/* index在pg中的oid定义 */
#define XK_PG_PARSER_INDEX_HEAP_TYPID                   (uint32_t) 2
#define XK_PG_PARSER_INDEX_BTREE_TYPID                  (uint32_t) 403
#define XK_PG_PARSER_INDEX_HASH_TYPID                   (uint32_t) 405
#define XK_PG_PARSER_INDEX_GIST_TYPID                   (uint32_t) 783
#define XK_PG_PARSER_INDEX_GIN_TYPID                    (uint32_t) 2742
#define XK_PG_PARSER_INDEX_SPGIST_TYPID                 (uint32_t) 4000
#define XK_PG_PARSER_INDEX_BRIN_TYPID                   (uint32_t) 3580

/* index在解析程序内部的定义值 */
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_HEAP             (uint8_t) 0x00
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_BTREE            (uint8_t) 0x01
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_HASH             (uint8_t) 0x02
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_GIST             (uint8_t) 0x03
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_GIN              (uint8_t) 0x04
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_SPGIST           (uint8_t) 0x05
#define XK_PG_PARSER_DDLSTMT_INDEXTYPE_BRIN             (uint8_t) 0x06

#define XK_PG_PARSER_INDEX_TYPE_CONVERT_MAXNUM 7

const xk_pg_parser_oid2type_convert xk_pg_parser_index_type_convert_box[] = 
{
    {XK_PG_PARSER_INDEX_HEAP_TYPID  , XK_PG_PARSER_DDLSTMT_INDEXTYPE_HEAP  },
    {XK_PG_PARSER_INDEX_BTREE_TYPID , XK_PG_PARSER_DDLSTMT_INDEXTYPE_BTREE },
    {XK_PG_PARSER_INDEX_HASH_TYPID  , XK_PG_PARSER_DDLSTMT_INDEXTYPE_HASH  },
    {XK_PG_PARSER_INDEX_GIST_TYPID  , XK_PG_PARSER_DDLSTMT_INDEXTYPE_GIST  },
    {XK_PG_PARSER_INDEX_GIN_TYPID   , XK_PG_PARSER_DDLSTMT_INDEXTYPE_GIN   },
    {XK_PG_PARSER_INDEX_SPGIST_TYPID, XK_PG_PARSER_DDLSTMT_INDEXTYPE_SPGIST},
    {XK_PG_PARSER_INDEX_BRIN_TYPID  , XK_PG_PARSER_DDLSTMT_INDEXTYPE_BRIN  }
};


/* sequence type */
#define XK_PG_PARSER_TYPE_INT2_OID              (uint32_t) 21
#define XK_PG_PARSER_TYPE_INT4_OID              (uint32_t) 23
#define XK_PG_PARSER_TYPE_INT5_OID              (uint32_t) 20

#define XK_PG_PARSER_DDLSTMT_SEQTYPE_INT2       (uint32_t) 0
#define XK_PG_PARSER_DDLSTMT_SEQTYPE_INT4       (uint32_t) 1
#define XK_PG_PARSER_DDLSTMT_SEQTYPE_INT8       (uint32_t) 2

#define XK_PG_PARSER_SEQ_TYPE_CONVERT_MAXNUM 3

const xk_pg_parser_oid2type_convert xk_pg_parser_seqtype_convert_box[] = 
{
    {XK_PG_PARSER_TYPE_INT2_OID ,XK_PG_PARSER_DDLSTMT_SEQTYPE_INT2},
    {XK_PG_PARSER_TYPE_INT4_OID ,XK_PG_PARSER_DDLSTMT_SEQTYPE_INT4},
    {XK_PG_PARSER_TYPE_INT5_OID ,XK_PG_PARSER_DDLSTMT_SEQTYPE_INT8}
};


uint32_t xk_pg_convert_typid2type(uint32_t oid, xk_pg_convert_enum type)
{
    int32_t i = 0;
    if (indextype_convert == type)
    {
        for (i = 0; i < XK_PG_PARSER_INDEX_TYPE_CONVERT_MAXNUM; i++)
        {
            if (xk_pg_parser_index_type_convert_box[i].oid == oid)
                return (uint32_t) xk_pg_parser_index_type_convert_box[i].type;
            return oid;
        }
    }
    else if (seqtype_convert == type)
    {
        for (i = 0; i < XK_PG_PARSER_SEQ_TYPE_CONVERT_MAXNUM; i++)
        {
            if (xk_pg_parser_seqtype_convert_box[i].oid == oid)
                return (uint32_t) xk_pg_parser_seqtype_convert_box[i].type;
            else
                return oid;
        }
    }
    return oid;

}
