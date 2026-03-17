#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/common/pg_parser_thirdparty_pgconvert.h"

typedef struct PG_PARSER_OID2TYPE_CONVERT
{
    uint32_t oid;
    uint8_t  type;
}pg_parser_oid2type_convert;

/* index type */
/* index在pg中的oid定义 */
#define PG_PARSER_INDEX_HEAP_TYPID                   (uint32_t) 2
#define PG_PARSER_INDEX_BTREE_TYPID                  (uint32_t) 403
#define PG_PARSER_INDEX_HASH_TYPID                   (uint32_t) 405
#define PG_PARSER_INDEX_GIST_TYPID                   (uint32_t) 783
#define PG_PARSER_INDEX_GIN_TYPID                    (uint32_t) 2742
#define PG_PARSER_INDEX_SPGIST_TYPID                 (uint32_t) 4000
#define PG_PARSER_INDEX_BRIN_TYPID                   (uint32_t) 3580

/* index在解析程序内部的定义值 */
#define PG_PARSER_DDLSTMT_INDEXTYPE_HEAP             (uint8_t) 0x00
#define PG_PARSER_DDLSTMT_INDEXTYPE_BTREE            (uint8_t) 0x01
#define PG_PARSER_DDLSTMT_INDEXTYPE_HASH             (uint8_t) 0x02
#define PG_PARSER_DDLSTMT_INDEXTYPE_GIST             (uint8_t) 0x03
#define PG_PARSER_DDLSTMT_INDEXTYPE_GIN              (uint8_t) 0x04
#define PG_PARSER_DDLSTMT_INDEXTYPE_SPGIST           (uint8_t) 0x05
#define PG_PARSER_DDLSTMT_INDEXTYPE_BRIN             (uint8_t) 0x06

#define PG_PARSER_INDEX_TYPE_CONVERT_MAXNUM 7

const pg_parser_oid2type_convert pg_parser_index_type_convert_box[] = 
{
    {PG_PARSER_INDEX_HEAP_TYPID  , PG_PARSER_DDLSTMT_INDEXTYPE_HEAP  },
    {PG_PARSER_INDEX_BTREE_TYPID , PG_PARSER_DDLSTMT_INDEXTYPE_BTREE },
    {PG_PARSER_INDEX_HASH_TYPID  , PG_PARSER_DDLSTMT_INDEXTYPE_HASH  },
    {PG_PARSER_INDEX_GIST_TYPID  , PG_PARSER_DDLSTMT_INDEXTYPE_GIST  },
    {PG_PARSER_INDEX_GIN_TYPID   , PG_PARSER_DDLSTMT_INDEXTYPE_GIN   },
    {PG_PARSER_INDEX_SPGIST_TYPID, PG_PARSER_DDLSTMT_INDEXTYPE_SPGIST},
    {PG_PARSER_INDEX_BRIN_TYPID  , PG_PARSER_DDLSTMT_INDEXTYPE_BRIN  }
};


/* sequence type */
#define PG_PARSER_TYPE_INT2_OID              (uint32_t) 21
#define PG_PARSER_TYPE_INT4_OID              (uint32_t) 23
#define PG_PARSER_TYPE_INT5_OID              (uint32_t) 20

#define PG_PARSER_DDLSTMT_SEQTYPE_INT2       (uint32_t) 0
#define PG_PARSER_DDLSTMT_SEQTYPE_INT4       (uint32_t) 1
#define PG_PARSER_DDLSTMT_SEQTYPE_INT8       (uint32_t) 2

#define PG_PARSER_SEQ_TYPE_CONVERT_MAXNUM 3

const pg_parser_oid2type_convert pg_parser_seqtype_convert_box[] = 
{
    {PG_PARSER_TYPE_INT2_OID ,PG_PARSER_DDLSTMT_SEQTYPE_INT2},
    {PG_PARSER_TYPE_INT4_OID ,PG_PARSER_DDLSTMT_SEQTYPE_INT4},
    {PG_PARSER_TYPE_INT5_OID ,PG_PARSER_DDLSTMT_SEQTYPE_INT8}
};


uint32_t pg_convert_typid2type(uint32_t oid, pg_convert_enum type)
{
    int32_t i = 0;
    if (indextype_convert == type)
    {
        for (i = 0; i < PG_PARSER_INDEX_TYPE_CONVERT_MAXNUM; i++)
        {
            if (pg_parser_index_type_convert_box[i].oid == oid)
                return (uint32_t) pg_parser_index_type_convert_box[i].type;
            return oid;
        }
    }
    else if (seqtype_convert == type)
    {
        for (i = 0; i < PG_PARSER_SEQ_TYPE_CONVERT_MAXNUM; i++)
        {
            if (pg_parser_seqtype_convert_box[i].oid == oid)
                return (uint32_t) pg_parser_seqtype_convert_box[i].type;
            else
                return oid;
        }
    }
    return oid;

}
