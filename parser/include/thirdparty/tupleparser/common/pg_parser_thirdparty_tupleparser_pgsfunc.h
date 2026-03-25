#ifndef PG_PARSER_THIRDPARTY_TUPLEPARSER_PGSFUNC_H
#define PG_PARSER_THIRDPARTY_TUPLEPARSER_PGSFUNC_H

typedef struct pg_parser_extraTypoutInfo
{
    uint8_t                                  valueinfo; /* out special marker */
    uint32_t                                 typrelid;  /* in OID required for some conversions */
    uint32_t                                 valuelen;  /* out value length */
    pg_parser_sysdicts*                      sysdicts;
    pg_parser_translog_convertinfo_with_zic* zicinfo;
} pg_parser_extraTypoutInfo;

/*
 * Define function pointers, provide function prototypes for special types
 */
typedef pg_parser_Datum (*pg_parser_Local_PGFunctionSpecial)(pg_parser_Datum            attr,
                                                             pg_parser_extraTypoutInfo* info);

typedef struct
{
    const char*                       funcName; /* C name of the function */
    pg_parser_Local_PGFunctionSpecial func;     /* pointer to compiled function */
} pg_parser_FmgrBuiltinSpecial;

/* Need dictionary */
extern pg_parser_Datum regprocout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

/* Need to identify if it's an extended type */
extern pg_parser_Datum textout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum pg_node_tree_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum json_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum cash_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum bpcharout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum varcharout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum timestamptz_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum timestamp_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum numeric_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum xml_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum byteaout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum enum_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum anyenum_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum tsvectorout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum bit_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum varbit_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum path_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum poly_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum array_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum int2vectorout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum record_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum jsonb_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum range_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum spheroid_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum box3d_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum box2d_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum box2df_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum gidx_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum raster_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum geometry_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

extern pg_parser_Datum geography_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info);

#endif
