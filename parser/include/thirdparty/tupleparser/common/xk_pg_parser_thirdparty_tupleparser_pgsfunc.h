#ifndef XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_PGSFUNC_H
#define XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_PGSFUNC_H

typedef struct xk_pg_parser_extraTypoutInfo
{
    uint8_t  valueinfo; /* out 特殊标记 */
    uint32_t typrelid;  /* in 某些转换需要的oid */
    uint32_t valuelen;  /* out value值的长度 */
    xk_pg_parser_sysdicts *sysdicts;
    xk_pg_parser_translog_convertinfo_with_zic *zicinfo;
}xk_pg_parser_extraTypoutInfo;

/*
 * 定义函数指针, 为特殊类型提供函数原型
 */
typedef xk_pg_parser_Datum (*xk_pg_parser_Local_PGFunctionSpecial) (xk_pg_parser_Datum attr,
                                                                     xk_pg_parser_extraTypoutInfo *info);


typedef struct
{
    const char                            *funcName;           /* C name of the function */
    xk_pg_parser_Local_PGFunctionSpecial   func;               /* pointer to compiled function */
} xk_pg_parser_FmgrBuiltinSpecial;

/* 需要字典 */
extern xk_pg_parser_Datum regprocout(xk_pg_parser_Datum attr,
                                     xk_pg_parser_extraTypoutInfo *info);

/* 需要标识是否为扩展类型 */
extern xk_pg_parser_Datum textout(xk_pg_parser_Datum attr,
                                  xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum pg_node_tree_out(xk_pg_parser_Datum attr,
                                  xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum json_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum cash_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum bpcharout(xk_pg_parser_Datum attr,
                                    xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum varcharout(xk_pg_parser_Datum attr,
                                     xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum timestamptz_out(xk_pg_parser_Datum attr,
                                          xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum oratimestampltz_out(xk_pg_parser_Datum attr,
                                              xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum timestamp_out(xk_pg_parser_Datum attr,
                                        xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum oradate_out(xk_pg_parser_Datum attr,
                                        xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum numeric_out(xk_pg_parser_Datum attr,
                                      xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum xml_out(xk_pg_parser_Datum attr,
                           xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum byteaout(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum enum_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum anyenum_out(xk_pg_parser_Datum attr,
                                      xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum tsvectorout(xk_pg_parser_Datum attr,
                               xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum bit_out(xk_pg_parser_Datum attr,
                                  xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum varbit_out(xk_pg_parser_Datum attr,
                                  xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum path_out(xk_pg_parser_Datum attr,
                                  xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum poly_out(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum array_out(xk_pg_parser_Datum attr,
                                    xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum int2vectorout(xk_pg_parser_Datum attr,
                                        xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum record_out(xk_pg_parser_Datum attr,
                                     xk_pg_parser_extraTypoutInfo *info);
//extern xk_pg_parser_Datum anyarray_out(xk_pg_parser_Datum attr,
//                                       xk_pg_parser_extraTypoutInfo *info);
//
extern xk_pg_parser_Datum jsonb_out(xk_pg_parser_Datum attr,
                                    xk_pg_parser_extraTypoutInfo *info);

//extern xk_pg_parser_Datum anyrange_out(xk_pg_parser_Datum attr,
//                                       xk_pg_parser_extraTypoutInfo *info);
//
extern xk_pg_parser_Datum range_out(xk_pg_parser_Datum attr,
                                    xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum rowidout(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

//extern xk_pg_parser_Datum jsonpath_out(xk_pg_parser_Datum attr,
//                                       xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum spheroid_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum box3d_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum box2d_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum box2df_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum gidx_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum raster_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum geometry_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

extern xk_pg_parser_Datum geography_out(xk_pg_parser_Datum attr,
                                   xk_pg_parser_extraTypoutInfo *info);

#endif
