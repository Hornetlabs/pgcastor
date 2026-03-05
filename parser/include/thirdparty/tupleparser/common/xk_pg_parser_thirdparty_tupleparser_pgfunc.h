#ifndef XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_PGFUNC_H
#define XK_PG_PARSER_THIRDPARTY_TUPLEPARSER_PGFUNC_H

/*
 * 定义函数指针, 为普通类型提供函数原型
 */
typedef xk_pg_parser_Datum (*xk_pg_parser_Local_PGFunctionNormal) (xk_pg_parser_Datum attr);

typedef struct
{
    const char                           *funcName;           /* C name of the function */
    xk_pg_parser_Local_PGFunctionNormal   func;               /* pointer to compiled function */
} xk_pg_parser_FmgrBuiltinNormal;


/* normal function extern */
extern xk_pg_parser_Datum charout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum nameout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum int2out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum int4out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum tidout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum xidout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum cidout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum oidvectorout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum point_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum lseg_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum box_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum float4out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum float8out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum macaddr_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum int8out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum inet_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum date_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum time_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum interval_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum dsinterval_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum boolout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum timetz_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum cidr_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum circle_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum line_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum oidout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum cstring_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum void_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum shell_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum uuid_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum tsqueryout(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum macaddr8_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum pg_lsn_out(xk_pg_parser_Datum attr);
extern xk_pg_parser_Datum regclassout(xk_pg_parser_Datum attr);
#endif
