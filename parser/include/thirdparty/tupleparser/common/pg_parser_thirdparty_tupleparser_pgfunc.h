#ifndef PG_PARSER_THIRDPARTY_TUPLEPARSER_PGFUNC_H
#define PG_PARSER_THIRDPARTY_TUPLEPARSER_PGFUNC_H

/*
 * 定义函数指针, 为普通类型提供函数原型
 */
typedef pg_parser_Datum (*pg_parser_Local_PGFunctionNormal) (pg_parser_Datum attr);

typedef struct
{
    const char                           *funcName;           /* C name of the function */
    pg_parser_Local_PGFunctionNormal   func;               /* pointer to compiled function */
} pg_parser_FmgrBuiltinNormal;


/* normal function extern */
extern pg_parser_Datum charout(pg_parser_Datum attr);
extern pg_parser_Datum nameout(pg_parser_Datum attr);
extern pg_parser_Datum int2out(pg_parser_Datum attr);
extern pg_parser_Datum int4out(pg_parser_Datum attr);
extern pg_parser_Datum tidout(pg_parser_Datum attr);
extern pg_parser_Datum xidout(pg_parser_Datum attr);
extern pg_parser_Datum cidout(pg_parser_Datum attr);
extern pg_parser_Datum oidvectorout(pg_parser_Datum attr);
extern pg_parser_Datum point_out(pg_parser_Datum attr);
extern pg_parser_Datum lseg_out(pg_parser_Datum attr);
extern pg_parser_Datum box_out(pg_parser_Datum attr);
extern pg_parser_Datum float4out(pg_parser_Datum attr);
extern pg_parser_Datum float8out(pg_parser_Datum attr);
extern pg_parser_Datum macaddr_out(pg_parser_Datum attr);
extern pg_parser_Datum int8out(pg_parser_Datum attr);
extern pg_parser_Datum inet_out(pg_parser_Datum attr);
extern pg_parser_Datum date_out(pg_parser_Datum attr);
extern pg_parser_Datum time_out(pg_parser_Datum attr);
extern pg_parser_Datum interval_out(pg_parser_Datum attr);
extern pg_parser_Datum dsinterval_out(pg_parser_Datum attr);
extern pg_parser_Datum boolout(pg_parser_Datum attr);
extern pg_parser_Datum timetz_out(pg_parser_Datum attr);
extern pg_parser_Datum cidr_out(pg_parser_Datum attr);
extern pg_parser_Datum circle_out(pg_parser_Datum attr);
extern pg_parser_Datum line_out(pg_parser_Datum attr);
extern pg_parser_Datum oidout(pg_parser_Datum attr);
extern pg_parser_Datum cstring_out(pg_parser_Datum attr);
extern pg_parser_Datum void_out(pg_parser_Datum attr);
extern pg_parser_Datum shell_out(pg_parser_Datum attr);
extern pg_parser_Datum uuid_out(pg_parser_Datum attr);
extern pg_parser_Datum tsqueryout(pg_parser_Datum attr);
extern pg_parser_Datum macaddr8_out(pg_parser_Datum attr);
extern pg_parser_Datum pg_lsn_out(pg_parser_Datum attr);
extern pg_parser_Datum regclassout(pg_parser_Datum attr);
#endif
