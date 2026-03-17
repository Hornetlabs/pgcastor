/**
 * @file                xk_pg_parser_thirdparty_timezone_tzdata_info.h
 * @author              ByteSynch
 * @brief               定义 zic 函数的调用 入口
 * @version             0.1
 * @date                2023-09-18
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H
#define XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H

#include "thirdparty/stringinfo/xk_pg_parser_thirdparty_stringinfo.h"
#include "./zones/Asia/xk_pg_parser_zones_asia_all.h"
#include "./zones/America/xk_pg_parser_zones_america_all.h"
#include "./zones/Africa/xk_pg_parser_zones_africa_all.h"
#include "./zones/Europe/xk_pg_parser_zones_europe_all.h"
#include "./zones/Antarctica/xk_pg_parser_zones_antarctica_all.h"
#include "./zones/Arctic/xk_pg_parser_zones_arctic_all.h"
#include "./zones/Atlantic/xk_pg_parser_zones_atlantic_all.h"
#include "./zones/Australia/xk_pg_parser_zones_australia_all.h"
#include "./zones/Brazil/xk_pg_parser_zones_brazil_all.h"
#include "./zones/Canada/xk_pg_parser_zones_canada_all.h"
#include "./zones/Chile/xk_pg_parser_zones_chile_all.h"
#include "./zones/Etc/xk_pg_parser_zones_etc_all.h"
#include "./zones/Indian/xk_pg_parser_zones_indian_all.h"
#include "./zones/Mexico/xk_pg_parser_zones_mexico_all.h"
#include "./zones/Pacific/xk_pg_parser_zones_pacific_all.h"
#include "./zones/Mideast/xk_pg_parser_zones_mideast_all.h"
#include "./zones/US/xk_pg_parser_zones_us_all.h"
#include "./zones/Top/xk_pg_parser_zones_other_all.h"

/* 获取时区数组 */
char** xk_pg_parser_get_tzdata_info(const char *tz_name, int32_t *tz_dataSize);

/* 获取时区数据的信息。 */
void xk_pg_parser_zic_get_tzdata(const char* dbtz_name, xk_pg_parser_StringInfo local_tzdata);

#endif /* XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H */
