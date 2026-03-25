/**
 * @file                pg_parser_thirdparty_timezone_tzdata_info.h
 * @author              ByteSynch
 * @brief               Define entry point for zic function calls
 * @version             0.1
 * @date                2023-09-18
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H
#define PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H

#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"
#include "./zones/Asia/pg_parser_zones_asia_all.h"
#include "./zones/America/pg_parser_zones_america_all.h"
#include "./zones/Africa/pg_parser_zones_africa_all.h"
#include "./zones/Europe/pg_parser_zones_europe_all.h"
#include "./zones/Antarctica/pg_parser_zones_antarctica_all.h"
#include "./zones/Arctic/pg_parser_zones_arctic_all.h"
#include "./zones/Atlantic/pg_parser_zones_atlantic_all.h"
#include "./zones/Australia/pg_parser_zones_australia_all.h"
#include "./zones/Brazil/pg_parser_zones_brazil_all.h"
#include "./zones/Canada/pg_parser_zones_canada_all.h"
#include "./zones/Chile/pg_parser_zones_chile_all.h"
#include "./zones/Etc/pg_parser_zones_etc_all.h"
#include "./zones/Indian/pg_parser_zones_indian_all.h"
#include "./zones/Mexico/pg_parser_zones_mexico_all.h"
#include "./zones/Pacific/pg_parser_zones_pacific_all.h"
#include "./zones/Mideast/pg_parser_zones_mideast_all.h"
#include "./zones/US/pg_parser_zones_us_all.h"
#include "./zones/Top/pg_parser_zones_other_all.h"

/* Get timezone array */
char** pg_parser_get_tzdata_info(const char* tz_name, int32_t* tz_dataSize);

/* Get timezone data information. */
void pg_parser_zic_get_tzdata(const char* dbtz_name, pg_parser_StringInfo local_tzdata);

#endif /* PG_PARSER_THIRDPARTY_TIMEZONE_TZDATA_INFO_H */
