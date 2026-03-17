#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Navajo[] =
{
    (char *)"Rule	US	1918	1919	-	Mar	lastSun	2:00	1:00	D",
    (char *)"Rule	US	1918	1919	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	US	1942	only	-	Feb	9	2:00	1:00	W # War",
    (char *)"Rule	US	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char *)"Rule	US	1945	only	-	Sep	30	2:00	0	S",
    (char *)"Rule	US	1967	2006	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	US	1967	1973	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	US	1974	only	-	Jan	6	2:00	1:00	D",
    (char *)"Rule	US	1975	only	-	Feb	lastSun	2:00	1:00	D",
    (char *)"Rule	US	1976	1986	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	US	1987	2006	-	Apr	Sun>=1	2:00	1:00	D",
    (char *)"Rule	US	2007	max	-	Mar	Sun>=8	2:00	1:00	D",
    (char *)"Rule	US	2007	max	-	Nov	Sun>=1	2:00	0	S",
    (char *)"Rule	Denver	1920	1921	-	Mar	lastSun	2:00	1:00	D",
    (char *)"Rule	Denver	1920	only	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Denver	1921	only	-	May	22	2:00	0	S",
    (char *)"Rule	Denver	1965	1966	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Denver	1965	1966	-	Oct	lastSun	2:00	0	S",
    (char *)"Zone America/Denver	-6:59:56 -	LMT	1883 Nov 18 12:00:04",
    (char *)"			-7:00	US	M%sT	1920",
    (char *)"			-7:00	Denver	M%sT	1942",
    (char *)"			-7:00	US	M%sT	1946",
    (char *)"			-7:00	Denver	M%sT	1967",
    (char *)"			-7:00	US	M%sT",
    (char *)"Link	America/Denver		Navajo"
};
