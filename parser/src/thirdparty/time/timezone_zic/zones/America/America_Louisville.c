#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_America_Louisville[] =
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
    (char *)"Rule Louisville	1921	only	-	May	1	2:00	1:00	D",
    (char *)"Rule Louisville	1921	only	-	Sep	1	2:00	0	S",
    (char *)"Rule Louisville	1941	only	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule Louisville	1941	only	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule Louisville	1946	only	-	Apr	lastSun	0:01	1:00	D",
    (char *)"Rule Louisville	1946	only	-	Jun	2	2:00	0	S",
    (char *)"Rule Louisville	1950	1961	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule Louisville	1950	1955	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule Louisville	1956	1961	-	Oct	lastSun	2:00	0	S",
    (char *)"Zone America/Kentucky/Louisville -5:43:02 -	LMT	1883 Nov 18 12:16:58",
    (char *)"			-6:00	US	C%sT	1921",
    (char *)"			-6:00 Louisville C%sT	1942",
    (char *)"			-6:00	US	C%sT	1946",
    (char *)"			-6:00 Louisville C%sT	1961 Jul 23  2:00",
    (char *)"			-5:00	-	EST	1968",
    (char *)"			-5:00	US	E%sT	1974 Jan  6  2:00",
    (char *)"			-6:00	1:00	CDT	1974 Oct 27  2:00",
    (char *)"			-5:00	US	E%sT",
    (char *)"Link	America/Kentucky/Louisville	America/Louisville"
};
