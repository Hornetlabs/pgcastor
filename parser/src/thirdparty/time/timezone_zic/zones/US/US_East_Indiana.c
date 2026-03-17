#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_US_East_Indiana[] =
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
    (char *)"Rule Indianapolis 1941	only	-	Jun	22	2:00	1:00	D",
    (char *)"Rule Indianapolis 1941	1954	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule Indianapolis 1946	1954	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Zone America/Indiana/Indianapolis -5:44:38 - LMT	1883 Nov 18 12:15:22",
    (char *)"			-6:00	US	C%sT	1920",
    (char *)"			-6:00 Indianapolis C%sT	1942",
    (char *)"			-6:00	US	C%sT	1946",
    (char *)"			-6:00 Indianapolis C%sT	1955 Apr 24  2:00",
    (char *)"			-5:00	-	EST	1957 Sep 29  2:00",
    (char *)"			-6:00	-	CST	1958 Apr 27  2:00",
    (char *)"			-5:00	-	EST	1969",
    (char *)"			-5:00	US	E%sT	1971",
    (char *)"			-5:00	-	EST	2006",
    (char *)"			-5:00	US	E%sT",
    (char *)"Link	America/Indiana/Indianapolis	US/East-Indiana"
};
