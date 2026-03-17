#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Australia_North[] =
{
    (char *)"Rule	Aus	1917	only	-	Jan	 1	0:01	1:00	D",
    (char *)"Rule	Aus	1917	only	-	Mar	25	2:00	0	S",
    (char *)"Rule	Aus	1942	only	-	Jan	 1	2:00	1:00	D",
    (char *)"Rule	Aus	1942	only	-	Mar	29	2:00	0	S",
    (char *)"Rule	Aus	1942	only	-	Sep	27	2:00	1:00	D",
    (char *)"Rule	Aus	1943	1944	-	Mar	lastSun	2:00	0	S",
    (char *)"Rule	Aus	1943	only	-	Oct	 3	2:00	1:00	D",
    (char *)"Zone Australia/Darwin	 8:43:20 -	LMT	1895 Feb",
    (char *)"			 9:00	-	ACST	1899 May",
    (char *)"			 9:30	Aus	AC%sT",
    (char *)"Link	Australia/Darwin	Australia/North"
};
