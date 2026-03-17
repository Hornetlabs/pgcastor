#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_America_Juneau[] =
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
    (char *)"Zone America/Juneau	 15:02:19 -	LMT	1867 Oct 19 15:33:32",
    (char *)"			 -8:57:41 -	LMT	1900 Aug 20 12:00",
    (char *)"			 -8:00	-	PST	1942",
    (char *)"			 -8:00	US	P%sT	1946",
    (char *)"			 -8:00	-	PST	1969",
    (char *)"			 -8:00	US	P%sT	1980 Apr 27  2:00",
    (char *)"			 -9:00	US	Y%sT	1980 Oct 26  2:00",
    (char *)"			 -8:00	US	P%sT	1983 Oct 30  2:00",
    (char *)"			 -9:00	US	Y%sT	1983 Nov 30",
    (char *)"			 -9:00	US	AK%sT"
};
