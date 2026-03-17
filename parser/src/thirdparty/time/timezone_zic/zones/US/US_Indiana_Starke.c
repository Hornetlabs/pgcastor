#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_US_Indiana_Starke[] =
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
    (char *)"Rule	Starke	1947	1961	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Starke	1947	1954	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Starke	1955	1956	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Starke	1957	1958	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Starke	1959	1961	-	Oct	lastSun	2:00	0	S",
    (char *)"Zone America/Indiana/Knox -5:46:30 -	LMT	1883 Nov 18 12:13:30",
    (char *)"			-6:00	US	C%sT	1947",
    (char *)"			-6:00	Starke	C%sT	1962 Apr 29  2:00",
    (char *)"			-5:00	-	EST	1963 Oct 27  2:00",
    (char *)"			-6:00	US	C%sT	1991 Oct 27  2:00",
    (char *)"			-5:00	-	EST	2006 Apr  2  2:00",
    (char *)"			-6:00	US	C%sT",
    (char *)"Link	America/Indiana/Knox	US/Indiana-Starke"
};
