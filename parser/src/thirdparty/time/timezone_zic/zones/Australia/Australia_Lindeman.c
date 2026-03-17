#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Australia_Lindeman[] =
{
    (char *)"Rule	Aus	1917	only	-	Jan	 1	0:01	1:00	D",
    (char *)"Rule	Aus	1917	only	-	Mar	25	2:00	0	S",
    (char *)"Rule	Aus	1942	only	-	Jan	 1	2:00	1:00	D",
    (char *)"Rule	Aus	1942	only	-	Mar	29	2:00	0	S",
    (char *)"Rule	Aus	1942	only	-	Sep	27	2:00	1:00	D",
    (char *)"Rule	Aus	1943	1944	-	Mar	lastSun	2:00	0	S",
    (char *)"Rule	Aus	1943	only	-	Oct	 3	2:00	1:00	D",
    (char *)"Rule	AQ	1971	only	-	Oct	lastSun	2:00s	1:00	D",
    (char *)"Rule	AQ	1972	only	-	Feb	lastSun	2:00s	0	S",
    (char *)"Rule	AQ	1989	1991	-	Oct	lastSun	2:00s	1:00	D",
    (char *)"Rule	AQ	1990	1992	-	Mar	Sun>=1	2:00s	0	S",
    (char *)"Rule	Holiday	1992	1993	-	Oct	lastSun	2:00s	1:00	D",
    (char *)"Rule	Holiday	1993	1994	-	Mar	Sun>=1	2:00s	0	S",
    (char *)"Zone Australia/Lindeman  9:55:56 -	LMT	1895",
    (char *)"			10:00	Aus	AE%sT	1971",
    (char *)"			10:00	AQ	AE%sT	1992 Jul",
    (char *)"			10:00	Holiday	AE%sT"
};
