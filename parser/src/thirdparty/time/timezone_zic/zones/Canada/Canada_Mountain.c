#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Canada_Mountain[] =
{
    (char *)"Rule	Edm	1918	1919	-	Apr	Sun>=8	2:00	1:00	D",
    (char *)"Rule	Edm	1918	only	-	Oct	27	2:00	0	S",
    (char *)"Rule	Edm	1919	only	-	May	27	2:00	0	S",
    (char *)"Rule	Edm	1920	1923	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Edm	1920	only	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Edm	1921	1923	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Edm	1942	only	-	Feb	 9	2:00	1:00	W # War",
    (char *)"Rule	Edm	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char *)"Rule	Edm	1945	only	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Edm	1947	only	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Edm	1947	only	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Edm	1972	1986	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Edm	1972	2006	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Canada	1918	only	-	Apr	14	2:00	1:00	D",
    (char *)"Rule	Canada	1918	only	-	Oct	27	2:00	0	S",
    (char *)"Rule	Canada	1942	only	-	Feb	 9	2:00	1:00	W # War",
    (char *)"Rule	Canada	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char *)"Rule	Canada	1945	only	-	Sep	30	2:00	0	S",
    (char *)"Rule	Canada	1974	1986	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Canada	1974	2006	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Canada	1987	2006	-	Apr	Sun>=1	2:00	1:00	D",
    (char *)"Rule	Canada	2007	max	-	Mar	Sun>=8	2:00	1:00	D",
    (char *)"Rule	Canada	2007	max	-	Nov	Sun>=1	2:00	0	S",
    (char *)"Zone America/Edmonton	-7:33:52 -	LMT	1906 Sep",
    (char *)"			-7:00	Edm	M%sT	1987",
    (char *)"			-7:00	Canada	M%sT",
    (char *)"Link	America/Edmonton	Canada/Mountain"
};
