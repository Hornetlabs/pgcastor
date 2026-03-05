#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_America_Moncton[] =
{
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
    (char *)"Rule	Moncton	1933	1935	-	Jun	Sun>=8	1:00	1:00	D",
    (char *)"Rule	Moncton	1933	1935	-	Sep	Sun>=8	1:00	0	S",
    (char *)"Rule	Moncton	1936	1938	-	Jun	Sun>=1	1:00	1:00	D",
    (char *)"Rule	Moncton	1936	1938	-	Sep	Sun>=1	1:00	0	S",
    (char *)"Rule	Moncton	1939	only	-	May	27	1:00	1:00	D",
    (char *)"Rule	Moncton	1939	1941	-	Sep	Sat>=21	1:00	0	S",
    (char *)"Rule	Moncton	1940	only	-	May	19	1:00	1:00	D",
    (char *)"Rule	Moncton	1941	only	-	May	 4	1:00	1:00	D",
    (char *)"Rule	Moncton	1946	1972	-	Apr	lastSun	2:00	1:00	D",
    (char *)"Rule	Moncton	1946	1956	-	Sep	lastSun	2:00	0	S",
    (char *)"Rule	Moncton	1957	1972	-	Oct	lastSun	2:00	0	S",
    (char *)"Rule	Moncton	1993	2006	-	Apr	Sun>=1	0:01	1:00	D",
    (char *)"Rule	Moncton	1993	2006	-	Oct	lastSun	0:01	0	S",
    (char *)"Zone America/Moncton	-4:19:08 -	LMT	1883 Dec  9",
    (char *)"			-5:00	-	EST	1902 Jun 15",
    (char *)"			-4:00	Canada	A%sT	1933",
    (char *)"			-4:00	Moncton	A%sT	1942",
    (char *)"			-4:00	Canada	A%sT	1946",
    (char *)"			-4:00	Moncton	A%sT	1973",
    (char *)"			-4:00	Canada	A%sT	1993",
    (char *)"			-4:00	Moncton	A%sT	2007",
    (char *)"			-4:00	Canada	A%sT"
};
