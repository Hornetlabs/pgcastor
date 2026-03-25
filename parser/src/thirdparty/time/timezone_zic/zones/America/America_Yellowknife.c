#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_America_Yellowknife[] = {
    (char*)"Rule	NT_YK	1918	only	-	Apr	14	2:00	1:00	D",
    (char*)"Rule	NT_YK	1918	only	-	Oct	27	2:00	0	S",
    (char*)"Rule	NT_YK	1919	only	-	May	25	2:00	1:00	D",
    (char*)"Rule	NT_YK	1919	only	-	Nov	 1	0:00	0	S",
    (char*)"Rule	NT_YK	1942	only	-	Feb	 9	2:00	1:00	W # War",
    (char*)"Rule	NT_YK	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char*)"Rule	NT_YK	1945	only	-	Sep	30	2:00	0	S",
    (char*)"Rule	NT_YK	1965	only	-	Apr	lastSun	0:00	2:00	DD",
    (char*)"Rule	NT_YK	1965	only	-	Oct	lastSun	2:00	0	S",
    (char*)"Rule	NT_YK	1980	1986	-	Apr	lastSun	2:00	1:00	D",
    (char*)"Rule	NT_YK	1980	2006	-	Oct	lastSun	2:00	0	S",
    (char*)"Rule	NT_YK	1987	2006	-	Apr	Sun>=1	2:00	1:00	D",
    (char*)"Rule	Canada	1918	only	-	Apr	14	2:00	1:00	D",
    (char*)"Rule	Canada	1918	only	-	Oct	27	2:00	0	S",
    (char*)"Rule	Canada	1942	only	-	Feb	 9	2:00	1:00	W # War",
    (char*)"Rule	Canada	1945	only	-	Aug	14	23:00u	1:00	P # Peace",
    (char*)"Rule	Canada	1945	only	-	Sep	30	2:00	0	S",
    (char*)"Rule	Canada	1974	1986	-	Apr	lastSun	2:00	1:00	D",
    (char*)"Rule	Canada	1974	2006	-	Oct	lastSun	2:00	0	S",
    (char*)"Rule	Canada	1987	2006	-	Apr	Sun>=1	2:00	1:00	D",
    (char*)"Rule	Canada	2007	max	-	Mar	Sun>=8	2:00	1:00	D",
    (char*)"Rule	Canada	2007	max	-	Nov	Sun>=1	2:00	0	S",
    (char*)"Zone America/Yellowknife 0	-	zzz	1935 # Yellowknife founded?",
    (char*)"			-7:00	NT_YK	M%sT	1980",
    (char*)"			-7:00	Canada	M%sT"};
