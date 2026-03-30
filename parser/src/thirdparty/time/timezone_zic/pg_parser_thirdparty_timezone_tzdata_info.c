/**
 * @file                pg_parser_thirdparty_timezone_tzdata_info.cpp
 * @author              ByteSynch
 * @brief               Implementation of timezone location function
 * @version             0.1
 * @date                2023-09-19
 *
 * @copyright           Copyright (c) 2023
 *
 */

#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

typedef struct LOCAL_TZDATA_DISPATCH_STRUCT
{
    char*  tz_name;
    char** tz_data;
    int    tz_data_size;
} local_tzdata_dispatch_struct;

static local_tzdata_dispatch_struct africa_tzdata_info[] = {
    {(char*)"Africa/Abidjan",       PG_Africa_Abidjan,       3  },
    {(char*)"Africa/Accra",         PG_Africa_Accra,         5  },
    {(char*)"Africa/Addis_Ababa",   PG_Africa_Addis_Ababa,   7  },
    {(char*)"Africa/Algiers",       PG_Africa_Algiers,       33 },
    {(char*)"Africa/Asmara",        PG_Africa_Asmara,        7  },
    {(char*)"Africa/Asmera",        PG_Africa_Asmera,        7  },
    {(char*)"Africa/Bamako",        PG_Africa_Bamako,        4  },
    {(char*)"Africa/Bangui",        PG_Africa_Bangui,        4  },
    {(char*)"Africa/Banjul",        PG_Africa_Banjul,        4  },
    {(char*)"Africa/Bissau",        PG_Africa_Bissau,        4  },
    {(char*)"Africa/Blantyre",      PG_Africa_Blantyre,      4  },
    {(char*)"Africa/Brazzaville",   PG_Africa_Brazzaville,   4  },
    {(char*)"Africa/Bujumbura",     PG_Africa_Bujumbura,     4  },
    {(char*)"Africa/Cairo",         PG_Africa_Cairo,         35 },
    {(char*)"Africa/Casablanca",    PG_Africa_Casablanca,    189},
    {(char*)"Africa/Ceuta",         PG_Africa_Ceuta,         55 },
    {(char*)"Africa/Conakry",       PG_Africa_Conakry,       4  },
    {(char*)"Africa/Dakar",         PG_Africa_Dakar,         4  },
    {(char*)"Africa/Dar_es_Salaam", PG_Africa_Dar_es_Salaam, 7  },
    {(char*)"Africa/Djibouti",      PG_Africa_Djibouti,      7  },
    {(char*)"Africa/Douala",        PG_Africa_Douala,        4  },
    {(char*)"Africa/El_Aaiun",      PG_Africa_El_Aaiun,      188},
    {(char*)"Africa/Freetown",      PG_Africa_Freetown,      4  },
    {(char*)"Africa/Gaborone",      PG_Africa_Gaborone,      4  },
    {(char*)"Africa/Harare",        PG_Africa_Harare,        4  },
    {(char*)"Africa/Johannesburg",  PG_Africa_Johannesburg,  6  },
    {(char*)"Africa/Juba",          PG_Africa_Juba,          8  },
    {(char*)"Africa/Kampala",       PG_Africa_Kampala,       7  },
    {(char*)"Africa/Khartoum",      PG_Africa_Khartoum,      9  },
    {(char*)"Africa/Kigali",        PG_Africa_Kigali,        4  },
    {(char*)"Africa/Kinshasa",      PG_Africa_Kinshasa,      4  },
    {(char*)"Africa/Lagos",         PG_Africa_Lagos,         3  },
    {(char*)"Africa/Libreville",    PG_Africa_Libreville,    4  },
    {(char*)"Africa/Lome",          PG_Africa_Lome,          4  },
    {(char*)"Africa/Luanda",        PG_Africa_Luanda,        4  },
    {(char*)"Africa/Lubumbashi",    PG_Africa_Lubumbashi,    4  },
    {(char*)"Africa/Lusaka",        PG_Africa_Lusaka,        4  },
    {(char*)"Africa/Malabo",        PG_Africa_Malabo,        4  },
    {(char*)"Africa/Maputo",        PG_Africa_Maputo,        3  },
    {(char*)"Africa/Maseru",        PG_Africa_Maseru,        7  },
    {(char*)"Africa/Mbabane",       PG_Africa_Mbabane,       7  },
    {(char*)"Africa/Mogadishu",     PG_Africa_Mogadishu,     7  },
    {(char*)"Africa/Monrovia",      PG_Africa_Monrovia,      5  },
    {(char*)"Africa/Nairobi",       PG_Africa_Nairobi,       6  },
    {(char*)"Africa/Ndjamena",      PG_Africa_Ndjamena,      5  },
    {(char*)"Africa/Niamey",        PG_Africa_Niamey,        4  },
    {(char*)"Africa/Nouakchott",    PG_Africa_Nouakchott,    4  },
    {(char*)"Africa/Ouagadougou",   PG_Africa_Ouagadougou,   4  },
    {(char*)"Africa/Porto-Novo",    PG_Africa_Porto_Novo,    4  },
    {(char*)"Africa/Sao_Tome",      PG_Africa_Sao_Tome,      6  },
    {(char*)"Africa/Timbuktu",      PG_Africa_Timbuktu,      4  },
    {(char*)"Africa/Tripoli",       PG_Africa_Tripoli,       27 },
    {(char*)"Africa/Tunis",         PG_Africa_Tunis,         29 },
    {(char*)"Africa/Windhoek",      PG_Africa_Windhoek,      10 }
};

static local_tzdata_dispatch_struct america_tzdata_info[] = {
    {(char*)"America/Adak",                     PG_America_Adak,                     23},
    {(char*)"America/Anchorage",                PG_America_Anchorage,                22},
    {(char*)"America/Anguilla",                 PG_America_Anguilla,                 4 },
    {(char*)"America/Antigua",                  PG_America_Antigua,                  4 },
    {(char*)"America/Araguaina",                PG_America_Araguaina,                65},
    {(char*)"America/Argentina/Buenos_Aires",   PG_America_Argentina_Buenos_Aires,   37},
    {(char*)"America/Argentina/Catamarca",      PG_America_Argentina_Catamarca,      42},
    {(char*)"America/Argentina/ComodRivadavia", PG_America_Argentina_ComodRivadavia, 43},
    {(char*)"America/Argentina/Cordoba",        PG_America_Argentina_Cordoba,        39},
    {(char*)"America/Argentina/Jujuy",          PG_America_Argentina_Jujuy,          43},
    {(char*)"America/Argentina/La_Rioja",       PG_America_Argentina_La_Rioja,       42},
    {(char*)"America/Argentina/Mendoza",        PG_America_Argentina_Mendoza,        46},
    {(char*)"America/Argentina/Rio_Gallegos",   PG_America_Argentina_Rio_Gallegos,   40},
    {(char*)"America/Argentina/Salta",          PG_America_Argentina_Salta,          40},
    {(char*)"America/Argentina/San_Juan",       PG_America_Argentina_San_Juan,       42},
    {(char*)"America/Argentina/San_Luis",       PG_America_Argentina_San_Luis,       48},
    {(char*)"America/Argentina/Tucuman",        PG_America_Argentina_Tucuman,        41},
    {(char*)"America/Argentina/Ushuaia",        PG_America_Argentina_Ushuaia,        40},
    {(char*)"America/Aruba",                    PG_America_Aruba,                    5 },
    {(char*)"America/Asuncion",                 PG_America_Asuncion,                 28},
    {(char*)"America/Atikokan",                 PG_America_Atikokan,                 16},
    {(char*)"America/Atka",                     PG_America_Atka,                     24},
    {(char*)"America/Bahia",                    PG_America_Bahia,                    63},
    {(char*)"America/Bahia_Banderas",           PG_America_Bahia_Banderas,           26},
    {(char*)"America/Barbados",                 PG_America_Barbados,                 9 },
    {(char*)"America/Belem",                    PG_America_Belem,                    61},
    {(char*)"America/Belize",                   PG_America_Belize,                   9 },
    {(char*)"America/Blanc-Sablon",             PG_America_Blanc_Sablon,             14},
    {(char*)"America/Boa_Vista",                PG_America_Boa_Vista,                63},
    {(char*)"America/Bogota",                   PG_America_Bogota,                   6 },
    {(char*)"America/Boise",                    PG_America_Boise,                    19},
    {(char*)"America/Buenos_Aires",             PG_America_Buenos_Aires,             38},
    {(char*)"America/Cambridge_Bay",            PG_America_Cambridge_Bay,            29},
    {(char*)"America/Campo_Grande",             PG_America_Campo_Grande,             60},
    {(char*)"America/Cancun",                   PG_America_Cancun,                   20},
    {(char*)"America/Caracas",                  PG_America_Caracas,                  7 },
    {(char*)"America/Catamarca",                PG_America_Catamarca,                43},
    {(char*)"America/Cayenne",                  PG_America_Cayenne,                  4 },
    {(char*)"America/Cayman",                   PG_America_Cayman,                   5 },
    {(char*)"America/Chicago",                  PG_America_Chicago,                  28},
    {(char*)"America/Chihuahua",                PG_America_Chihuahua,                25},
    {(char*)"America/Coral_Harbour",            PG_America_Coral_Harbour,            17},
    {(char*)"America/Cordoba",                  PG_America_Cordoba,                  40},
    {(char*)"America/Costa_Rica",               PG_America_Costa_Rica,               9 },
    {(char*)"America/Creston",                  PG_America_Creston,                  5 },
    {(char*)"America/Cuiaba",                   PG_America_Cuiaba,                   62},
    {(char*)"America/Curacao",                  PG_America_Curacao,                  4 },
    {(char*)"America/Danmarkshavn",             PG_America_Danmarkshavn,             11},
    {(char*)"America/Dawson",                   PG_America_Dawson,                   28},
    {(char*)"America/Dawson_Creek",             PG_America_Dawson_Creek,             24},
    {(char*)"America/Denver",                   PG_America_Denver,                   25},
    {(char*)"America/Detroit",                  PG_America_Detroit,                  26},
    {(char*)"America/Dominica",                 PG_America_Dominica,                 4 },
    {(char*)"America/Edmonton",                 PG_America_Edmonton,                 27},
    {(char*)"America/Eirunepe",                 PG_America_Eirunepe,                 65},
    {(char*)"America/El_Salvador",              PG_America_El_Salvador,              5 },
    {(char*)"America/Ensenada",                 PG_America_Ensenada,                 53},
    {(char*)"America/Fortaleza",                PG_America_Fortaleza,                65},
    {(char*)"America/Fort_Nelson",              PG_America_Fort_Nelson,              26},
    {(char*)"America/Fort_Wayne",               PG_America_Fort_Wayne,               29},
    {(char*)"America/Glace_Bay",                PG_America_Glace_Bay,                58},
    {(char*)"America/Godthab",                  PG_America_Godthab,                  11},
    {(char*)"America/Goose_Bay",                PG_America_Goose_Bay,                40},
    {(char*)"America/Grand_Turk",               PG_America_Grand_Turk,               20},
    {(char*)"America/Grenada",                  PG_America_Grenada,                  4 },
    {(char*)"America/Guadeloupe",               PG_America_Guadeloupe,               4 },
    {(char*)"America/Guatemala",                PG_America_Guatemala,                11},
    {(char*)"America/Guayaquil",                PG_America_Guayaquil,                6 },
    {(char*)"America/Guyana",                   PG_America_Guyana,                   5 },
    {(char*)"America/Halifax",                  PG_America_Halifax,                  59},
    {(char*)"America/Havana",                   PG_America_Havana,                   43},
    {(char*)"America/Hermosillo",               PG_America_Hermosillo,               26},
    {(char*)"America/Indiana/Indianapolis",     PG_America_Indiana_Indianapolis,     28},
    {(char*)"America/Indiana/Knox",             PG_America_Indiana_Knox,             26},
    {(char*)"America/Indiana/Marengo",          PG_America_Indiana_Marengo,          27},
    {(char*)"America/Indiana/Petersburg",       PG_America_Indiana_Petersburg,       26},
    {(char*)"America/Indiana/Tell_City",        PG_America_Indiana_Tell_City,        26},
    {(char*)"America/Indiana/Vevay",            PG_America_Indiana_Vevay,            20},
    {(char*)"America/Indiana/Vincennes",        PG_America_Indiana_Vincennes,        31},
    {(char*)"America/Indiana/Winamac",          PG_America_Indiana_Winamac,          26},
    {(char*)"America/Indianapolis",             PG_America_Indianapolis,             29},
    {(char*)"America/Inuvik",                   PG_America_Inuvik,                   27},
    {(char*)"America/Iqaluit",                  PG_America_Iqaluit,                  27},
    {(char*)"America/Jamaica",                  PG_America_Jamaica,                  19},
    {(char*)"America/Jujuy",                    PG_America_Jujuy,                    44},
    {(char*)"America/Juneau",                   PG_America_Juneau,                   24},
    {(char*)"America/Kentucky/Louisville",      PG_America_Kentucky_Louisville,      32},
    {(char*)"America/Kentucky/Monticello",      PG_America_Kentucky_Monticello,      19},
    {(char*)"America/Knox_IN",                  PG_America_Knox_IN,                  27},
    {(char*)"America/Kralendijk",               PG_America_Kralendijk,               5 },
    {(char*)"America/La_Paz",                   PG_America_La_Paz,                   5 },
    {(char*)"America/Lima",                     PG_America_Lima,                     14},
    {(char*)"America/Los_Angeles",              PG_America_Los_Angeles,              23},
    {(char*)"America/Louisville",               PG_America_Louisville,               33},
    {(char*)"America/Lower_Princes",            PG_America_Lower_Princes,            5 },
    {(char*)"America/Maceio",                   PG_America_Maceio,                   67},
    {(char*)"America/Managua",                  PG_America_Managua,                  16},
    {(char*)"America/Manaus",                   PG_America_Manaus,                   63},
    {(char*)"America/Marigot",                  PG_America_Marigot,                  4 },
    {(char*)"America/Martinique",               PG_America_Martinique,               6 },
    {(char*)"America/Matamoros",                PG_America_Matamoros,                33},
    {(char*)"America/Mazatlan",                 PG_America_Mazatlan,                 25},
    {(char*)"America/Mendoza",                  PG_America_Mendoza,                  47},
    {(char*)"America/Menominee",                PG_America_Menominee,                23},
    {(char*)"America/Merida",                   PG_America_Merida,                   19},
    {(char*)"America/Metlakatla",               PG_America_Metlakatla,               24},
    {(char*)"America/Mexico_City",              PG_America_Mexico_City,              24},
    {(char*)"America/Miquelon",                 PG_America_Miquelon,                 15},
    {(char*)"America/Moncton",                  PG_America_Moncton,                  33},
    {(char*)"America/Monterrey",                PG_America_Monterrey,                32},
    {(char*)"America/Montevideo",               PG_America_Montevideo,               60},
    {(char*)"America/Montreal",                 PG_America_Montreal,                 40},
    {(char*)"America/Montserrat",               PG_America_Montserrat,               4 },
    {(char*)"America/Nassau",                   PG_America_Nassau,                   19},
    {(char*)"America/New_York",                 PG_America_New_York,                 25},
    {(char*)"America/Nipigon",                  PG_America_Nipigon,                  15},
    {(char*)"America/Nome",                     PG_America_Nome,                     23},
    {(char*)"America/Noronha",                  PG_America_Noronha,                  65},
    {(char*)"America/North_Dakota/Beulah",      PG_America_North_Dakota_Beulah,      17},
    {(char*)"America/North_Dakota/Center",      PG_America_North_Dakota_Center,      17},
    {(char*)"America/North_Dakota/New_Salem",   PG_America_North_Dakota_New_Salem,   17},
    {(char*)"America/Nuuk",                     PG_America_Nuuk,                     10},
    {(char*)"America/Ojinaga",                  PG_America_Ojinaga,                  39},
    {(char*)"America/Panama",                   PG_America_Panama,                   4 },
    {(char*)"America/Pangnirtung",              PG_America_Pangnirtung,              28},
    {(char*)"America/Paramaribo",               PG_America_Paramaribo,               6 },
    {(char*)"America/Phoenix",                  PG_America_Phoenix,                  21},
    {(char*)"America/Port-au-Prince",           PG_America_Port_au_Prince,           15},
    {(char*)"America/Porto_Acre",               PG_America_Porto_Acre,               64},
    {(char*)"America/Port_of_Spain",            PG_America_Port_of_Spain,            3 },
    {(char*)"America/Porto_Velho",              PG_America_Porto_Velho,              61},
    {(char*)"America/Puerto_Rico",              PG_America_Puerto_Rico,              18},
    {(char*)"America/Punta_Arenas",             PG_America_Punta_Arenas,             44},
    {(char*)"America/Rainy_River",              PG_America_Rainy_River,              15},
    {(char*)"America/Rankin_Inlet",             PG_America_Rankin_Inlet,             27},
    {(char*)"America/Recife",                   PG_America_Recife,                   65},
    {(char*)"America/Regina",                   PG_America_Regina,                   21},
    {(char*)"America/Resolute",                 PG_America_Resolute,                 29},
    {(char*)"America/Rio_Branco",               PG_America_Rio_Branco,               63},
    {(char*)"America/Rosario",                  PG_America_Rosario,                  40},
    {(char*)"America/Santa_Isabel",             PG_America_Santa_Isabel,             53},
    {(char*)"America/Santarem",                 PG_America_Santarem,                 62},
    {(char*)"America/Santiago",                 PG_America_Santiago,                 43},
    {(char*)"America/Santo_Domingo",            PG_America_Santo_Domingo,            26},
    {(char*)"America/Sao_Paulo",                PG_America_Sao_Paulo,                62},
    {(char*)"America/Scoresbysund",             PG_America_Scoresbysund,             28},
    {(char*)"America/Shiprock",                 PG_America_Shiprock,                 26},
    {(char*)"America/Sitka",                    PG_America_Sitka,                    22},
    {(char*)"America/St_Barthelemy",            PG_America_St_Barthelemy,            4 },
    {(char*)"America/St_Johns",                 PG_America_St_Johns,                 38},
    {(char*)"America/St_Kitts",                 PG_America_St_Kitts,                 4 },
    {(char*)"America/St_Lucia",                 PG_America_St_Lucia,                 4 },
    {(char*)"America/St_Thomas",                PG_America_St_Thomas,                4 },
    {(char*)"America/St_Vincent",               PG_America_St_Vincent,               4 },
    {(char*)"America/Swift_Current",            PG_America_Swift_Current,            38},
    {(char*)"America/Tegucigalpa",              PG_America_Tegucigalpa,              7 },
    {(char*)"America/Thule",                    PG_America_Thule,                    9 },
    {(char*)"America/Thunder_Bay",              PG_America_Thunder_Bay,              40},
    {(char*)"America/Tijuana",                  PG_America_Tijuana,                  52},
    {(char*)"America/Toronto",                  PG_America_Toronto,                  39},
    {(char*)"America/Tortola",                  PG_America_Tortola,                  4 },
    {(char*)"America/Vancouver",                PG_America_Vancouver,                23},
    {(char*)"America/Virgin",                   PG_America_Virgin,                   4 },
    {(char*)"America/Whitehorse",               PG_America_Whitehorse,               28},
    {(char*)"America/Winnipeg",                 PG_America_Winnipeg,                 38},
    {(char*)"America/Yakutat",                  PG_America_Yakutat,                  21},
    {(char*)"America/Yellowknife",              PG_America_Yellowknife,              26}
};

static local_tzdata_dispatch_struct antartica_tzdata_info[] = {
    {(char*)"Antarctica/Casey",          PG_Antarctica_Casey,          14},
    {(char*)"Antarctica/Davis",          PG_Antarctica_Davis,          9 },
    {(char*)"Antarctica/DumontDUrville", PG_Antarctica_DumontDUrville, 5 },
    {(char*)"Antarctica/Macquarie",      PG_Antarctica_Macquarie,      36},
    {(char*)"Antarctica/Mawson",         PG_Antarctica_Mawson,         4 },
    {(char*)"Antarctica/McMurdo",        PG_Antarctica_McMurdo,        21},
    {(char*)"Antarctica/Palmer",         PG_Antarctica_Palmer,         69},
    {(char*)"Antarctica/Rothera",        PG_Antarctica_Rothera,        3 },
    {(char*)"Antarctica/South_Pole",     PG_Antarctica_South_Pole,     18},
    {(char*)"Antarctica/Syowa",          PG_Antarctica_Syowa,          3 },
    {(char*)"Antarctica/Troll",          PG_Antarctica_Troll,          4 },
    {(char*)"Antarctica/Vostok",         PG_Antarctica_Vostok,         3 }
};

static local_tzdata_dispatch_struct arctic_tzdata_info[] = {
    {(char*)"Arctic/Longyearbyen", PG_Arctic_Longyearbyen, 37}
};

static local_tzdata_dispatch_struct asia_tzdata_info[] = {
    {(char*)"Asia/Aden",          PG_Asia_Aden,          4  },
    {(char*)"Asia/Almaty",        PG_Asia_Almaty,        12 },
    {(char*)"Asia/Amman",         PG_Asia_Amman,         34 },
    {(char*)"Asia/Anadyr",        PG_Asia_Anadyr,        25 },
    {(char*)"Asia/Aqtau",         PG_Asia_Aqtau,         15 },
    {(char*)"Asia/Aqtobe",        PG_Asia_Aqtobe,        15 },
    {(char*)"Asia/Ashgabat",      PG_Asia_Ashgabat,      11 },
    {(char*)"Asia/Ashkhabad",     PG_Asia_Ashkhabad,     12 },
    {(char*)"Asia/Atyrau",        PG_Asia_Atyrau,        15 },
    {(char*)"Asia/Baghdad",       PG_Asia_Baghdad,       13 },
    {(char*)"Asia/Bahrain",       PG_Asia_Bahrain,       5  },
    {(char*)"Asia/Baku",          PG_Asia_Baku,          18 },
    {(char*)"Asia/Bangkok",       PG_Asia_Bangkok,       4  },
    {(char*)"Asia/Barnaul",       PG_Asia_Barnaul,       26 },
    {(char*)"Asia/Beijing",       PG_Asia_Beijing,       5  },
    {(char*)"Asia/Beirut",        PG_Asia_Beirut,        27 },
    {(char*)"Asia/Bishkek",       PG_Asia_Bishkek,       16 },
    {(char*)"Asia/Brunei",        PG_Asia_Brunei,        4  },
    {(char*)"Asia/Calcutta",      PG_Asia_Calcutta,      10 },
    {(char*)"Asia/Choibalsan",    PG_Asia_Choibalsan,    15 },
    {(char*)"Asia/Chongqing",     PG_Asia_Chongqing,     22 },
    {(char*)"Asia/Chungking",     PG_Asia_Chungking,     22 },
    {(char*)"Asia/Colombo",       PG_Asia_Colombo,       10 },
    {(char*)"Asia/Dacca",         PG_Asia_Dacca,         11 },
    {(char*)"Asia/Damascus",      PG_Asia_Damascus,      44 },
    {(char*)"Asia/Dhaka",         PG_Asia_Dhaka,         11 },
    {(char*)"Asia/Dili",          PG_Asia_Dili,          6  },
    {(char*)"Asia/Dubai",         PG_Asia_Dubai,         3  },
    {(char*)"Asia/Dushanbe",      PG_Asia_Dushanbe,      11 },
    {(char*)"Asia/Famagusta",     PG_Asia_Famagusta,     18 },
    {(char*)"Asia/Gaza",          PG_Asia_Gaza,          161},
    {(char*)"Asia/Harbin",        PG_Asia_Harbin,        22 },
    {(char*)"Asia/Hebron",        PG_Asia_Hebron,        155},
    {(char*)"Asia/Ho_Chi_Minh",   PG_Asia_Ho_Chi_Minh,   11 },
    {(char*)"Asia/Hong_Kong",     PG_Asia_Hong_Kong,     21 },
    {(char*)"Asia/Hovd",          PG_Asia_Hovd,          13 },
    {(char*)"Asia/Irkutsk",       PG_Asia_Irkutsk,       25 },
    {(char*)"Asia/Istanbul",      PG_Asia_Istanbul,      66 },
    {(char*)"Asia/Jakarta",       PG_Asia_Jakarta,       10 },
    {(char*)"Asia/Jayapura",      PG_Asia_Jayapura,      5  },
    {(char*)"Asia/Jerusalem",     PG_Asia_Jerusalem,     91 },
    {(char*)"Asia/Kabul",         PG_Asia_Kabul,         4  },
    {(char*)"Asia/Kamchatka",     PG_Asia_Kamchatka,     24 },
    {(char*)"Asia/Karachi",       PG_Asia_Karachi,       12 },
    {(char*)"Asia/Kashgar",       PG_Asia_Kashgar,       8  },
    {(char*)"Asia/Kathmandu",     PG_Asia_Kathmandu,     4  },
    {(char*)"Asia/Katmandu",      PG_Asia_Katmandu,      5  },
    {(char*)"Asia/Khandyga",      PG_Asia_Khandyga,      26 },
    {(char*)"Asia/Kolkata",       PG_Asia_Kolkata,       10 },
    {(char*)"Asia/Krasnoyarsk",   PG_Asia_Krasnoyarsk,   24 },
    {(char*)"Asia/Kuala_Lumpur",  PG_Asia_Kuala_Lumpur,  10 },
    {(char*)"Asia/Kuching",       PG_Asia_Kuching,       8  },
    {(char*)"Asia/Kuwait",        PG_Asia_Kuwait,        4  },
    {(char*)"Asia/Macao",         PG_Asia_Macao,         33 },
    {(char*)"Asia/Macau",         PG_Asia_Macau,         32 },
    {(char*)"Asia/Magadan",       PG_Asia_Magadan,       25 },
    {(char*)"Asia/Makassar",      PG_Asia_Makassar,      6  },
    {(char*)"Asia/Manila",        PG_Asia_Manila,        12 },
    {(char*)"Asia/Muscat",        PG_Asia_Muscat,        4  },
    {(char*)"Asia/Nicosia",       PG_Asia_Nicosia,       16 },
    {(char*)"Asia/Novokuznetsk",  PG_Asia_Novokuznetsk,  24 },
    {(char*)"Asia/Novosibirsk",   PG_Asia_Novosibirsk,   26 },
    {(char*)"Asia/Omsk",          PG_Asia_Omsk,          24 },
    {(char*)"Asia/Oral",          PG_Asia_Oral,          16 },
    {(char*)"Asia/Phnom_Penh",    PG_Asia_Phnom_Penh,    5  },
    {(char*)"Asia/Pontianak",     PG_Asia_Pontianak,     10 },
    {(char*)"Asia/Pyongyang",     PG_Asia_Pyongyang,     7  },
    {(char*)"Asia/Qatar",         PG_Asia_Qatar,         4  },
    {(char*)"Asia/Qostanay",      PG_Asia_Qostanay,      15 },
    {(char*)"Asia/Qyzylorda",     PG_Asia_Qyzylorda,     18 },
    {(char*)"Asia/Rangoon",       PG_Asia_Rangoon,       6  },
    {(char*)"Asia/Riyadh",        PG_Asia_Riyadh,        3  },
    {(char*)"Asia/Riyadh87",      PG_Asia_Riyadh87,      369},
    {(char*)"Asia/Riyadh88",      PG_Asia_Riyadh88,      370},
    {(char*)"Asia/Riyadh89",      PG_Asia_Riyadh89,      369},
    {(char*)"Asia/Saigon",        PG_Asia_Saigon,        12 },
    {(char*)"Asia/Sakhalin",      PG_Asia_Sakhalin,      26 },
    {(char*)"Asia/Samarkand",     PG_Asia_Samarkand,     13 },
    {(char*)"Asia/Seoul",         PG_Asia_Seoul,         21 },
    {(char*)"Asia/Shanghai",      PG_Asia_Shanghai,      21 },
    {(char*)"Asia/Singapore",     PG_Asia_Singapore,     10 },
    {(char*)"Asia/Srednekolymsk", PG_Asia_Srednekolymsk, 24 },
    {(char*)"Asia/Taipei",        PG_Asia_Taipei,        20 },
    {(char*)"Asia/Tashkent",      PG_Asia_Tashkent,      11 },
    {(char*)"Asia/Tbilisi",       PG_Asia_Tbilisi,       20 },
    {(char*)"Asia/Tehran",        PG_Asia_Tehran,        107},
    {(char*)"Asia/Tel_Aviv",      PG_Asia_Tel_Aviv,      92 },
    {(char*)"Asia/Thimbu",        PG_Asia_Thimbu,        5  },
    {(char*)"Asia/Thimphu",       PG_Asia_Thimphu,       4  },
    {(char*)"Asia/Tokyo",         PG_Asia_Tokyo,         7  },
    {(char*)"Asia/Tomsk",         PG_Asia_Tomsk,         26 },
    {(char*)"Asia/Ujung_Pandang", PG_Asia_Ujung_Pandang, 7  },
    {(char*)"Asia/Ulaanbaatar",   PG_Asia_Ulaanbaatar,   13 },
    {(char*)"Asia/Ulan_Bator",    PG_Asia_Ulan_Bator,    14 },
    {(char*)"Asia/Urumqi",        PG_Asia_Urumqi,        7  },
    {(char*)"Asia/Ust-Nera",      PG_Asia_Ust_Nera,      26 },
    {(char*)"Asia/Vientiane",     PG_Asia_Vientiane,     5  },
    {(char*)"Asia/Vladivostok",   PG_Asia_Vladivostok,   24 },
    {(char*)"Asia/Yakutsk",       PG_Asia_Yakutsk,       24 },
    {(char*)"Asia/Yangon",        PG_Asia_Yangon,        6  },
    {(char*)"Asia/Yekaterinburg", PG_Asia_Yekaterinburg, 25 },
    {(char*)"Asia/Yerevan",       PG_Asia_Yerevan,       15 }
};

static local_tzdata_dispatch_struct atlantic_tzdata_info[] = {
    {(char*)"Atlantic/Azores",        PG_Atlantic_Azores,        70},
    {(char*)"Atlantic/Bermuda",       PG_Atlantic_Bermuda,       28},
    {(char*)"Atlantic/Canary",        PG_Atlantic_Canary,        12},
    {(char*)"Atlantic/Cape_Verde",    PG_Atlantic_Cape_Verde,    6 },
    {(char*)"Atlantic/Faeroe",        PG_Atlantic_Faeroe,        11},
    {(char*)"Atlantic/Faroe",         PG_Atlantic_Faroe,         10},
    {(char*)"Atlantic/Jan_Mayen",     PG_Atlantic_Jan_Mayen,     37},
    {(char*)"Atlantic/Madeira",       PG_Atlantic_Madeira,       62},
    {(char*)"Atlantic/Reykjavik",     PG_Atlantic_Reykjavik,     20},
    {(char*)"Atlantic/South_Georgia", PG_Atlantic_South_Georgia, 3 },
    {(char*)"Atlantic/Stanley",       PG_Atlantic_Stanley,       19},
    {(char*)"Atlantic/St_Helena",     PG_Atlantic_St_Helena,     4 }
};

static local_tzdata_dispatch_struct australia_tzdata_info[] = {
    {(char*)"Australia/ACT",         PG_Australia_ACT,         28},
    {(char*)"Australia/Adelaide",    PG_Australia_Adelaide,    27},
    {(char*)"Australia/Brisbane",    PG_Australia_Brisbane,    15},
    {(char*)"Australia/Broken_Hill", PG_Australia_Broken_Hill, 45},
    {(char*)"Australia/Canberra",    PG_Australia_Canberra,    28},
    {(char*)"Australia/Currie",      PG_Australia_Currie,      32},
    {(char*)"Australia/Darwin",      PG_Australia_Darwin,      11},
    {(char*)"Australia/Eucla",       PG_Australia_Eucla,       20},
    {(char*)"Australia/Hobart",      PG_Australia_Hobart,      32},
    {(char*)"Australia/LHI",         PG_Australia_LHI,         20},
    {(char*)"Australia/Lindeman",    PG_Australia_Lindeman,    18},
    {(char*)"Australia/Lord_Howe",   PG_Australia_Lord_Howe,   19},
    {(char*)"Australia/Melbourne",   PG_Australia_Melbourne,   25},
    {(char*)"Australia/North",       PG_Australia_North,       12},
    {(char*)"Australia/NSW",         PG_Australia_NSW,         28},
    {(char*)"Australia/Perth",       PG_Australia_Perth,       20},
    {(char*)"Australia/Queensland",  PG_Australia_Queensland,  16},
    {(char*)"Australia/South",       PG_Australia_South,       28},
    {(char*)"Australia/Sydney",      PG_Australia_Sydney,      27},
    {(char*)"Australia/Tasmania",    PG_Australia_Tasmania,    33},
    {(char*)"Australia/Victoria",    PG_Australia_Victoria,    26},
    {(char*)"Australia/West",        PG_Australia_West,        21},
    {(char*)"Australia/Yancowinna",  PG_Australia_Yancowinna,  46}
};

static local_tzdata_dispatch_struct brazil_tzdata_info[] = {
    {(char*)"Brazil/Acre",      PG_Brazil_Acre,      64},
    {(char*)"Brazil/DeNoronha", PG_Brazil_DeNoronha, 66},
    {(char*)"Brazil/East",      PG_Brazil_East,      63},
    {(char*)"Brazil/West",      PG_Brazil_West,      64}
};

static local_tzdata_dispatch_struct canada_tzdata_info[] = {
    {(char*)"Canada/Atlantic",     PG_Canada_Atlantic,     60},
    {(char*)"Canada/Central",      PG_Canada_Central,      39},
    {(char*)"Canada/Eastern",      PG_Canada_Eastern,      40},
    {(char*)"Canada/Mountain",     PG_Canada_Mountain,     28},
    {(char*)"Canada/Newfoundland", PG_Canada_Newfoundland, 39},
    {(char*)"Canada/Pacific",      PG_Canada_Pacific,      24},
    {(char*)"Canada/Saskatchewan", PG_Canada_Saskatchewan, 22},
    {(char*)"Canada/Yukon",        PG_Canada_Yukon,        29}
};

static local_tzdata_dispatch_struct chile_tzdata_info[] = {
    {(char*)"Chile/Continental",  PG_Chile_Continental,  44},
    {(char*)"Chile/EasterIsland", PG_Chile_EasterIsland, 40}
};

static local_tzdata_dispatch_struct etc_tzdata_info[] = {
    {(char*)"Etc/GMT",       PG_Etc_GMT,         2},
    {(char*)"Etc/GMT0",      PG_Etc_GMT0,        3},
    {(char*)"Etc/GMT-0",     PG_Etc_GMT_dash_0,  3},
    {(char*)"Etc/GMT+0",     PG_Etc_GMT_plus_0,  3},
    {(char*)"Etc/GMT-1",     PG_Etc_GMT_dash_1,  2},
    {(char*)"Etc/GMT+1",     PG_Etc_GMT_plus_1,  2},
    {(char*)"Etc/GMT-10",    PG_Etc_GMT_dash_10, 2},
    {(char*)"Etc/GMT+10",    PG_Etc_GMT_plus_10, 2},
    {(char*)"Etc/GMT-11",    PG_Etc_GMT_dash_11, 2},
    {(char*)"Etc/GMT+11",    PG_Etc_GMT_plus_11, 2},
    {(char*)"Etc/GMT-12",    PG_Etc_GMT_dash_12, 2},
    {(char*)"Etc/GMT+12",    PG_Etc_GMT_plus_12, 2},
    {(char*)"Etc/GMT-13",    PG_Etc_GMT_dash_13, 2},
    {(char*)"Etc/GMT-14",    PG_Etc_GMT_dash_14, 2},
    {(char*)"Etc/GMT-2",     PG_Etc_GMT_dash_2,  2},
    {(char*)"Etc/GMT+2",     PG_Etc_GMT_plus_2,  2},
    {(char*)"Etc/GMT-3",     PG_Etc_GMT_dash_3,  2},
    {(char*)"Etc/GMT+3",     PG_Etc_GMT_plus_3,  2},
    {(char*)"Etc/GMT-4",     PG_Etc_GMT_dash_4,  2},
    {(char*)"Etc/GMT+4",     PG_Etc_GMT_plus_4,  2},
    {(char*)"Etc/GMT-5",     PG_Etc_GMT_dash_5,  2},
    {(char*)"Etc/GMT+5",     PG_Etc_GMT_plus_5,  2},
    {(char*)"Etc/GMT-6",     PG_Etc_GMT_dash_6,  2},
    {(char*)"Etc/GMT+6",     PG_Etc_GMT_plus_6,  2},
    {(char*)"Etc/GMT-7",     PG_Etc_GMT_dash_7,  2},
    {(char*)"Etc/GMT+7",     PG_Etc_GMT_plus_7,  2},
    {(char*)"Etc/GMT-8",     PG_Etc_GMT_dash_8,  2},
    {(char*)"Etc/GMT+8",     PG_Etc_GMT_plus_8,  2},
    {(char*)"Etc/GMT-9",     PG_Etc_GMT_dash_9,  2},
    {(char*)"Etc/GMT+9",     PG_Etc_GMT_plus_9,  2},
    {(char*)"Etc/Greenwich", PG_Etc_Greenwich,   3},
    {(char*)"Etc/UCT",       PG_Etc_UCT,         3},
    {(char*)"Etc/Universal", PG_Etc_Universal,   3},
    {(char*)"Etc/UTC",       PG_Etc_UTC,         2},
    {(char*)"Etc/Zulu",      PG_Etc_Zulu,        2}
};

static local_tzdata_dispatch_struct europe_tzdata_info[] = {
    {(char*)"Europe/Amsterdam",   PG_Europe_Amsterdam,   50},
    {(char*)"Europe/Andorra",     PG_Europe_Andorra,     11},
    {(char*)"Europe/Astrakhan",   PG_Europe_Astrakhan,   26},
    {(char*)"Europe/Athens",      PG_Europe_Athens,      34},
    {(char*)"Europe/Belfast",     PG_Europe_Belfast,     78},
    {(char*)"Europe/Belgrade",    PG_Europe_Belgrade,    31},
    {(char*)"Europe/Berlin",      PG_Europe_Berlin,      40},
    {(char*)"Europe/Bratislava",  PG_Europe_Bratislava,  38},
    {(char*)"Europe/Brussels",    PG_Europe_Brussels,    67},
    {(char*)"Europe/Bucharest",   PG_Europe_Bucharest,   46},
    {(char*)"Europe/Budapest",    PG_Europe_Budapest,    49},
    {(char*)"Europe/Busingen",    PG_Europe_Busingen,    14},
    {(char*)"Europe/Chisinau",    PG_Europe_Chisinau,    61},
    {(char*)"Europe/Copenhagen",  PG_Europe_Copenhagen,  41},
    {(char*)"Europe/Dublin",      PG_Europe_Dublin,      84},
    {(char*)"Europe/Gibraltar",   PG_Europe_Gibraltar,   76},
    {(char*)"Europe/Guernsey",    PG_Europe_Guernsey,    78},
    {(char*)"Europe/Helsinki",    PG_Europe_Helsinki,    15},
    {(char*)"Europe/Isle_of_Man", PG_Europe_Isle_of_Man, 78},
    {(char*)"Europe/Istanbul",    PG_Europe_Istanbul,    65},
    {(char*)"Europe/Jersey",      PG_Europe_Jersey,      78},
    {(char*)"Europe/Kaliningrad", PG_Europe_Kaliningrad, 61},
    {(char*)"Europe/Kiev",        PG_Europe_Kiev,        54},
    {(char*)"Europe/Kirov",       PG_Europe_Kirov,       24},
    {(char*)"Europe/Lisbon",      PG_Europe_Lisbon,      71},
    {(char*)"Europe/Ljubljana",   PG_Europe_Ljubljana,   32},
    {(char*)"Europe/London",      PG_Europe_London,      77},
    {(char*)"Europe/Luxembourg",  PG_Europe_Luxembourg,  88},
    {(char*)"Europe/Madrid",      PG_Europe_Madrid,      41},
    {(char*)"Europe/Malta",       PG_Europe_Malta,       59},
    {(char*)"Europe/Mariehamn",   PG_Europe_Mariehamn,   16},
    {(char*)"Europe/Minsk",       PG_Europe_Minsk,       43},
    {(char*)"Europe/Monaco",      PG_Europe_Monaco,      48},
    {(char*)"Europe/Moscow",      PG_Europe_Moscow,      28},
    {(char*)"Europe/Nicosia",     PG_Europe_Nicosia,     17},
    {(char*)"Europe/Oslo",        PG_Europe_Oslo,        36},
    {(char*)"Europe/Paris",       PG_Europe_Paris,       73},
    {(char*)"Europe/Podgorica",   PG_Europe_Podgorica,   32},
    {(char*)"Europe/Prague",      PG_Europe_Prague,      37},
    {(char*)"Europe/Riga",        PG_Europe_Riga,        57},
    {(char*)"Europe/Rome",        PG_Europe_Rome,        71},
    {(char*)"Europe/Samara",      PG_Europe_Samara,      27},
    {(char*)"Europe/San_Marino",  PG_Europe_San_Marino,  72},
    {(char*)"Europe/Sarajevo",    PG_Europe_Sarajevo,    32},
    {(char*)"Europe/Saratov",     PG_Europe_Saratov,     26},
    {(char*)"Europe/Simferopol",  PG_Europe_Simferopol,  62},
    {(char*)"Europe/Skopje",      PG_Europe_Skopje,      32},
    {(char*)"Europe/Sofia",       PG_Europe_Sofia,       45},
    {(char*)"Europe/Stockholm",   PG_Europe_Stockholm,   13},
    {(char*)"Europe/Tallinn",     PG_Europe_Tallinn,     53},
    {(char*)"Europe/Tirane",      PG_Europe_Tirane,      36},
    {(char*)"Europe/Tiraspol",    PG_Europe_Tiraspol,    62},
    {(char*)"Europe/Uzhgorod",    PG_Europe_Uzhgorod,    57},
    {(char*)"Europe/Vaduz",       PG_Europe_Vaduz,       14},
    {(char*)"Europe/Vatican",     PG_Europe_Vatican,     72},
    {(char*)"Europe/Vienna",      PG_Europe_Vienna,      41},
    {(char*)"Europe/Vilnius",     PG_Europe_Vilnius,     55},
    {(char*)"Europe/Volgograd",   PG_Europe_Volgograd,   27},
    {(char*)"Europe/Warsaw",      PG_Europe_Warsaw,      59},
    {(char*)"Europe/Zagreb",      PG_Europe_Zagreb,      32},
    {(char*)"Europe/Zaporozhye",  PG_Europe_Zaporozhye,  54},
    {(char*)"Europe/Zurich",      PG_Europe_Zurich,      13}
};

static local_tzdata_dispatch_struct indian_tzdata_info[] = {
    {(char*)"Indian/Antananarivo", PG_Indian_Antananarivo, 7},
    {(char*)"Indian/Chagos",       PG_Indian_Chagos,       4},
    {(char*)"Indian/Christmas",    PG_Indian_Christmas,    3},
    {(char*)"Indian/Cocos",        PG_Indian_Cocos,        3},
    {(char*)"Indian/Comoro",       PG_Indian_Comoro,       7},
    {(char*)"Indian/Kerguelen",    PG_Indian_Kerguelen,    3},
    {(char*)"Indian/Mahe",         PG_Indian_Mahe,         3},
    {(char*)"Indian/Maldives",     PG_Indian_Maldives,     4},
    {(char*)"Indian/Mauritius",    PG_Indian_Mauritius,    7},
    {(char*)"Indian/Mayotte",      PG_Indian_Mayotte,      7},
    {(char*)"Indian/Reunion",      PG_Indian_Reunion,      3}
};

static local_tzdata_dispatch_struct mexico_tzdata_info[] = {
    {(char*)"Mexico/BajaNorte", PG_Mexico_BajaNorte, 53},
    {(char*)"Mexico/BajaSur",   PG_Mexico_BajaSur,   26},
    {(char*)"Mexico/General",   PG_Mexico_General,   25}
};

static local_tzdata_dispatch_struct mideast_tzdata_info[] = {
    {(char*)"Mideast/Riyadh87", PG_Mideast_Riyadh87, 370},
    {(char*)"Mideast/Riyadh88", PG_Mideast_Riyadh88, 371},
    {(char*)"Mideast/Riyadh89", PG_Mideast_Riyadh89, 370}
};

static local_tzdata_dispatch_struct pacific_tzdata_info[] = {
    {(char*)"Pacific/Apia",         PG_Pacific_Apia,         11},
    {(char*)"Pacific/Auckland",     PG_Pacific_Auckland,     20},
    {(char*)"Pacific/Chatham",      PG_Pacific_Chatham,      12},
    {(char*)"Pacific/Chuuk",        PG_Pacific_Chuuk,        8 },
    {(char*)"Pacific/Easter",       PG_Pacific_Easter,       39},
    {(char*)"Pacific/Efate",        PG_Pacific_Efate,        9 },
    {(char*)"Pacific/Enderbury",    PG_Pacific_Enderbury,    5 },
    {(char*)"Pacific/Fakaofo",      PG_Pacific_Fakaofo,      4 },
    {(char*)"Pacific/Fiji",         PG_Pacific_Fiji,         14},
    {(char*)"Pacific/Funafuti",     PG_Pacific_Funafuti,     3 },
    {(char*)"Pacific/Galapagos",    PG_Pacific_Galapagos,    6 },
    {(char*)"Pacific/Gambier",      PG_Pacific_Gambier,      3 },
    {(char*)"Pacific/Guadalcanal",  PG_Pacific_Guadalcanal,  3 },
    {(char*)"Pacific/Guam",         PG_Pacific_Guam,         21},
    {(char*)"Pacific/Honolulu",     PG_Pacific_Honolulu,     19},
    {(char*)"Pacific/Johnston",     PG_Pacific_Johnston,     20},
    {(char*)"Pacific/Kiritimati",   PG_Pacific_Kiritimati,   5 },
    {(char*)"Pacific/Kosrae",       PG_Pacific_Kosrae,       11},
    {(char*)"Pacific/Kwajalein",    PG_Pacific_Kwajalein,    8 },
    {(char*)"Pacific/Majuro",       PG_Pacific_Majuro,       9 },
    {(char*)"Pacific/Marquesas",    PG_Pacific_Marquesas,    3 },
    {(char*)"Pacific/Midway",       PG_Pacific_Midway,       5 },
    {(char*)"Pacific/Nauru",        PG_Pacific_Nauru,        6 },
    {(char*)"Pacific/Niue",         PG_Pacific_Niue,         5 },
    {(char*)"Pacific/Norfolk",      PG_Pacific_Norfolk,      24},
    {(char*)"Pacific/Noumea",       PG_Pacific_Noumea,       7 },
    {(char*)"Pacific/Pago_Pago",    PG_Pacific_Pago_Pago,    4 },
    {(char*)"Pacific/Palau",        PG_Pacific_Palau,        4 },
    {(char*)"Pacific/Pitcairn",     PG_Pacific_Pitcairn,     4 },
    {(char*)"Pacific/Pohnpei",      PG_Pacific_Pohnpei,      9 },
    {(char*)"Pacific/Ponape",       PG_Pacific_Ponape,       10},
    {(char*)"Pacific/Port_Moresby", PG_Pacific_Port_Moresby, 4 },
    {(char*)"Pacific/Rarotonga",    PG_Pacific_Rarotonga,    7 },
    {(char*)"Pacific/Saipan",       PG_Pacific_Saipan,       22},
    {(char*)"Pacific/Samoa",        PG_Pacific_Samoa,        5 },
    {(char*)"Pacific/Tahiti",       PG_Pacific_Tahiti,       3 },
    {(char*)"Pacific/Tarawa",       PG_Pacific_Tarawa,       3 },
    {(char*)"Pacific/Tongatapu",    PG_Pacific_Tongatapu,    11},
    {(char*)"Pacific/Truk",         PG_Pacific_Truk,         9 },
    {(char*)"Pacific/Wake",         PG_Pacific_Wake,         3 },
    {(char*)"Pacific/Wallis",       PG_Pacific_Wallis,       3 },
    {(char*)"Pacific/Yap",          PG_Pacific_Yap,          9 }
};

static local_tzdata_dispatch_struct us_tzdata_info[] = {
    {(char*)"US/Alaska",         PG_US_Alaska,         23},
    {(char*)"US/Aleutian",       PG_US_Aleutian,       24},
    {(char*)"US/Arizona",        PG_US_Arizona,        22},
    {(char*)"US/Central",        PG_US_Central,        29},
    {(char*)"US/Eastern",        PG_US_Eastern,        26},
    {(char*)"US/East-Indiana",   PG_US_East_Indiana,   29},
    {(char*)"US/Hawaii",         PG_US_Hawaii,         20},
    {(char*)"US/Indiana-Starke", PG_US_Indiana_Starke, 27},
    {(char*)"US/Michigan",       PG_US_Michigan,       27},
    {(char*)"US/Mountain",       PG_US_Mountain,       26},
    {(char*)"US/Pacific",        PG_US_Pacific,        24},
    {(char*)"US/Pacific-New",    PG_US_Pacific_New,    24},
    {(char*)"US/Samoa",          PG_US_Samoa,          5 }
};

static local_tzdata_dispatch_struct top_tzdata_info[] = {
    {(char*)"CET",        PG_CET,        19 },
    {(char*)"CST6CDT",    PG_CST6CDT,    15 },
    {(char*)"Cuba",       PG_Cuba,       44 },
    {(char*)"EET",        PG_EET,        8  },
    {(char*)"Egypt",      PG_Egypt,      36 },
    {(char*)"Eire",       PG_Eire,       85 },
    {(char*)"EST",        PG_EST,        2  },
    {(char*)"EST5EDT",    PG_EST5EDT,    15 },
    {(char*)"Factory",    PG_Factory,    2  },
    {(char*)"GB",         PG_GB,         78 },
    {(char*)"GB-Eire",    PG_GB_Eire,    78 },
    {(char*)"GMT",        PG_GMT,        3  },
    {(char*)"GMT0",       PG_GMT0,       3  },
    {(char*)"GMT-0",      PG_GMT_dash_0, 3  },
    {(char*)"GMT+0",      PG_GMT_plus_0, 3  },
    {(char*)"Greenwich",  PG_Greenwich,  3  },
    {(char*)"Hongkong",   PG_Hongkong,   22 },
    {(char*)"HST",        PG_HST,        2  },
    {(char*)"Iceland",    PG_Iceland,    21 },
    {(char*)"Iran",       PG_Iran,       108},
    {(char*)"Israel",     PG_Israel,     92 },
    {(char*)"Jamaica",    PG_Jamaica,    20 },
    {(char*)"Japan",      PG_Japan,      8  },
    {(char*)"Kwajalein",  PG_Kwajalein,  9  },
    {(char*)"Libya",      PG_Libya,      28 },
    {(char*)"MET",        PG_MET,        19 },
    {(char*)"MST",        PG_MST,        2  },
    {(char*)"MST7MDT",    PG_MST7MDT,    15 },
    {(char*)"Navajo",     PG_Navajo,     26 },
    {(char*)"NZ",         PG_NZ,         21 },
    {(char*)"NZ-CHAT",    PG_NZ_CHAT,    13 },
    {(char*)"Poland",     PG_Poland,     60 },
    {(char*)"Portugal",   PG_Portugal,   72 },
    {(char*)"posixrules", PG_posixrules, 1  },
    {(char*)"PRC",        PG_PRC,        22 },
    {(char*)"PST8PDT",    PG_PST8PDT,    15 },
    {(char*)"ROC",        PG_ROC,        21 },
    {(char*)"ROK",        PG_ROK,        22 },
    {(char*)"Singapore",  PG_Singapore,  11 },
    {(char*)"Turkey",     PG_Turkey,     66 },
    {(char*)"UCT",        PG_UCT,        3  },
    {(char*)"Universal",  PG_Universal,  3  },
    {(char*)"UTC",        PG_UTC,        3  },
    {(char*)"WET",        PG_WET,        8  },
    {(char*)"W-SU",       PG_W_SU,       29 },
    {(char*)"Zulu",       PG_Zulu,       3  }
};

char** pg_parser_get_tzdata_info(const char* tz_name, int32_t* tz_dataSize)
{
    local_tzdata_dispatch_struct* entry = NULL;
    size_t                        i = 0, entrySize = 0;

    if (0 == strncmp(tz_name, "Africa", 6))
    {
        entry = africa_tzdata_info;
        entrySize = sizeof(africa_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "America", 7))
    {
        entry = america_tzdata_info;
        entrySize = sizeof(america_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Antarctica", 10))
    {
        entry = antartica_tzdata_info;
        entrySize = sizeof(antartica_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Arctic", 6))
    {
        entry = arctic_tzdata_info;
        entrySize = sizeof(arctic_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Asia", 4))
    {
        entry = asia_tzdata_info;
        entrySize = sizeof(asia_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Atlantic", 8))
    {
        entry = atlantic_tzdata_info;
        entrySize = sizeof(atlantic_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Australia", 9))
    {
        entry = australia_tzdata_info;
        entrySize = sizeof(australia_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Brazil", 6))
    {
        entry = brazil_tzdata_info;
        entrySize = sizeof(brazil_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Canada", 6))
    {
        entry = canada_tzdata_info;
        entrySize = sizeof(canada_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Chile", 5))
    {
        entry = chile_tzdata_info;
        entrySize = sizeof(chile_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Etc", 3))
    {
        entry = etc_tzdata_info;
        entrySize = sizeof(etc_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Europe", 6))
    {
        entry = europe_tzdata_info;
        entrySize = sizeof(europe_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Indian", 6))
    {
        entry = indian_tzdata_info;
        entrySize = sizeof(indian_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Mexico", 6))
    {
        entry = mexico_tzdata_info;
        entrySize = sizeof(mexico_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Mideast", 7))
    {
        entry = mideast_tzdata_info;
        entrySize = sizeof(mideast_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Pacific", 7))
    {
        entry = pacific_tzdata_info;
        entrySize = sizeof(pacific_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "US", 2))
    {
        entry = us_tzdata_info;
        entrySize = sizeof(us_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else
    {
        entry = top_tzdata_info;
        entrySize = sizeof(top_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }

    for (i = 0; i < entrySize; ++i)
    {
        if (0 == strcmp(tz_name, entry[i].tz_name))
        {
            *tz_dataSize = entry[i].tz_data_size;
            return entry[i].tz_data;
        }
    }

    return NULL;
}
