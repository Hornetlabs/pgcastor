#include "app_incl.h"
#include "utils/init/init.h"
#include "command/cmd.h"

bool cmd_init(void *extra_config)
{
    UNUSED(extra_config);
    return init();
}
