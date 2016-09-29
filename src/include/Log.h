#ifndef FDS_BLOCK_CONNECTORS_LOG_H_
#define FDS_BLOCK_CONNECTORS_LOG_H_

#include <iostream>
extern "C" {
#include <assert.h>
}

#define LOGTRACE    std::cerr
#define LOGDEBUG    std::cerr
#define LOGIO       std::cerr
#define LOGNORMAL   std::cerr
#define LOGWARN     std::cerr
#define LOGERROR    std::cerr
#define LOGNOTIFY   std::cerr
#define LOGCRITICAL std::cerr

#define GLOGTRACE       LOGTRACE
#define GLOGDEBUG       LOGDEBUG
#define GLOGIO          LOGIO
#define GLOGNORMAL      LOGNORMAL
#define GLOGWARN        LOGWARN
#define GLOGERROR       LOGERROR
#define GLOGNOTIFY      LOGNOTIFY
#define GLOGCRITICAL    LOGCRITICAL

#endif  // FDS_BLOCK_CONNECTORS_LOG_H_
