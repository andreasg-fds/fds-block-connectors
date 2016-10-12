/**
 * Copyright 2016 by Formation Data Systems, Inc.
 */

#define BOOST_LOG_DYN_LINK 1

#ifndef LOG_LOG_H_
#define LOG_LOG_H_

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

#ifdef test
#undef test
#endif

#include <fstream>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/lockfree/detail/branch_hints.hpp>

#ifndef DONTLOGLINE
#define _ATLINE_ << "[" __FILE__ << ":" << std::dec << __LINE__ << ":" << __FUNCTION__ << "] - "
#else
#define _ATLINE_
#endif

#define LOG_LOCATION "/fds/var/logs/"

#define FDS_LOG(lg) BOOST_LOG_SEV(lg.get_slog(), xdi::fds_log::debug)
#define FDS_LOG_SEV(lg, sev) BOOST_LOG_SEV(lg.get_slog(), sev)
#define FDS_PLOG(lg_ptr) BOOST_LOG_SEV(lg_ptr->get_slog(), xdi::fds_log::debug)
#define FDS_PLOG_SEV(lg_ptr, sev) BOOST_LOG_SEV(lg_ptr->get_slog(), sev)
#define FDS_PLOG_COMMON(lg_ptr, sev) BOOST_LOG_SEV(lg_ptr->get_slog(), sev) << __FUNCTION__ << " " << __LINE__ << " " << log_string() << " "
#define FDS_PLOG_INFO(lg_ptr) FDS_PLOG_COMMON(lg_ptr, xdi::fds_log::normal)
#define FDS_PLOG_WARN(lg_ptr) FDS_PLOG_COMMON(lg_ptr, xdi::fds_log::warning)
#define FDS_PLOG_ERR(lg_ptr)  FDS_PLOG_COMMON(lg_ptr, xdi::fds_log::error)

//For classes that expose the GetLog() fn .
#define LOGGERPTR  GetLog()
// For static functions
#define GLOGGERPTR  xdi::GetLog()
// incase your current logger is different fom GetLog(),
// redefine macro [LOGGERPTR] at the top of your cpp file

#define LEVELCHECK(sev) if (LOGGERPTR->getSeverityLevel()<= xdi::fds_log::sev)
#define GLEVELCHECK(sev) if (GLOGGERPTR->getSeverityLevel()<= xdi::fds_log::sev)

#define LOGTRACE    LEVELCHECK(trace)        FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::trace)        _ATLINE_
#define LOGDEBUG    LEVELCHECK(debug)        FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::debug)        _ATLINE_
#define LOGMIGRATE  LEVELCHECK(migrate)      FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::migrate)      _ATLINE_
#define LOGIO       LEVELCHECK(io)           FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::io)           _ATLINE_
#define LOGNORMAL   LEVELCHECK(normal)       FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::normal)       _ATLINE_
#define LOGNOTIFY   LEVELCHECK(notification) FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::notification) _ATLINE_
#define LOGWARN     LEVELCHECK(warning)      FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::warning)      _ATLINE_
#define LOGERROR    LEVELCHECK(error)        FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::error)        _ATLINE_
#define LOGCRITICAL LEVELCHECK(critical)     FDS_PLOG_SEV(LOGGERPTR, xdi::fds_log::critical)     _ATLINE_

// for static functions inside classes
#define GLOGTRACE    GLEVELCHECK(trace)        FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::trace)        _ATLINE_
#define GLOGDEBUG    GLEVELCHECK(debug)        FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::debug)        _ATLINE_
#define GLOGMIGRATE  GLEVELCHECK(migrate)      FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::migrate)      _ATLINE_
#define GLOGIO       GLEVELCHECK(io)           FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::io)           _ATLINE_
#define GLOGNORMAL   GLEVELCHECK(normal)       FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::normal)       _ATLINE_
#define GLOGNOTIFY   GLEVELCHECK(notification) FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::notification) _ATLINE_
#define GLOGWARN     GLEVELCHECK(warning)      FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::warning)      _ATLINE_
#define GLOGERROR    GLEVELCHECK(error)        FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::error)        _ATLINE_
#define GLOGCRITICAL GLEVELCHECK(critical)     FDS_PLOG_SEV(GLOGGERPTR, xdi::fds_log::critical)     _ATLINE_

// #define FUNCTRACING

#ifdef FUNCTRACING
#define TRACEFUNC fds::__TRACER__ __tt__(__PRETTY_FUNCTION__, __FILE__, __LINE__);
#else
#define TRACEFUNC
#endif

namespace xdi {
struct __TRACER__ {
    __TRACER__(const std::string& prettyName, const std::string& filename, int lineno);
    ~__TRACER__();
    const std::string prettyName;
    const std::string filename;
    int lineno;
};

std::string cleanNameFromPrettyFunc(const std::string& prettyFunction, bool fClassOnly = false);
class fds_log {
  public:
    enum severity_level {
        trace,
        debug,
        migrate,
        io,
        normal,
        notification,
        warning,
        error,
        critical
    };

    enum log_options {
        record,
        pid,
        pname,
        tid
    };

  private:
    typedef boost::log::sinks::synchronous_sink< boost::log::sinks::text_ostream_backend > text_sink;
    typedef boost::log::sinks::synchronous_sink< boost::log::sinks::text_file_backend > file_sink;

    boost::shared_ptr< file_sink > sink;
    boost::log::sources::severity_logger_mt< severity_level > slg;

    void init(const std::string& logfile,
              const std::string& logloc,
              bool timestamp,
              bool severity,
              severity_level level,
              bool pname,
              bool pid,
              bool tid,
              bool record);

  public:

    /*
     * Constructs new log in specific location.
     */
    explicit fds_log(const std::string& logfile = "fds",
                     const std::string& logloc  = "",
                     severity_level level       = normal);

    ~fds_log();

    static severity_level getLevelFromName(std::string level);

    void setSeverityFilter(const severity_level &level);
    inline severity_level getSeverityLevel() { return severityLevel; }
    boost::log::sources::severity_logger_mt<severity_level>& get_slog() { return slg; }

    void flush() { sink->flush(); }
    void rotate();

  private :
    severity_level severityLevel = normal;
};

struct HasLogger {
    // get the class logger
    fds_log* GetLog() const;

    // set a new logger & return the old one.
    fds_log* SetLog(fds_log* logptr) const;
  protected:
    mutable fds_log* logptr= NULL;
};

// get the global logger;
extern fds_log* GetLog();

typedef boost::shared_ptr<fds_log> fds_logPtr;

}  // namespace xdi
#endif  // LOG_LOG_H_
