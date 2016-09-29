/*
 * scst/ScstInquiry.h
 *
 * Copyright (c) 2016, Brian Szmyd <szmyd@formationds.com>
 * Copyright (c) 2016, Formation Data Systems
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTMODE_H_
#define SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTMODE_H_

#include <cstdint>
#include <cstring>
extern "C" {
#include <endian.h>
}
#include <map>
#include <string>
#include <vector>

#include "connector/scst-standalone/ScstScsiCommon.h"

namespace fds {
namespace connector {
namespace scst {

struct ScstTask;

#ifdef __LITTLE_ENDIAN
/// Bit-fields in little endian GCC are backwards
struct __attribute__((__packed__)) ModeHeader {
    enum DpoFua : bool { NoDpoFuaSupport, DpoFuaSupport };
    enum WP : bool { NotWriteProtected, WriteProtected };

    ModeHeader() { std::memset(this, '\0', sizeof(ModeHeader)); }

    void operator &=(DpoFua const dpofua_support) { _dpofua = to_underlying(dpofua_support); }
    void operator &=(WP const write_protected) { _wp = to_underlying(write_protected); }

    uint8_t _data_length;
    uint8_t _medium_type;
    union {
        uint8_t : 3, _dpofua : 1, : 2, _wp : 1;
    };
};
static_assert(3 == sizeof(ModeHeader), "Size of ModeHeader has changed!");

struct __attribute__((__packed__)) Mode6Header {
    Mode6Header() { std::memset(this, '\0', sizeof(Mode6Header)); }

    ModeHeader _header;
    uint8_t _block_descriptor_length;
};
static_assert(4 == sizeof(Mode6Header), "Size of Mode6Header has changed!");

struct __attribute__((__packed__)) Mode10Header {
    enum LongLBA : bool { ShortLBAs, LongLBAs };

    Mode10Header() { std::memset(this, '\0', sizeof(Mode10Header)); }

    void operator &=(LongLBA const long_lbas) { _longlba = to_underlying(long_lbas); }

    uint8_t _data_length_hi;
    ModeHeader _header;
    uint8_t _longlba : 1, :0;
    uint8_t _reserved;
    uint16_t _block_descriptor_length;
};
static_assert(8 == sizeof(Mode10Header), "Size of Mode10Header has changed!");

struct __attribute__((__packed__)) PageHeader {
    PageHeader() { std::memset(this, '\0', sizeof(PageHeader)); }

    uint8_t _page_code : 6, _spf : 1, _ps : 1;
    uint8_t _length;
};
static_assert(2 == sizeof(PageHeader), "Size of PageHeader has changed!");

struct __attribute__((__packed__)) CachingModePage {
    enum AbortPrefetch : bool { WontAbortPrefetch, WillAbortPrefetch };
    enum AnalysisPermit : bool { AnalysisNotPermitted, AnalysisPermitted };
    enum Control : bool { AdaptiveControl, InitiatorControl };
    enum Disc : bool { DiscontinuityTrunc, DiscontinuityNoTrunc };
    enum ForceSeq : bool { WriteSequentialDisabled, WriteSequentialEnabled };
    enum LBCSS : bool { SegmentSizeInBytes, SegmentSizeInBlocks };
    enum MultiFactor : bool { NoMultiplyPrefetch, MultiplyPrefetch };
    enum NonVolatile : bool { NonVolatileEnabled, NonVolatileDisabled };
    enum RCD : bool { ReadCacheEnabled, ReadCacheDisabled };
    enum ReadAhead : bool { ReadAheadEnabled, ReadAheadDisabled };
    enum ReadPriority : uint8_t { NoReadPriority, ReadPrefetchDelay, ReadPrefetchHasten = 0xf };
    enum Size : bool { NumberOfSegments, SegmentSize };
    enum SyncProg : uint8_t { NoSyncProgress, SyncProgressProvided, SyncProgressBlocking };
    enum WCE : bool { WritebackCacheDisabled, WritebackCacheEnabled };
    enum WritePriority : uint8_t { NoWritePriority, WritePrefetchDelay, WritePrefetchHasten = 0xf };

    CachingModePage();

    void operator &=(AbortPrefetch const abort_prefetch) { _abpf = to_underlying(abort_prefetch); }
    void operator &=(AnalysisPermit const permit_analysis) { _cap = to_underlying(permit_analysis); }
    void operator &=(Control const control) { _ic = to_underlying(control); }
    void operator &=(Disc const discontinuity) { _disc = to_underlying(discontinuity); }
    void operator &=(ForceSeq const force_sequential) { _fsw = to_underlying(force_sequential); }
    void operator &=(LBCSS const segment_size) { _lbcss = to_underlying(segment_size); }
    void operator &=(MultiFactor const multiply) { _mf = to_underlying(multiply); }
    void operator &=(NonVolatile const non_volatile) { _nvdis = to_underlying(non_volatile); }
    void operator &=(RCD const read_cache) { _rcd = to_underlying(read_cache); }
    void operator &=(ReadAhead const read_ahead) { _dra = to_underlying(read_ahead); }
    void operator &=(ReadPriority const read_priority) { _read_ret_prior = to_underlying(read_priority); }
    void operator &=(Size const size_control) { _size = to_underlying(size_control); }
    void operator &=(SyncProg const sync_prog) { _sync_prog = to_underlying(sync_prog); }
    void operator &=(WCE const write_back) { _wce = to_underlying(write_back); }
    void operator &=(WritePriority const write_priority) { _write_ret_prior = to_underlying(write_priority); }

    void setPrefetches(size_t const minimum, size_t const maximum, size_t const ceiling, size_t const segment_size);

    PageHeader _header;
    uint8_t _rcd : 1, _mf : 1, _wce : 1, _size : 1, _disc : 1, _cap : 1, _abpf : 1, _ic : 1;
    uint8_t _write_ret_prior : 4, _read_ret_prior : 4;
    uint16_t _disable_prefetch_length;
    uint16_t _minimum_prefetch;
    uint16_t _maximum_prefetch;
    uint16_t _maximum_prefetch_ceiling;
    uint8_t _nvdis : 1, _sync_prog : 2, : 2, _dra : 1, _lbcss : 1, _fsw : 1;
    uint8_t _cache_segments;
    uint16_t _cache_segment_size;
    uint8_t obsolete[4];
};
static_assert(20 == sizeof(CachingModePage), "Size of CachingModePage has changed!");

struct __attribute__((__packed__)) ControlModePage {
    enum DPICZ : bool { DefaultProtectionEnabled, DefaultProtectionDisabled };
    enum DSense : bool { FixedFormatSense, DescriptorFormatSense };
    enum QueueAlg : uint8_t { RestrictedCommandOrdering, UnrestrictedCommandOrdering };
    enum ReleaseUA : bool { UAOnRelease, NoUAOnRelease };
    enum TAS : bool { NoAbortStatusOnTMF, AbortStatusOnTMF };
    enum TMFOnly : bool { ProcessAllOnACA, ProcessTMFOnACA };
    enum TST : uint8_t { SingleTaskSet, MultipleTaskSets };

    ControlModePage();

    void operator &=(DPICZ const protection_default) { _dpicz = to_underlying(protection_default); }
    void operator &=(DSense const desc_sense) { _dsense = to_underlying(desc_sense); }
    void operator &=(QueueAlg const queue_alg_mod) { _queue_alq_modifier = to_underlying(queue_alg_mod); }
    void operator &=(ReleaseUA const release_ua) { _nuar = to_underlying(release_ua); }
    void operator &=(TAS const abort_status) { _tas = to_underlying(abort_status); }
    void operator &=(TMFOnly const tmf_only) { _tmfonly = to_underlying(tmf_only); }
    void operator &=(TST const task_sets) { _tst = to_underlying(task_sets); }

    PageHeader _header;
    uint8_t _rlec : 1, _gltsd : 1, _dsense : 1, _dpicz : 1, _tmfonly : 1, _tst : 3;
    uint8_t : 1, _qerr : 2, _nuar : 1, _queue_alq_modifier : 4;
    uint8_t : 2, _swp : 1, _ua_intlck_ctrl : 2, _rac : 1, : 0;
    uint8_t _autoload_mode : 3, : 1, _rwwp : 1, _atmpe : 1, _tas : 1, _ato : 1;
    uint16_t _obsolete;
    uint16_t _busy_timeout_period;
    uint16_t _extended_selftest_completion_time;
};
static_assert(12 == sizeof(ControlModePage), "Size of ControlModePage has changed!");

struct __attribute__((__packed__)) ReadWriteRecoveryPage {
    enum AutoWriteReassign : bool { NoAutomaticReassignWrite, AutomaticReassignWrite };
    enum AutoReadReassign : bool { NoAutomaticReassignRead, AutomaticReassignRead };
    enum EarlyRecovery : bool { SafeRecovery, ExpedientRecovery };
    enum DataTerminate : bool { NoTerminationOnRecovery, TerminateOnRecovery };
    enum DisableCorrection : bool { CorrectionAllowed, CorrectionPrevented };
    enum PostError : bool { NotifyReadRecovery, IgnoreReadRecovery };
    enum ReadContinuous : bool { DelayOnTransferOk, NoDelayOnTransfer };
    enum TransferBlock : bool { NoPseudoDataOnError, PseudoDataOnError };

    ReadWriteRecoveryPage();

    void operator &=(AutoWriteReassign const auto_reassign) { _awre = to_underlying(auto_reassign); }
    void operator &=(AutoReadReassign const auto_reassign) { _arre = to_underlying(auto_reassign); }
    void operator &=(DataTerminate const data_terminate) { _dte = to_underlying(data_terminate); }
    void operator &=(DisableCorrection const disable_correction) { _dcr = to_underlying(disable_correction); }
    void operator &=(EarlyRecovery const early_recovery) { _eer = to_underlying(early_recovery); }
    void operator &=(PostError const post_error) { _per = to_underlying(post_error); }
    void operator &=(ReadContinuous const read_continuous) { _rc = to_underlying(read_continuous); }
    void operator &=(TransferBlock const transfer_block) { _tb = to_underlying(transfer_block); }

    PageHeader _header;
    uint8_t _dcr : 1, _dte : 1, _per : 1, _eer : 1, _rc : 1, _tb : 1, _arre : 1, _awre : 1;
    uint8_t _read_retry_count;
    uint8_t _obsolete[3];
    uint8_t _mmc6_restricted : 2, : 5, _lbpere : 1;
    uint8_t _write_retry_count;
    uint8_t _reserved;
    uint16_t _recovery_time_limit;
};
static_assert(12 == sizeof(ReadWriteRecoveryPage), "Size of ReadWriteRecoveryPage has changed!");

struct __attribute__((__packed__)) BlockDescriptor {
    BlockDescriptor() { std::memset(this, '\0', sizeof(BlockDescriptor)); }

    uint8_t _density_code;
    uint32_t _number_of_blocks : 24;
    uint8_t _reserved;
    uint32_t _block_length : 24;
};
static_assert(8 == sizeof(BlockDescriptor), "Size of BlockDescriptor has changed!");


#else
#error "Big Endian not supported!"
#endif

struct ModeHandler {
    ModeHandler();

    template<typename ModePage>
    void addModePage(ModePage && page);
    void writeModeParameters6(ScstTask* task, bool const block_descriptor, uint8_t const page_code) const;
    void writeModeParameters10(ScstTask* task, bool const block_descriptor, uint8_t const page_code) const;

    void setBlockDescriptor(size_t const lba_count, size_t const lba_size);

   private:
    size_t writePage(ScstTask* task, size_t& offset, uint8_t const page_code) const;

    BlockDescriptor _block_descriptor;
    std::map<uint8_t, std::vector<uint8_t>> mode_pages;
};

template<typename ModePage>
void ModeHandler::addModePage(ModePage && page)
{
    static_assert(2 < sizeof(ModePage), "Refusing to add pages smaller than 2 bytes.");
    auto raw_descriptor = reinterpret_cast<uint8_t*>(&page);
    uint8_t page_code = raw_descriptor[0] & 0x3F;
    size_t length = raw_descriptor[1] + 2;
    if (sizeof(page) < length ) return; // Refuse to read into out-of-bounds memory
    auto& page_buffer = mode_pages[page_code];
    page_buffer.resize(length);
    memcpy(page_buffer.data(), &page, length);
}

}  // namespace scst
}  // namespace connector
}  // namespace fds

#endif  // SOURCE_ACCESS_MGR_INCLUDE_CONNECTOR_SCST_STANDALONE_SCSTMODE_H_
