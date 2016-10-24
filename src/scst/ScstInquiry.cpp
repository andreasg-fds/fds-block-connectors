/*
 * scst/ScstInquiry.cpp
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

#include "connector/scst-standalone/ScstInquiry.h"
#include "connector/scst-standalone/ScstTask.h"

namespace fds {
namespace connector {
namespace scst {

static void setPaddedString(void* buffer, std::string const & str, size_t const buf_max)
{
    auto to_copy = buf_max;
    if (to_copy > str.size()) {
        std::memset(reinterpret_cast<char*>(buffer) + str.size(), ' ', (to_copy - str.size()));
        to_copy = str.size();
    }
    std::memcpy(buffer, str.data(), to_copy);
}


void VPDPage::writePage(uint8_t const page_code, void const* params, size_t const len)
{
    auto to_write = std::min(len, sizeof(_parameters));
    memcpy(_parameters, params, to_write);
    _header._page_code = page_code;
    _header._page_length = htobe16(static_cast<uint16_t>(len));
}

NAADesignator::NAADesignator()
{
    _header &= DesignatorHeader::NAA;
    _header &= DesignatorHeader::BinaryCodeSet;
    _header._designator_length = 8;
    _naa = 0x5;
}

NAADesignator::NAADesignator(uint32_t const company_id, uint64_t const vendor_id)
    : NAADesignator()
{
    // First get in the write order
    auto cid = htobe32(company_id);
    auto vid = htobe64(vendor_id);

    // Write Company_Id
    uint8_t* ieee_oui = reinterpret_cast<uint8_t*>(&cid) + 1;
    _hi_company_id = (ieee_oui[0] >> 4);
    _mid_company_id = ((ieee_oui[0] << 4) | (ieee_oui[1] >> 4));
    _mid_company_id = htobe16((_mid_company_id << 8) | ((ieee_oui[1] << 4) | (ieee_oui[2] >> 4)));
    _low_company_id = (ieee_oui[2] & 0x0f);
    // Write Vendor Specific_Id
    uint8_t* vid_id = reinterpret_cast<uint8_t*>(&vid) + 3;
    _hi_vendor_id = (vid_id[0] >> 4);
    _low_vendor_id = *reinterpret_cast<uint32_t*>(vid_id + 1);
}

VendorSpecificIdentifier::VendorSpecificIdentifier()
{
    _header &= DesignatorHeader::VendorSpecific;
    _header &= DesignatorHeader::ASCIICodeSet;
    _header._designator_length = sizeof(_id);
}

VendorSpecificIdentifier::VendorSpecificIdentifier(std::string const& id)
    : VendorSpecificIdentifier()
{
    setPaddedString(_id, id, sizeof(_id));
}

T10Designator::T10Designator()
{
    _header &= DesignatorHeader::T10VendorId;
    _header &= DesignatorHeader::ASCIICodeSet;
    _header._designator_length = 8;
}

T10Designator::T10Designator(std::string const& vendor_id)
    : T10Designator()
{
    setPaddedString(_t10_vendor_id, vendor_id, sizeof(_t10_vendor_id));
}

StandardInquiry::StandardInquiry() :
    _header {}
{
    _header._version = 0x06;
    _header._response_format = 0x02;
    _header._additional_length = 31;
    (*this) &= PeripheralQualifer::ConnectedOrUnknown;
    (*this) &= NotRemovable;
    (*this) &= NotPartOfConglomerate;
    (*this) &= NoNACASupport;
    (*this) &= NoHistoricalSupport;
    (*this) &= NoArrayController;
    (*this) &= NoAccessController;
    (*this) &= NoAsymmetricSupport;
    (*this) &= ThirdPartyCopy;
    (*this) &= NoProtection;
    (*this) &= NoEnclosure;
    (*this) &= MultiPorted;
    (*this) &= CommandMgmt;
}

void StandardInquiry::setProductId(std::string const & product_id)
{ setPaddedString(_product_identification, product_id, sizeof(_product_identification)); }

void StandardInquiry::setRevision(std::string const & revision)
{ setPaddedString(_product_revision_level, revision, sizeof(_product_revision_level)); }

void StandardInquiry::setVendorId(std::string const & vendor_id)
{ setPaddedString(_t10_vendor_id, vendor_id, sizeof(_t10_vendor_id)); }

InquiryHandler::InquiryHandler()
{
    // Add the supported pages VPD page
    addVPDPage(VPDPage());
}

void InquiryHandler::addVPDPage(VPDPage page)
{
    auto page_code = page.getPageCode();
    vpd_pages[page_code] = page;

    // Update the Supported Pages page
    std::array<uint8_t, 60> page_codes;
    size_t i = 0;
    for (auto const& page_pair : vpd_pages) {
        page_codes[i++] = page_pair.first;
    }
    vpd_pages[0].writePage(0, page_codes.data(), vpd_pages.size());
}

void InquiryHandler::writeToBuffer(ScstTask* task, void* src, size_t const len)
{
    auto buffer = task->getResponseBuffer();
    auto to_write = std::min(task->getResponseBufferLen(), len);
    memcpy(buffer, src, to_write);
    task->setResponseLength(to_write);
}

void InquiryHandler::writeStandardInquiry(ScstTask* task)
{
    writeToBuffer(task, &standard_inquiry, sizeof(standard_inquiry));
}

void InquiryHandler::writeVPDPage(ScstTask* task, uint8_t const page_code)
{
    auto it = vpd_pages.find(page_code);
    if (vpd_pages.end() == it) {
        task->checkCondition(SCST_LOAD_SENSE(scst_sense_invalid_field_in_cdb));
        return;
    }
    auto& page = it->second;
    writeToBuffer(task, &page, page.getParamLength() + sizeof(VPDHeader));
}

}  // namespace scst
}  // namespace connector
}  // namespace fds
