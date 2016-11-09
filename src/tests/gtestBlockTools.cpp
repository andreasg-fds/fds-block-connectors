/*
 * gtestBlockTools.cpp
 *
 * Copyright (c) 2016, Andreas Griesshammer <andreas@formationds.com>
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
#include <gtest/gtest.h>

#include "connector/block/BlockTools.h"

static const uint32_t lba_size = 512;

// Aligned single 128k object
TEST(BlockToolsTest, AlignedSingleBlock) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 0, 131072, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 0);
    EXPECT_EQ(oi.startDiffOffset, 0);
    EXPECT_EQ(oi.endDiffOffset, 0);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 1);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 0);
    EXPECT_TRUE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 1);
}

// Aligned write spanning two 128k objects
TEST(BlockToolsTest, AlignedBlocks) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 0, 262144, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 1);
    EXPECT_EQ(oi.startDiffOffset, 0);
    EXPECT_EQ(oi.endDiffOffset, 0);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 2);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 1);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 2);
}

// Single partial 128k block
TEST(BlockToolsTest, SinglePartialBlock) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 1*lba_size, 10*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 0);
    EXPECT_EQ(oi.startDiffOffset, 512);
    EXPECT_EQ(oi.endDiffOffset, 125440);
    EXPECT_FALSE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 0);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 0);
    EXPECT_TRUE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 1);
}

// 128k but spanning over two objects
TEST(BlockToolsTest, SpanningBlock) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 50*lba_size, 131072, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 1);
    EXPECT_EQ(oi.startDiffOffset, 25600);
    EXPECT_EQ(oi.endDiffOffset, 105472);
    EXPECT_FALSE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 0);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 0);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 2);
}

// <128k but spanning over two objects
TEST(BlockToolsTest, SpanningBlock2) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 160*lba_size, 250*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 1);
    EXPECT_EQ(oi.startDiffOffset, 81920);
    EXPECT_EQ(oi.endDiffOffset, 52224);
    EXPECT_FALSE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 0);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 0);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 2);
}

// >128k but spanning over two objects
TEST(BlockToolsTest, SpanningBlock3) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 150*lba_size, 300*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 0);
    EXPECT_EQ(oi.endBlockOffset, 1);
    EXPECT_EQ(oi.startDiffOffset, 76800);
    EXPECT_EQ(oi.endDiffOffset, 31744);
    EXPECT_FALSE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 0);
    EXPECT_EQ(oi.fullStartBlockOffset, 0);
    EXPECT_EQ(oi.fullEndBlockOffset, 0);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 2);
}

// >128k, spanning and ending on object boundary
TEST(BlockToolsTest, SpanningBlock4) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 506*lba_size, 262*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 1);
    EXPECT_EQ(oi.endBlockOffset, 2);
    EXPECT_EQ(oi.startDiffOffset, 128000);
    EXPECT_EQ(oi.endDiffOffset, 0);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 1);
    EXPECT_EQ(oi.fullStartBlockOffset, 2);
    EXPECT_EQ(oi.fullEndBlockOffset, 2);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 2);
}

// Large, aligned, 50 objects
TEST(BlockToolsTest, LargeAligned) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 15*256*lba_size, 50*256*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 15);
    EXPECT_EQ(oi.endBlockOffset, 64);
    EXPECT_EQ(oi.startDiffOffset, 0);
    EXPECT_EQ(oi.endDiffOffset, 0);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 50);
    EXPECT_EQ(oi.fullStartBlockOffset, 15);
    EXPECT_EQ(oi.fullEndBlockOffset, 64);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 50);
}

// Large, spanning and starting on object boundary
// Start on block 10 and write >20
TEST(BlockToolsTest, LargeSpanningStartAligned) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 10*256*lba_size, 5130*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 10);
    EXPECT_EQ(oi.endBlockOffset, 30);
    EXPECT_EQ(oi.startDiffOffset, 0);
    EXPECT_EQ(oi.endDiffOffset, 125952);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 20);
    EXPECT_EQ(oi.fullStartBlockOffset, 10);
    EXPECT_EQ(oi.fullEndBlockOffset, 29);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 21);
}

// Large, spanning and ending on object boundary
TEST(BlockToolsTest, LargeSpanningEndAligned) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 2570*lba_size, 5366*lba_size, 131072);
    EXPECT_EQ(oi.startBlockOffset, 10);
    EXPECT_EQ(oi.endBlockOffset, 30);
    EXPECT_EQ(oi.startDiffOffset, 5120);
    EXPECT_EQ(oi.endDiffOffset, 0);
    EXPECT_TRUE(oi.spansFullBlocks);
    EXPECT_EQ(oi.numFullBlocks, 20);
    EXPECT_EQ(oi.fullStartBlockOffset, 11);
    EXPECT_EQ(oi.fullEndBlockOffset, 30);
    EXPECT_FALSE(oi.isSingleObject());
    EXPECT_EQ(oi.numTotalBlocks(), 21);
}

// MaxObjectSize of 0 was causing a coredump
TEST(BlockToolsTest, ZeroMaxObjectSize) {
    fds::block::OffsetInfo oi;
    calculateOffsets(oi, 0, 256*lba_size, 0);
}

TEST(BlockToolsTest, ZeroLength) {
    fds::block::OffsetInfo oi;
    EXPECT_DEATH(calculateOffsets(oi, 2147614720, 0, 131072), "length != 0");
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
