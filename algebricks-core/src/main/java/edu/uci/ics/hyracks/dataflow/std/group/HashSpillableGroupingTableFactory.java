/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;

/**
 * @author jarodwen
 */
public class HashSpillableGroupingTableFactory implements ISpillableTableFactory {

    private static final long serialVersionUID = 1L;

    private final ITuplePartitionComputerFactory tpcf;

    private final int tableSize;

    public HashSpillableGroupingTableFactory(ITuplePartitionComputerFactory tpcf, int tableSize) {
        this.tpcf = tpcf;
        this.tableSize = tableSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory#
     * buildSpillableTable
     * (edu.uci.ics.hyracks.api.context.IHyracksStageletContext, int[],
     * edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory[],
     * edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int)
     */
    @Override
    public ISpillableTable buildSpillableTable(final IHyracksStageletContext ctx, final int[] keyFields,
            IBinaryComparatorFactory[] comparatorFactories, final IAggregatorDescriptorFactory aggregatorFactory,
            final RecordDescriptor inRecordDescriptor, final RecordDescriptor outRecordDescriptor, final int framesLimit)
            throws HyracksDataException {
        final int[] storedKeys = new int[keyFields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keyFields[i]];
        }

        final FrameTupleAccessor storedKeysAccessor1 = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);
        final FrameTupleAccessor storedKeysAccessor2 = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final FrameTuplePairComparator ftpcPartial = new FrameTuplePairComparator(keyFields, storedKeys, comparators);

        final FrameTuplePairComparator ftpcTuple = new FrameTuplePairComparator(storedKeys, storedKeys, comparators);

        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        final ITuplePartitionComputer tpc = tpcf.createPartitioner();

        final ByteBuffer outFrame = ctx.allocateFrame();

        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new ISpillableTable() {

            private int dataFrameIndex;
            /**
             * The hashing group table containing pointers to aggregators and
             * also the corresponding key tuples. So for each entry, there will
             * be three integer fields: 1. The frame index containing the key
             * tuple; 2. The tuple index inside of the frame for the key tuple.
             * Note that each link in the table is a partition for the input
             * records. Multiple records in the same partition based on the
             * {@link #tpc} are stored as an array of pointers.
             */
            private final Link[] table = new Link[tableSize];

            private final List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

            /**
             * Pointers for the sorted aggregators
             */
            private int[] tPointers;

            private int groupSize = 0;

            private IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor,
                    outRecordDescriptor, keyFields);

            @Override
            public void reset() {
                groupSize = 0;
                dataFrameIndex = -1;
                tPointers = null;
                // Reset the grouping hash table
                for (int i = 0; i < table.length; i++) {
                    if (table[i] != null)
                        table[i].size = 0;
                }
                aggregator.close();
            }

            @Override
            public boolean insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                if (dataFrameIndex < 0)
                    nextAvailableFrame();
                // Get the partition for the inserting tuple
                int entry = tpc.partition(accessor, tIndex, table.length);
                Link link = table[entry];
                if (link == null) {
                    link = table[entry] = new Link();
                }
                boolean foundGroup = false;
                int sbIndex = -1, stIndex = -1;
                // Find the corresponding aggregator from existing aggregators
                for (int i = 0; i < link.size; i += 2) {
                    sbIndex = link.pointers[i];
                    stIndex = link.pointers[i + 1];
                    storedKeysAccessor1.reset(frames.get(sbIndex));
                    int c = ftpcPartial.compare(accessor, tIndex, storedKeysAccessor1, stIndex);
                    if (c == 0) {
                        foundGroup = true;
                        break;
                    }
                }
                // Do insert
                if (!foundGroup) {
                    // If no matching group is found, create a new aggregator
                    // Create a tuple for the new group
                    tupleBuilder.reset();
                    for (int i = 0; i < keyFields.length; i++) {
                        tupleBuilder.addField(accessor, tIndex, keyFields[i]);
                    }
                    aggregator.init(accessor, tIndex, tupleBuilder);
                    if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        if (!nextAvailableFrame()) {
                            return false;
                        } else {
                            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize())) {
                                throw new IllegalStateException("Failed to init an aggregator");
                            }
                        }
                    }
                    // Write the aggregator back to the hash table
                    sbIndex = dataFrameIndex;
                    stIndex = appender.getTupleCount() - 1;
                    link.add(sbIndex, stIndex);
                    groupSize++;
                } else {
                    // If there is a matching found, do aggregation directly
                    int tupleOffset = storedKeysAccessor1.getTupleStartOffset(stIndex);
                    int aggFieldOffset = storedKeysAccessor1.getFieldStartOffset(stIndex, keyFields.length);
                    int aggFieldLength = storedKeysAccessor1.getFieldLength(stIndex, keyFields.length);
                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor1.getBuffer().array(), tupleOffset
                            + storedKeysAccessor1.getFieldSlotsLength() + aggFieldOffset, aggFieldLength);
                }
                return true;
            }

            @Override
            public List<ByteBuffer> getFrames() {
                return frames;
            }

            @Override
            public int getFrameCount() {
                return dataFrameIndex;
            }

            @Override
            public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

                writer.open();
                appender.reset(outFrame, true);

                if (tPointers == null) {
                    // Not sorted
                    for (int i = 0; i < table.length; ++i) {
                        Link link = table[i];
                        if (link != null) {
                            for (int j = 0; j < link.size; j += 2) {
                                int bIndex = link.pointers[j];
                                int tIndex = link.pointers[j + 1];
                                storedKeysAccessor1.reset(frames.get(bIndex));
                                // Reset the tuple for the partial result
                                tupleBuilder.reset();
                                for (int k = 0; k < keyFields.length; k++) {
                                    tupleBuilder.addField(storedKeysAccessor1, tIndex, k);
                                }
                                if (isPartial)
                                    aggregator.outputPartialResult(storedKeysAccessor1, tIndex, tupleBuilder);
                                else
                                    aggregator.outputResult(storedKeysAccessor1, tIndex, tupleBuilder);
                                while (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(),
                                        0, tupleBuilder.getSize())) {
                                    FrameUtils.flushFrame(outFrame, writer);
                                    appender.reset(outFrame, true);
                                }
                            }
                        }
                    }
                    if (appender.getTupleCount() != 0) {
                        FrameUtils.flushFrame(outFrame, writer);
                    }
                    aggregator.close();
                    return;
                }
                int n = tPointers.length / 2;
                for (int ptr = 0; ptr < n; ptr++) {
                    int tableIndex = tPointers[ptr * 2];
                    int rowIndex = tPointers[ptr * 2 + 1];
                    int frameIndex = table[tableIndex].pointers[rowIndex];
                    int tupleIndex = table[tableIndex].pointers[rowIndex + 1];
                    // Get the frame containing the value
                    ByteBuffer buffer = frames.get(frameIndex);
                    storedKeysAccessor1.reset(buffer);

                    // Insert
                    // Reset the tuple for the partial result
                    tupleBuilder.reset();
                    for (int k = 0; k < keyFields.length; k++) {
                        tupleBuilder.addField(storedKeysAccessor1, tupleIndex, k);
                    }
                    if (isPartial)
                        aggregator.outputPartialResult(storedKeysAccessor1, tupleIndex, tupleBuilder);
                    else
                        aggregator.outputResult(storedKeysAccessor1, tupleIndex, tupleBuilder);

                    if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outFrame, writer);
                        appender.reset(outFrame, true);
                        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outFrame, writer);
                }
                aggregator.close();
            }

            /**
             * Set the working frame to the next available frame in the frame
             * list. There are two cases:<br>
             * 1) If the next frame is not initialized, allocate a new frame. 2)
             * When frames are already created, they are recycled.
             * 
             * @return Whether a new frame is added successfully.
             */
            private boolean nextAvailableFrame() {
                // Return false if the number of frames is equal to the limit.
                if (dataFrameIndex + 1 >= framesLimit)
                    return false;

                if (frames.size() < framesLimit) {
                    // Insert a new frame
                    ByteBuffer frame = ctx.allocateFrame();
                    frame.position(0);
                    frame.limit(frame.capacity());
                    frames.add(frame);
                    appender.reset(frame, true);
                    dataFrameIndex = frames.size() - 1;
                } else {
                    // Reuse an old frame
                    dataFrameIndex++;
                    ByteBuffer frame = frames.get(dataFrameIndex);
                    frame.position(0);
                    frame.limit(frame.capacity());
                    appender.reset(frame, true);
                }
                return true;
            }

            @Override
            public void sortFrames() {
                int totalTCount = 0;
                // Get the number of records
                for (int i = 0; i < table.length; i++) {
                    if (table[i] == null)
                        continue;
                    totalTCount += table[i].size / 2;
                }
                // Start sorting:
                /*
                 * Based on the data structure for the partial aggregates, the
                 * pointers should be initialized.
                 */
                tPointers = new int[totalTCount * 2];
                // Initialize pointers
                int ptr = 0;
                // Maintain two pointers to each entry of the hashing group
                // table
                for (int i = 0; i < table.length; i++) {
                    if (table[i] == null)
                        continue;
                    for (int j = 0; j < table[i].size; j = j + 2) {
                        tPointers[ptr * 2] = i;
                        tPointers[ptr * 2 + 1] = j;
                        ptr++;
                    }
                }
                // Sort using quick sort
                if (tPointers.length > 0) {
                    sort(tPointers, 0, totalTCount);
                }
            }

            private void sort(int[] tPointers, int offset, int length) {
                int m = offset + (length >> 1);
                // Get table index
                int mTable = tPointers[m * 2];
                int mRow = tPointers[m * 2 + 1];
                // Get frame and tuple index
                int mFrame = table[mTable].pointers[mRow];
                int mTuple = table[mTable].pointers[mRow + 1];
                storedKeysAccessor1.reset(frames.get(mFrame));

                int a = offset;
                int b = a;
                int c = offset + length - 1;
                int d = c;
                while (true) {
                    while (b <= c) {
                        int bTable = tPointers[b * 2];
                        int bRow = tPointers[b * 2 + 1];
                        int bFrame = table[bTable].pointers[bRow];
                        int bTuple = table[bTable].pointers[bRow + 1];
                        storedKeysAccessor2.reset(frames.get(bFrame));
                        int cmp = ftpcTuple.compare(storedKeysAccessor2, bTuple, storedKeysAccessor1, mTuple);
                        // int cmp = compare(tPointers, b, mi, mj, mv);
                        if (cmp > 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, a++, b);
                        }
                        ++b;
                    }
                    while (c >= b) {
                        int cTable = tPointers[c * 2];
                        int cRow = tPointers[c * 2 + 1];
                        int cFrame = table[cTable].pointers[cRow];
                        int cTuple = table[cTable].pointers[cRow + 1];
                        storedKeysAccessor2.reset(frames.get(cFrame));
                        int cmp = ftpcTuple.compare(storedKeysAccessor2, cTuple, storedKeysAccessor1, mTuple);
                        // int cmp = compare(tPointers, c, mi, mj, mv);
                        if (cmp < 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, c, d--);
                        }
                        --c;
                    }
                    if (b > c)
                        break;
                    swap(tPointers, b++, c--);
                }

                int s;
                int n = offset + length;
                s = Math.min(a - offset, b - a);
                vecswap(tPointers, offset, b - s, s);
                s = Math.min(d - c, n - d - 1);
                vecswap(tPointers, b, n - s, s);

                if ((s = b - a) > 1) {
                    sort(tPointers, offset, s);
                }
                if ((s = d - c) > 1) {
                    sort(tPointers, n - s, s);
                }
            }

            private void swap(int x[], int a, int b) {
                for (int i = 0; i < 2; ++i) {
                    int t = x[a * 2 + i];
                    x[a * 2 + i] = x[b * 2 + i];
                    x[b * 2 + i] = t;
                }
            }

            private void vecswap(int x[], int a, int b, int n) {
                for (int i = 0; i < n; i++, a++, b++) {
                    swap(x, a, b);
                }
            }
        };
    }

    /**
     * The pointers in the link store 3 int values for each entry in the
     * hashtable: (bufferIdx, tIndex, accumulatorIdx).
     * 
     * @author vinayakb
     */
    private static class Link {
        private static final int INIT_POINTERS_SIZE = 6;

        int[] pointers;
        int size;

        Link() {
            pointers = new int[INIT_POINTERS_SIZE];
            size = 0;
        }

        void add(int bufferIdx, int tIndex) {
            while (size + 2 > pointers.length) {
                pointers = Arrays.copyOf(pointers, pointers.length * 2);
            }
            pointers[size++] = bufferIdx;
            pointers[size++] = tIndex;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[Size=" + size + "]");
            for (int i = 0; i < pointers.length; i = i + 2) {
                sb.append(pointers[i] + ",");
                sb.append(pointers[i + 1] + ";");
            }
            return sb.toString();
        }
    }

}