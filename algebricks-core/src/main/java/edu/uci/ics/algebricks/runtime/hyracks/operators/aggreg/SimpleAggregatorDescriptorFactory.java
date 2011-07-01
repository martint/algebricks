package edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg;

import java.util.Arrays;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.runtime.hyracks.base.IAggregateFunction;
import edu.uci.ics.algebricks.runtime.hyracks.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;

public class SimpleAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private IAggregateFunctionFactory[] aggFactories;

    public SimpleAggregatorDescriptorFactory(IAggregateFunctionFactory[] aggFactories) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields) throws HyracksDataException {
        final int[] keys = keyFields;
        final int OFFSET_INT_LENGTH = 4;

        /**
         * one IAggregatorDescriptor instance per Gby operator
         */
        return new IAggregatorDescriptor() {
            private final static int AGGFUNCS_INIT_SIZE = 8;
            private int numOfAggs = 0;
            private FrameTupleReference ftr = new FrameTupleReference();
            private IAggregateFunction[][] aggFuncList = new IAggregateFunction[AGGFUNCS_INIT_SIZE][];
            private ArrayBackedValueStorage[][] aggBufList = new ArrayBackedValueStorage[AGGFUNCS_INIT_SIZE][];
            private int offsetFieldIndex = keys.length;

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                /**
                 * put the aggregate function to the end of the list the
                 * internal state is the index in aggList
                 */
                int offset = numOfAggs;
                tb.addField(IntegerSerializerDeserializer.INSTANCE, offset);
                IAggregateFunction[] agg = new IAggregateFunction[aggFactories.length];
                ArrayBackedValueStorage[] aggBuf = new ArrayBackedValueStorage[aggFactories.length];
                if (numOfAggs >= aggFuncList.length) {
                    aggFuncList = Arrays.copyOf(aggFuncList, aggFuncList.length * 2);
                    aggBufList = Arrays.copyOf(aggBufList, aggBufList.length * 2);
                }
                aggFuncList[numOfAggs] = agg;
                aggBufList[numOfAggs] = aggBuf;
                numOfAggs++;
                for (int i = 0; i < agg.length; i++) {
                    try {
                        aggBuf[i] = new ArrayBackedValueStorage();
                        agg[i] = aggFactories[i].createAggregateFunction(aggBuf[i]);
                        agg[i].init();
                        agg[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                int refIndex = IntegerSerializerDeserializer.getInt(data, offset);
                ftr.reset(accessor, tIndex);
                IAggregateFunction[] aggs = aggFuncList[refIndex];
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return OFFSET_INT_LENGTH;
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int refIndex = IntegerSerializerDeserializer.getInt(data, refOffset);

                IAggregateFunction[] aggs = aggFuncList[refIndex];
                ArrayBackedValueStorage[] aggBuffer = aggBufList[refIndex];
                try {
                    for (int i = 0; i < aggs.length; i++) {
                        aggs[i].finishPartial();
                        tb.addField(aggBuffer[i].getBytes(), aggBuffer[i].getStartIndex(), aggBuffer[i].getLength());
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
                aggFuncList[refIndex] = null;
                aggBufList[refIndex] = null;
            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int refIndex = IntegerSerializerDeserializer.getInt(data, refOffset);

                try {
                    IAggregateFunction[] aggs = aggFuncList[refIndex];
                    ArrayBackedValueStorage[] aggBuffer = aggBufList[refIndex];
                    for (int i = 0; i < aggs.length; i++) {
                        aggs[i].finish();
                        tb.addField(aggBuffer[i].getBytes(), aggBuffer[i].getStartIndex(), aggBuffer[i].getLength());
                    }
                    aggFuncList[refIndex] = null;
                    aggBufList[refIndex] = null;
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }

            }

            @Override
            public void reset() {
                for (int i = 0; i < aggFuncList.length; i++)
                    aggFuncList[i] = null;
                for (int i = 0; i < aggBufList.length; i++)
                    aggBufList[i] = null;
            }

            @Override
            public void close() {
                reset();
            }

        };
    }
}
