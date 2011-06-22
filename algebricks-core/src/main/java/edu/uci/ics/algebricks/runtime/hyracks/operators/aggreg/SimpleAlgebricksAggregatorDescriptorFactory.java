package edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg;

import java.util.ArrayList;
import java.util.List;

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

public class SimpleAlgebricksAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private IAggregateFunctionFactory[] aggFactories;
    private IAggregateFunctionFactory[] mergeFactories;

    public SimpleAlgebricksAggregatorDescriptorFactory(IAggregateFunctionFactory[] aggFactories,
            IAggregateFunctionFactory[] mergeFactories) {
        this.aggFactories = aggFactories;
        this.mergeFactories = mergeFactories;
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
            private FrameTupleReference ftr = new FrameTupleReference();
            private List<IAggregateFunction[]> aggFuncList = new ArrayList<IAggregateFunction[]>();
            private List<ArrayBackedValueStorage[]> aggBufList = new ArrayList<ArrayBackedValueStorage[]>();
            private int offsetFieldIndex = keys.length;;
            private IAggregateFunction[] mergeFuncs = new IAggregateFunction[mergeFactories.length];;
            private ArrayBackedValueStorage[] mergeBuffer = new ArrayBackedValueStorage[mergeFactories.length];

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                /**
                 * put the aggregate function to the end of the list the
                 * internal state is the index in aggList
                 */
                int offset = aggFuncList.size();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, offset);
                IAggregateFunction[] agg = new IAggregateFunction[aggFactories.length];
                ArrayBackedValueStorage[] aggBuf = new ArrayBackedValueStorage[aggFactories.length];
                aggFuncList.add(agg);
                aggBufList.add(aggBuf);
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
            public void mergeInit(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < mergeFuncs.length; i++) {
                    try {
                        if (mergeBuffer[i] == null)
                            mergeBuffer[i] = new ArrayBackedValueStorage();
                        mergeBuffer[i].reset();
                        mergeFuncs[i] = mergeFactories[i].createAggregateFunction(mergeBuffer[i]);
                        mergeFuncs[i].init();
                        mergeFuncs[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                if (length != 4)
                    throw new IllegalStateException("integer length is wrong");
                int refIndex = IntegerSerializerDeserializer.getInt(data, offset);
                ftr.reset(accessor, tIndex);
                IAggregateFunction[] aggs = aggFuncList.get(refIndex);
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
            public int merge(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < mergeFuncs.length; i++) {
                    try {
                        mergeFuncs[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return OFFSET_INT_LENGTH;
            }

            @Override
            public void outputPartialAggregateResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int refIndex = IntegerSerializerDeserializer.getInt(data, refOffset);

                if (refIndex < 0) {
                    throw new IllegalStateException("ref index less than 0");
                } else {
                    IAggregateFunction[] aggs = aggFuncList.get(refIndex);
                    ArrayBackedValueStorage[] aggBuffer = aggBufList.get(refIndex);
                    try {
                        for (int i = 0; i < aggs.length; i++) {
                            aggs[i].finishPartial();
                            tb.addField(aggBuffer[i].getBytes(), aggBuffer[i].getStartIndex(), aggBuffer[i].getLength());
                        }
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                    aggFuncList.set(refIndex, null);
                    aggBufList.set(refIndex, null);
                }
            }

            @Override
            public void outputPartialMergeResult(ArrayTupleBuilder tb) throws HyracksDataException {
                try {
                    for (int i = 0; i < mergeFuncs.length; i++) {
                        mergeFuncs[i].finishPartial();
                        tb.addField(mergeBuffer[i].getBytes(), mergeBuffer[i].getStartIndex(),
                                mergeBuffer[i].getLength());
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void outputAggregateResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int refIndex = IntegerSerializerDeserializer.getInt(data, refOffset);

                try {
                    if (refIndex < 0) {
                        throw new IllegalStateException("ref index less than 0");
                    } else {
                        IAggregateFunction[] aggs = aggFuncList.get(refIndex);
                        ArrayBackedValueStorage[] aggBuffer = aggBufList.get(refIndex);
                        if (aggs == null)
                            throw new IllegalStateException("duplicate groups!");
                        for (int i = 0; i < aggs.length; i++) {
                            aggs[i].finish();
                            tb.addField(aggBuffer[i].getBytes(), aggBuffer[i].getStartIndex(), aggBuffer[i].getLength());
                        }
                        aggFuncList.set(refIndex, null);
                        aggBufList.set(refIndex, null);
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }

            }

            @Override
            public void outputMergeResult(ArrayTupleBuilder tb) throws HyracksDataException {
                try {
                    for (int i = 0; i < mergeFuncs.length; i++) {
                        mergeFuncs[i].finish();
                        tb.addField(mergeBuffer[i].getBytes(), mergeBuffer[i].getStartIndex(),
                                mergeBuffer[i].getLength());
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void reset() {
                aggFuncList.clear();
                aggBufList.clear();
            }

            @Override
            public void close() {
                aggFuncList.clear();
                aggBufList.clear();
                reset();
            }

        };
    }
}
