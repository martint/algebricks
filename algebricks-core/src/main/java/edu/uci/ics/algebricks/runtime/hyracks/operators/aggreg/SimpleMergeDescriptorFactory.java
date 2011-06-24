package edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg;

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
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;

public class SimpleMergeDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private IAggregateFunctionFactory[] mergeFactories;

    public SimpleMergeDescriptorFactory(IAggregateFunctionFactory[] mergeFactories) {
        this.mergeFactories = mergeFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields) throws HyracksDataException {
        /**
         * one IAggregatorDescriptor instance per Gby operator
         */
        return new IAggregatorDescriptor() {
            private FrameTupleReference ftr = new FrameTupleReference();
            private IAggregateFunction[] mergeFuncs = new IAggregateFunction[mergeFactories.length];;
            private ArrayBackedValueStorage[] mergeBuffer = new ArrayBackedValueStorage[mergeFactories.length];

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
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
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < mergeFuncs.length; i++) {
                    try {
                        mergeFuncs[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return 0;
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
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
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
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

            }

            @Override
            public void close() {
                reset();
            }

        };
    }
}
