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
package edu.uci.ics.algebricks.runtime.hyracks.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.runtime.hyracks.base.IUnnestingFunction;
import edu.uci.ics.algebricks.runtime.hyracks.base.IUnnestingFunctionFactory;
import edu.uci.ics.algebricks.runtime.hyracks.context.RuntimeContext;
import edu.uci.ics.algebricks.runtime.hyracks.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.algebricks.runtime.hyracks.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private int outCol;
    private IUnnestingFunctionFactory unnestingFactory;

    // Each time step() is called on the aggregate, a new value is written in
    // its output. One byte is written before that value and is neglected.
    // By convention, if the aggregate function writes nothing, it means it
    // produced the last value.

    public UnnestRuntimeFactory(int outCol, IUnnestingFunctionFactory unnestingFactory, int[] projectionList) {
        super(projectionList);
        this.outCol = outCol;
        this.unnestingFactory = unnestingFactory;
    }

    @Override
    public String toString() {
        return "unnest " + outCol + " <- " + unnestingFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final RuntimeContext context)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private ArrayBackedValueStorage evalOutput;
            private IUnnestingFunction agg;
            private ArrayTupleBuilder tupleBuilder;

            @Override
            public void open() throws HyracksDataException {
                initAccessAppendRef(context);
                evalOutput = new ArrayBackedValueStorage();
                try {
                    agg = unnestingFactory.createUnnestingFunction(evalOutput);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                tupleBuilder = new ArrayTupleBuilder(projectionList.length);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    produceTuples(tupleBuilder, tAccess, t, tRef);
                }
            }

            private void produceTuples(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    FrameTupleReference tupleRef) throws HyracksDataException {
                try {
                    agg.init(tupleRef);
                    boolean goon = true;
                    do {
                        tb.reset();
                        evalOutput.reset();
                        if (!agg.step()) {
                            goon = false;
                        } else {
                            for (int f = 0; f < projectionList.length; f++) {
                                if (projectionList[f] == outCol) {
                                    tb.addField(evalOutput.getBytes(), evalOutput.getStartIndex(),
                                            evalOutput.getLength());
                                } else {
                                    tb.addField(accessor, tIndex, f);
                                }
                            }
                            appendToFrameFromTupleBuilder(tupleBuilder);
                        }
                    } while (goon);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
            }

            @Override
            public void flush() throws HyracksDataException {
            }
        };
    }

}
