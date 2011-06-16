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

import java.io.PrintStream;
import java.nio.ByteBuffer;

import edu.uci.ics.algebricks.api.data.IPrinter;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.runtime.hyracks.context.RuntimeContext;
import edu.uci.ics.algebricks.runtime.hyracks.operators.base.AbstractOneInputSinkPushRuntime;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class PrinterRuntime extends AbstractOneInputSinkPushRuntime {

    private RuntimeContext context;
    private final int[] printColumns;
    private final IPrinter[] printers;
    private PrintStream printStream;
    private RecordDescriptor inputRecordDesc;
    private FrameTupleAccessor tAccess;
    private boolean autoClose = false;
    private boolean first = true;

    public PrinterRuntime(int[] printColumns, IPrinter[] printers, RuntimeContext context, PrintStream printStream,
            RecordDescriptor inputRecordDesc) {
        this.printColumns = printColumns;
        this.printers = printers;
        this.context = context;
        this.printStream = printStream;
        this.inputRecordDesc = inputRecordDesc;
        this.tAccess = new FrameTupleAccessor(context.getHyracksContext().getFrameSize(), inputRecordDesc);
    }

    public PrinterRuntime(int[] printColumns, IPrinter[] printers, RuntimeContext context, PrintStream printStream,
            RecordDescriptor inputRecordDesc, boolean autoClose) {
        this(printColumns, printers, context, printStream, inputRecordDesc);
        this.autoClose = autoClose;
    }

    @Override
    public void open() throws HyracksDataException {
        if (first) {
            first = false;
            tAccess = new FrameTupleAccessor(context.getHyracksContext().getFrameSize(), inputRecordDesc);
            for (int i = 0; i < printers.length; i++) {
                try {
                    printers[i].init();
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            try {
                printTuple(buffer, t);
            } catch (AlgebricksException ae) {
                throw new HyracksDataException(ae);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (autoClose) {
            printStream.close();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        printStream.flush();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
    }

    private void printTuple(ByteBuffer buffer, int tIdx) throws HyracksDataException, AlgebricksException {
        for (int i = 0; i < printColumns.length; i++) {
            int startField = tAccess.getTupleStartOffset(tIdx) + tAccess.getFieldSlotsLength()
                    + tAccess.getFieldStartOffset(tIdx, printColumns[i]);
            int fldLen = tAccess.getFieldLength(tIdx, printColumns[i]);
            if (i > 0) {
                printStream.print("; ");
            }
            printers[i].print(buffer.array(), startField, fldLen, printStream);
        }
        printStream.println();
    }

}
