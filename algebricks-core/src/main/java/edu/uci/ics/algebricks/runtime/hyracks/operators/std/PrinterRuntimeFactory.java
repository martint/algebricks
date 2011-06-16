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

import edu.uci.ics.algebricks.api.data.IPrinter;
import edu.uci.ics.algebricks.api.data.IPrinterFactory;
import edu.uci.ics.algebricks.runtime.hyracks.base.IPushRuntime;
import edu.uci.ics.algebricks.runtime.hyracks.base.IPushRuntimeFactory;
import edu.uci.ics.algebricks.runtime.hyracks.context.RuntimeContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class PrinterRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int[] printColumns;
    private final IPrinterFactory[] printerFactories;
    private final RecordDescriptor inputRecordDesc;

    public PrinterRuntimeFactory(int[] printColumns, IPrinterFactory[] printerFactories,
            RecordDescriptor inputRecordDesc) {
        this.printColumns = printColumns;
        this.printerFactories = printerFactories;
        this.inputRecordDesc = inputRecordDesc;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("print [");
        for (int i = 0; i < printColumns.length; i++) {
            if (i > 0) {
                buf.append("; ");
            }
            buf.append(printColumns[i]);
        }
        buf.append("]");
        return buf.toString();
    }

    @Override
    public IPushRuntime createPushRuntime(final RuntimeContext context) {
        IPrinter[] printers = createPrinters(printerFactories);
        return new PrinterRuntime(printColumns, printers, context, System.out, inputRecordDesc);
    }

    public static IPrinter[] createPrinters(IPrinterFactory[] pf) {
        IPrinter[] printers = new IPrinter[pf.length];
        for (int i = 0; i < pf.length; i++) {
            printers[i] = pf[i].createPrinter();
        }
        return printers;
    }
}
