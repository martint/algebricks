package edu.uci.ics.algebricks.examples.piglet.compiler;

import java.io.PrintStream;

import edu.uci.ics.algebricks.api.data.IPrinter;
import edu.uci.ics.algebricks.api.data.IPrinterFactory;
import edu.uci.ics.algebricks.api.data.IPrinterFactoryProvider;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.examples.piglet.types.Type;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.data.IntegerPrinterFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class PigletPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final PigletPrinterFactoryProvider INSTANCE = new PigletPrinterFactoryProvider();

    private PigletPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {
        Type t = (Type) type;
        switch (t.getTag()) {
            case INTEGER:
                return IntegerPrinterFactory.INSTANCE;
            case CHAR_ARRAY:
                return CharArrayPrinterFactory.INSTANCE;
            case FLOAT:
                return FloatPrinterFactory.INSTANCE;
            default:
                throw new UnsupportedOperationException();

        }
    }

    public static class CharArrayPrinterFactory implements IPrinterFactory {

        private static final long serialVersionUID = 1L;

        public static final CharArrayPrinterFactory INSTANCE = new CharArrayPrinterFactory();

        private CharArrayPrinterFactory() {
        }

        @Override
        public IPrinter createPrinter() {
            return new IPrinter() {
                @Override
                public void init() throws AlgebricksException {
                }

                @Override
                public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                    int stringLength = StringUtils.getUTFLen(b, s);
                    int position = s + 3;
                    int maxPosition = position + stringLength;
                    ps.print("\"");
                    while (position < maxPosition) {
                        char c = StringUtils.charAt(b, position);
                        switch (c) {
                            case '\\':
                            case '"':
                                ps.print('\\');
                                break;
                        }
                        ps.print(c);
                        position += StringUtils.charSize(b, position);
                    }
                    ps.print("\"");
                }
            };
        }
    }

    public static class FloatPrinterFactory implements IPrinterFactory {

        private static final long serialVersionUID = 1L;

        public static final FloatPrinterFactory INSTANCE = new FloatPrinterFactory();

        private FloatPrinterFactory() {
        }

        @Override
        public IPrinter createPrinter() {
            return new IPrinter() {
                @Override
                public void init() throws AlgebricksException {
                }

                @Override
                public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                    ps.print(FloatSerializerDeserializer.getFloat(b, s));
                }
            };
        }
    }

}
