package edu.uci.ics.algebricks.tests.tools;

import org.junit.Test;

import edu.uci.ics.algebricks.utils.WriteValueTools;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;

public class WriteValueTest {

    private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();

    @Test
    public void writeIntegers() throws Exception {
        writeIntTest(6);
        writeIntTest(1234);
        writeIntTest(-1234);
        writeIntTest(Integer.MAX_VALUE);
        writeIntTest(Integer.MAX_VALUE - 1);
        writeIntTest(Integer.MIN_VALUE);
        writeIntTest(Integer.MIN_VALUE + 1);
    }

    private void writeIntTest(int i) throws Exception {
        baaos.reset();
        WriteValueTools.writeInt(i, baaos);
        byte[] goal = Integer.toString(i).getBytes();
        if (baaos.size() != goal.length) {
            throw new Exception("Expecting to write " + i + " in " + goal.length + " bytes, but found " + baaos.size()
                    + " bytes.");
        }
        for (int k = 0; k < goal.length; k++) {
            if (goal[k] != baaos.getByteArray()[k]) {
                throw new Exception("Expecting to write " + i + " as " + goal + ", but found " + baaos.getByteArray()
                        + " instead.");
            }
        }
    }
}
