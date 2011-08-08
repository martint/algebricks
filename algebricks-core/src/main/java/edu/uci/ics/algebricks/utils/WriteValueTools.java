package edu.uci.ics.algebricks.utils;

import java.io.IOException;
import java.io.OutputStream;

public class WriteValueTools {

    private final static int[] INT_INTERVALS = { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999,
            Integer.MAX_VALUE };
    private final static int[] INT_DIVIDERS = { 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };
    private final static int[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    public static void writeInt(int i, OutputStream os) throws IOException {
        if (i < 0) {
            if (i == Integer.MIN_VALUE) {
                os.write("-2147483648".getBytes());
                return;
            }
            os.write('-');
            i = -i;
        }
        int k = 0;
        for (; k < INT_INTERVALS.length; k++) {
            if (i <= INT_INTERVALS[k]) {
                break;
            }
        }
        while (k > 0) {
            int q = i / INT_DIVIDERS[k - 1];
            os.write(DIGITS[q % 10]);
            k--;
        }
        // now, print the units
        os.write(DIGITS[i % 10]);
    }

    public static void main(String[] args) throws IOException {
        writeInt(1000, System.out);
        System.out.println();
        writeInt(-1000, System.out);
        System.out.println();
        writeInt(Integer.MAX_VALUE, System.out);
        System.out.println();
        System.out.println(Integer.MAX_VALUE);
        writeInt(Integer.MIN_VALUE, System.out);
        System.out.println();
        System.out.println(Integer.MIN_VALUE);
        writeInt(Integer.MIN_VALUE + 1, System.out);
        System.out.println();
        System.out.println(Integer.MIN_VALUE + 1);
        writeInt(-204050, System.out);
        System.out.println();
        System.out.println(-204050);
        System.out.flush();
    }

}
