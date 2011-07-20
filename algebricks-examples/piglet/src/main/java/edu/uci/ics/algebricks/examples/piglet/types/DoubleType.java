package edu.uci.ics.algebricks.examples.piglet.types;

public class DoubleType extends Type {
    public static final Type INSTANCE = new DoubleType();

    private DoubleType() {
    }

    @Override
    public Tag getTag() {
        return Tag.DOUBLE;
    }
}