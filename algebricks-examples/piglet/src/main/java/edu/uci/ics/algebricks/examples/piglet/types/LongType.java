package edu.uci.ics.algebricks.examples.piglet.types;

public class LongType extends Type {
    public static final Type INSTANCE = new LongType();

    private LongType() {
    }

    @Override
    public Tag getTag() {
        return Tag.LONG;
    }
}