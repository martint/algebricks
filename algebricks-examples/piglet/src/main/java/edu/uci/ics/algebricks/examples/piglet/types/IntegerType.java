package edu.uci.ics.algebricks.examples.piglet.types;

public class IntegerType extends Type {
    public static final Type INSTANCE = new IntegerType();

    private IntegerType() {
    }

    @Override
    public Tag getTag() {
        return Tag.INTEGER;
    }
}