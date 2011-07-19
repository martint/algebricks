package edu.uci.ics.algebricks.examples.piglet.types;

public class TupleType extends Type {
    @Override
    public Tag getTag() {
        return Tag.TUPLE;
    }
}