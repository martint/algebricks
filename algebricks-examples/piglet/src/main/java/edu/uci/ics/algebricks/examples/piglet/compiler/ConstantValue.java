package edu.uci.ics.algebricks.examples.piglet.compiler;

import edu.uci.ics.algebricks.examples.piglet.types.Type;

public final class ConstantValue {
    private final Type type;

    private final String image;

    public ConstantValue(Type type, String image) {
        this.type = type;
        this.image = image;
    }

    public Type getType() {
        return type;
    }

    public String getImage() {
        return image;
    }
}