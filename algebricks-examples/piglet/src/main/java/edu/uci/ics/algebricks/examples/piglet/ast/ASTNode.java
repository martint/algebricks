package edu.uci.ics.algebricks.examples.piglet.ast;

public abstract class ASTNode {
    public enum Tag {
        ASSIGNMENT,
        DUMP,
        LOAD
    }

    public abstract Tag getTag();
}