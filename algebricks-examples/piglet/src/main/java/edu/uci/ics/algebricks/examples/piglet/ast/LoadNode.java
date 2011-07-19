package edu.uci.ics.algebricks.examples.piglet.ast;

import edu.uci.ics.algebricks.examples.piglet.types.Schema;

public class LoadNode extends RelationNode {
    private String dataFile;

    private Schema schema;

    public LoadNode(String dataFile, Schema schema) {
        this.dataFile = dataFile;
        this.schema = schema;
    }
    
    @Override
    public Tag getTag() {
        return Tag.LOAD;
    }

    public String getDataFile() {
        return dataFile;
    }

    public Schema getSchema() {
        return schema;
    }
}