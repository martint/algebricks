package edu.uci.ics.algebricks.runtime.hyracks.base;

import java.io.Serializable;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;

public interface ISerializableAggregateFunctionFactory extends Serializable {
    public ISerializableAggregateFunction createAggregateFunction() throws AlgebricksException;
}
