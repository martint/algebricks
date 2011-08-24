package edu.uci.ics.algebricks.api.expr;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;

public interface INullableTypeComputer {
    public Object makeNullableType(Object type) throws AlgebricksException;
}
