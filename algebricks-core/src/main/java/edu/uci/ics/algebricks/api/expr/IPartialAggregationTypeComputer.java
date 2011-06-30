package edu.uci.ics.algebricks.api.expr;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;

public interface IPartialAggregationTypeComputer {
    public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env) throws AlgebricksException;
}
