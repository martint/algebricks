package edu.uci.ics.algebricks.api.expr;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;

public interface IMergeAggregationExpressionFactory {
    ILogicalExpression createMergeAggregation(ILogicalExpression expr, IOptimizationContext env)
            throws AlgebricksException;
}
