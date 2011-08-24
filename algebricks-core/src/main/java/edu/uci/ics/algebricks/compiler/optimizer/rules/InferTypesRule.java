package edu.uci.ics.algebricks.compiler.optimizer.rules;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;

public class InferTypesRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator op = opRef.getOperator();
        if (context.getTypeEnvironment(op) != null) {
            return false;
        }
        context.setTypeEnvironment(op, op.computeTypeEnvironment(context));
        return true;
    }

}
