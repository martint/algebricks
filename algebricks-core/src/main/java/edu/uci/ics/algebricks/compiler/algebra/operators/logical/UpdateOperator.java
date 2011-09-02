package edu.uci.ics.algebricks.compiler.algebra.operators.logical;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypingContext;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class UpdateOperator extends AbstractLogicalOperator {

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isMap() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public LogicalOperatorTag getOperatorTag() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

}
