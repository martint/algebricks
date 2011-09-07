package edu.uci.ics.algebricks.compiler.algebra.operators.logical;

import java.util.ArrayList;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.TypePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypeEnvPointer;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypingContext;
import edu.uci.ics.algebricks.compiler.algebra.typing.OpRefTypeEnvPointer;
import edu.uci.ics.algebricks.compiler.algebra.typing.PropagatingTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class SinkOperator extends AbstractLogicalOperator {

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getOperator().getSchema());
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSinkOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[1];
        envPointers[0] = new OpRefTypeEnvPointer(inputs.get(0), ctx);
        PropagatingTypeEnvironment env = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getNullableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
        return env;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SINK;
    }

}
