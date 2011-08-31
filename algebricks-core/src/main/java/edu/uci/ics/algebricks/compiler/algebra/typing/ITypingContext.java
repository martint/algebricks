package edu.uci.ics.algebricks.compiler.algebra.typing;

import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;

public interface ITypingContext {
    public abstract IVariableTypeEnvironment getOutputTypeEnvironment(ILogicalOperator op);

    public abstract void setOutputTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env);

    public abstract IExpressionTypeComputer getExpressionTypeComputer();

    public abstract INullableTypeComputer getNullableTypeComputer();

    public abstract IMetadataProvider<?, ?> getMetadataProvider();
}
