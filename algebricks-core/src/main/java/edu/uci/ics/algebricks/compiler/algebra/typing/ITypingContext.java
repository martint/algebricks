package edu.uci.ics.algebricks.compiler.algebra.typing;

import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;

public interface ITypingContext {
    public abstract IVariableTypeEnvironment getTypeEnvironment(ILogicalOperator op);

    public abstract void setTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env);

    public abstract IExpressionTypeComputer getExpressionTypeComputer();

    public abstract INullableTypeComputer getNullableTypeComputer();
}
