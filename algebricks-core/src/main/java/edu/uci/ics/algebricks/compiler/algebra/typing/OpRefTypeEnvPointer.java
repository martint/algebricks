package edu.uci.ics.algebricks.compiler.algebra.typing;

import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;

public class OpRefTypeEnvPointer implements ITypeEnvPointer {

    private final LogicalOperatorReference op;
    private final ITypingContext ctx;

    public OpRefTypeEnvPointer(LogicalOperatorReference op, ITypingContext ctx) {
        this.op = op;
        this.ctx = ctx;
    }

    @Override
    public IVariableTypeEnvironment getTypeEnv() {
        return ctx.getTypeEnvironment(op.getOperator());
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + op;
    }

}
