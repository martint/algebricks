package edu.uci.ics.algebricks.compiler.algebra.typing;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.TypePropagationPolicy;

public class PropagatingTypeEnvironment extends AbstractTypeEnvironment {

    private final TypePropagationPolicy policy;

    private final INullableTypeComputer nullableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    public PropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            INullableTypeComputer nullableTypeComputer, TypePropagationPolicy policy, ITypeEnvPointer[] envPointers) {
        super(expressionTypeComputer);
        this.nullableTypeComputer = nullableTypeComputer;
        this.policy = policy;
        this.envPointers = envPointers;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        return policy.getVarType(var, nullableTypeComputer, envPointers);
    }

}
