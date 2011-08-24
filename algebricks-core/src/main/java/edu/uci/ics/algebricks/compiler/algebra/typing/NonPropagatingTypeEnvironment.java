package edu.uci.ics.algebricks.compiler.algebra.typing;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;

public class NonPropagatingTypeEnvironment extends AbstractTypeEnvironment {

    public NonPropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer) {
        super(expressionTypeComputer);
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return varTypeMap.get(var);
    }

}
