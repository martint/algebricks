package edu.uci.ics.algebricks.compiler.algebra.typing;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;

public abstract class AbstractTypeEnvironment implements IVariableTypeEnvironment {

    protected final Map<LogicalVariable, Object> varTypeMap = new HashMap<LogicalVariable, Object>();
    protected final IExpressionTypeComputer expressionTypeComputer;

    public AbstractTypeEnvironment(IExpressionTypeComputer expressionTypeComputer) {
        this.expressionTypeComputer = expressionTypeComputer;
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        return expressionTypeComputer.getType(expr, this);
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        varTypeMap.put(var, type);
    }

}
