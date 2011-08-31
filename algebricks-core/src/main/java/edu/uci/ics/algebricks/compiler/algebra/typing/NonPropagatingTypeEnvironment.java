package edu.uci.ics.algebricks.compiler.algebra.typing;

import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;

public class NonPropagatingTypeEnvironment extends AbstractTypeEnvironment {

    public NonPropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMetadataProvider<?, ?> metadataProvider) {
        super(expressionTypeComputer, metadataProvider);
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return varTypeMap.get(var);
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables) throws AlgebricksException {
        return getVarType(var);
    }

}
