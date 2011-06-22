package edu.uci.ics.algebricks.compiler.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class AggregateOperator extends AbstractAssignOperator {

    // private ArrayList<AggregateFunctionCallExpression> expressions;
    // TODO type safe list of expressions
    private List<LogicalExpressionReference> mergeExpressions;

    public AggregateOperator(ArrayList<LogicalVariable> variables, ArrayList<LogicalExpressionReference> expressions) {
        super(variables, expressions);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.AGGREGATE;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitAggregateOperator(this, arg);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(variables);
    }

    public void setMergeExpressions(List<LogicalExpressionReference> merges) {
        mergeExpressions = merges;
    }

    public List<LogicalExpressionReference> getMergeExpressions() {
        return mergeExpressions;
    }

}
