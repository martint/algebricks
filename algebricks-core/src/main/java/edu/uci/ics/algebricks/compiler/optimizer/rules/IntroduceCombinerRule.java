package edu.uci.ics.algebricks.compiler.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.OperatorAnnotations;
import edu.uci.ics.algebricks.compiler.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.functions.IFunctionInfo;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.algebricks.compiler.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.utils.Pair;

public class IntroduceCombinerRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gbyOp = (GroupByOperator) op;
        if (gbyOp.getExecutionMode() != ExecutionMode.PARTITIONED) {
            return false;
        }

        Map<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> toReplaceMap = new HashMap<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>>();

        GroupByOperator newGbyOp = opToPush(gbyOp, toReplaceMap, context);
        if (newGbyOp == null) {
            return false;
        }

        for (Map.Entry<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> entry : toReplaceMap
                .entrySet()) {
            AggregateFunctionCallExpression aggFun = entry.getKey();
            Pair<IFunctionInfo, LogicalExpressionReference> p = entry.getValue();
            aggFun.setFunctionInfo(p.first);
            aggFun.getArguments().clear();
            aggFun.getArguments().add(p.second);
        }

        for (Pair<LogicalVariable, LogicalExpressionReference> p : gbyOp.getGroupByList()) {
            LogicalVariable newGbyVar = context.newVar();
            newGbyOp.addGbyExpression(newGbyVar, p.second.getExpression());
            p.second.setExpression(new VariableReferenceExpression(newGbyVar));
        }

        for (Pair<LogicalVariable, LogicalExpressionReference> p : gbyOp.getDecorList()) {
            LogicalVariable newDecorVar = context.newVar();
            newGbyOp.addDecorExpression(newDecorVar, p.second.getExpression());
            p.second.setExpression(new VariableReferenceExpression(newDecorVar));
        }
        newGbyOp.setExecutionMode(ExecutionMode.LOCAL);
        Object v = gbyOp.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, v);

        Object v2 = gbyOp.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_EXTERNAL_GROUP_BY, v2);

        List<LogicalVariable> propagatedVars = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(newGbyOp, propagatedVars);

        Set<LogicalVariable> usedVars = new HashSet<LogicalVariable>();
        for (ILogicalPlan p : gbyOp.getNestedPlans()) {
            for (LogicalOperatorReference r : p.getRoots()) {
                VariableUtilities.getUsedVariables(r.getOperator(), usedVars);
            }
        }

        for (LogicalVariable var : usedVars) {
            if (!propagatedVars.contains(var)) {
                LogicalVariable newDecorVar = context.newVar();
                newGbyOp.addDecorExpression(newDecorVar, new VariableReferenceExpression(var));
                VariableUtilities.substituteVariables(gbyOp.getNestedPlans().get(0).getRoots().get(0).getOperator(),
                        var, newDecorVar);
            }
        }

        LogicalOperatorReference opRef3 = gbyOp.getInputs().get(0);
        opRef3.setOperator(newGbyOp);
        return true;
    }

    private GroupByOperator opToPush(GroupByOperator gbyOp,
            Map<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> toReplaceMap,
            IOptimizationContext context) throws AlgebricksException {

        LogicalOperatorReference opRef3 = gbyOp.getInputs().get(0);
        ILogicalOperator op3 = opRef3.getOperator();
        GroupByOperator newGbyOp = new GroupByOperator();
        newGbyOp.getInputs().add(new LogicalOperatorReference(op3));
        // copy annotations
        Map<String, Object> annotations = newGbyOp.getAnnotations();
        for (Entry<String, Object> a : gbyOp.getAnnotations().entrySet())
            annotations.put(a.getKey(), a.getValue());

        List<LogicalVariable> gbyVars = gbyOp.getGbyVarList();

        for (ILogicalPlan p : gbyOp.getNestedPlans()) {
            ILogicalPlan pushedSubplan = tryToPushSubplan(p, newGbyOp, toReplaceMap, gbyVars, context);
            if (pushedSubplan != null) {
                newGbyOp.getNestedPlans().add(pushedSubplan);
            } else {
                // for now, if we cannot push everything, give up
                return null;
            }
        }
        return newGbyOp;
    }

    private ILogicalPlan tryToPushSubplan(ILogicalPlan p, GroupByOperator newGbyOp,
            Map<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> toReplaceMap,
            List<LogicalVariable> gbyVars, IOptimizationContext context) {
        List<LogicalOperatorReference> pushedRoots = new ArrayList<LogicalOperatorReference>();
        for (LogicalOperatorReference r : p.getRoots()) {
            Pair<List<LogicalVariable>, LogicalOperatorReference> pushedRoot = tryToPushRoot(r, newGbyOp, toReplaceMap,
                    gbyVars, context);
            if (pushedRoot != null) {
                pushedRoots.add(pushedRoot.second);
            } else {
                // for now, if we cannot push everything, give up
                return null;
            }
        }
        if (pushedRoots.isEmpty()) {
            return null;
        } else {
            return new ALogicalPlanImpl(pushedRoots);
        }
    }

    private Pair<List<LogicalVariable>, LogicalOperatorReference> tryToPushRoot(LogicalOperatorReference r,
            GroupByOperator newGbyOp,
            Map<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> toReplaceMap,
            List<LogicalVariable> gbyVars, IOptimizationContext context) {

        AbstractLogicalOperator op1 = (AbstractLogicalOperator) r.getOperator();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return null;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getOperator();
        if (op2.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return null;
        }

        AggregateOperator initAgg = (AggregateOperator) op1;

        LogicalOperatorReference opRef = tryToPushAgg(initAgg, newGbyOp, toReplaceMap, context);

        if (opRef == null) {
            return null;
        }

        return new Pair<List<LogicalVariable>, LogicalOperatorReference>(gbyVars, opRef);
    }

    private LogicalOperatorReference tryToPushAgg(AggregateOperator initAgg, GroupByOperator newGbyOp,
            Map<AggregateFunctionCallExpression, Pair<IFunctionInfo, LogicalExpressionReference>> toReplaceMap,
            IOptimizationContext context) {

        ArrayList<LogicalVariable> pushedVars = new ArrayList<LogicalVariable>();
        ArrayList<LogicalExpressionReference> pushedExprs = new ArrayList<LogicalExpressionReference>();

        ArrayList<LogicalVariable> initVars = initAgg.getVariables();
        ArrayList<LogicalExpressionReference> initExprs = initAgg.getExpressions();
        int sz = initVars.size();
        for (int i = 0; i < sz; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) initExprs.get(i).getExpression();
            if (!aggFun.isTwoStep()) {
                return null;
            }
        }

        for (int i = 0; i < sz; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) initExprs.get(i).getExpression();
            // if (aggFun.isTwoStep()) {
            // local part
            IFunctionInfo fi1 = aggFun.getStepOneAggregate();
            AggregateFunctionCallExpression aggLocal = new AggregateFunctionCallExpression(fi1, false,
                    new ArrayList<LogicalExpressionReference>(aggFun.getArguments()));
            pushedExprs.add(new LogicalExpressionReference(aggLocal));
            LogicalVariable newAggVar = context.newVar();
            pushedVars.add(newAggVar);
            // global part
            IFunctionInfo fi2 = aggFun.getStepTwoAggregate();

            toReplaceMap.put(aggFun, new Pair<IFunctionInfo, LogicalExpressionReference>(fi2,
                    new LogicalExpressionReference(new VariableReferenceExpression(newAggVar))));
        }

        if (pushedVars.isEmpty()) {
            return null;
        } else {
            AggregateOperator pushedAgg = new AggregateOperator(pushedVars, pushedExprs);
            pushedAgg.setExecutionMode(ExecutionMode.LOCAL);
            NestedTupleSourceOperator nts = new NestedTupleSourceOperator(newGbyOp.getInputs().get(0));
            nts.setExecutionMode(ExecutionMode.LOCAL);
            pushedAgg.getInputs().add(new LogicalOperatorReference(nts));
            return new LogicalOperatorReference(pushedAgg);
        }
    }
}
