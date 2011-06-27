/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.algebricks.compiler.optimizer.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.expressions.ConstantExpression;
import edu.uci.ics.algebricks.compiler.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.algebricks.compiler.algebra.functions.IFunctionInfo;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractBinaryJoin;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AssignOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.SelectOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.algebricks.compiler.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.algebricks.compiler.algebra.properties.FunctionalDependency;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.compiler.optimizer.base.OperatorManipulationUtil;
import edu.uci.ics.algebricks.compiler.optimizer.base.PhysicalOptimizationsUtil;
import edu.uci.ics.algebricks.config.AlgebricksConfig;
import edu.uci.ics.algebricks.utils.Pair;

public class IntroduceGroupByForSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getOperator();
        if (op0.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op0;

        Iterator<ILogicalPlan> plansIter = subplan.getNestedPlans().iterator();
        ILogicalPlan p = null;
        while (plansIter.hasNext()) {
            p = plansIter.next();
        }
        if (p == null) {
            return false;
        }
        if (p.getRoots().size() != 1) {
            return false;
        }
        LogicalOperatorReference subplanRoot = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) subplanRoot.getOperator();

        LogicalOperatorReference botRef = subplanRoot;
        AbstractLogicalOperator op2;
        // Project is optional
        if (op1.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            op2 = op1;
        } else {
            ProjectOperator project = (ProjectOperator) op1;
            botRef = project.getInputs().get(0);
            op2 = (AbstractLogicalOperator) botRef.getOperator();
        }
        if (op2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) op2;

        LogicalOperatorReference op3Ref = aggregate.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op3Ref.getOperator();

        while (op3.getInputs().size() == 1) {
            botRef = op3Ref;
            op3Ref = op3.getInputs().get(0);
            op3 = (AbstractLogicalOperator) op3Ref.getOperator();
        }

        if (op3.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op3.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoin join = (AbstractBinaryJoin) op3;
        if (join.getCondition().getExpression() == ConstantExpression.TRUE) {
            return false;
        }

        AbstractLogicalOperator b0 = (AbstractLogicalOperator) join.getInputs().get(0).getOperator();
        // see if there's an NTS at the end of the pipeline
        NestedTupleSourceOperator outerNts = getNts(b0);
        if (outerNts == null) {
            AbstractLogicalOperator b1 = (AbstractLogicalOperator) join.getInputs().get(1).getOperator();
            outerNts = getNts(b1);
            if (outerNts == null) {
                return false;
            }
        }

        Set<LogicalVariable> pkVars = computeGbyVarsUsingPksOnly(outerNts, context);
        if (pkVars == null || pkVars.size() < 1) {
            // could not group only by primary keys
            return false;
        }
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Found primary key for introducing group-by: " + pkVars);

        LogicalOperatorReference rightRef = join.getInputs().get(1);
        LogicalVariable testForNull = null;
        AbstractLogicalOperator right = (AbstractLogicalOperator) rightRef.getOperator();
        switch (right.getOperatorTag()) {
            case UNNEST: {
                UnnestOperator innerUnnest = (UnnestOperator) right;
                // Select [ $y != null ]
                testForNull = innerUnnest.getVariable();
                break;
            }
            case DATASOURCESCAN: {
                DataSourceScanOperator innerScan = (DataSourceScanOperator) right;
                // Select [ $y != null ]
                if (innerScan.getVariables().size() == 1) {
                    testForNull = innerScan.getVariables().get(0);
                }
                break;
            }
        }
        if (testForNull == null) {
            testForNull = context.newVar();
            AssignOperator tmpAsgn = new AssignOperator(testForNull, new LogicalExpressionReference(
                    ConstantExpression.TRUE));
            tmpAsgn.getInputs().add(new LogicalOperatorReference(rightRef.getOperator()));
            rightRef.setOperator(tmpAsgn);
        }

        IFunctionInfo finfoEq = AlgebricksBuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.IS_NULL);
        ILogicalExpression isNullTest = new ScalarFunctionCallExpression(finfoEq, new LogicalExpressionReference(
                new VariableReferenceExpression(testForNull)));
        IFunctionInfo finfoNot = AlgebricksBuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.NOT);
        ScalarFunctionCallExpression nonNullTest = new ScalarFunctionCallExpression(finfoNot,
                new LogicalExpressionReference(isNullTest));
        SelectOperator selectNonNull = new SelectOperator(new LogicalExpressionReference(nonNullTest));
        GroupByOperator g = new GroupByOperator();
        LogicalOperatorReference newSubplanRef = new LogicalOperatorReference(subplan);
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new LogicalOperatorReference(g));
        opRef.setOperator(g);
        selectNonNull.getInputs().add(new LogicalOperatorReference(nts));

        List<LogicalOperatorReference> prodInpList = botRef.getOperator().getInputs();
        prodInpList.clear();
        prodInpList.add(new LogicalOperatorReference(selectNonNull));

        ILogicalPlan gPlan = new ALogicalPlanImpl(new LogicalOperatorReference(subplanRoot.getOperator()));
        g.getNestedPlans().add(gPlan);
        subplanRoot.setOperator(op3Ref.getOperator());
        g.getInputs().add(newSubplanRef);

        HashSet<LogicalVariable> underVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(subplan.getInputs().get(0).getOperator(), underVars);
        underVars.removeAll(pkVars);
        buildVarExprList(pkVars, context, g, g.getGroupByList());
        buildVarExprList(underVars, context, g, g.getDecorList());

        return true;
    }

    private NestedTupleSourceOperator getNts(AbstractLogicalOperator op) {
        AbstractLogicalOperator alo = op;
        do {
            if (alo.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                return (NestedTupleSourceOperator) alo;
            }
            if (alo.getInputs().size() != 1) {
                return null;
            }
            alo = (AbstractLogicalOperator) alo.getInputs().get(0).getOperator();
        } while (true);
    }

    protected Set<LogicalVariable> computeGbyVarsUsingPksOnly(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
        List<FunctionalDependency> fdList = context.getFDList(op);
        if (fdList == null) {
            return null;
        }
        // check if any of the FDs is a key
        List<LogicalVariable> all = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(op, all);
        for (FunctionalDependency fd : fdList) {
            if (fd.getTail().containsAll(all)) {
                return new HashSet<LogicalVariable>(fd.getHead());
            }
        }
        return null;
    }

    private void buildVarExprList(Collection<LogicalVariable> vars, IOptimizationContext context, GroupByOperator g,
            List<Pair<LogicalVariable, LogicalExpressionReference>> outVeList) throws AlgebricksException {
        for (LogicalVariable ov : vars) {
            LogicalVariable newVar = context.newVar();
            ILogicalExpression varExpr = new VariableReferenceExpression(newVar);
            outVeList.add(new Pair<LogicalVariable, LogicalExpressionReference>(ov, new LogicalExpressionReference(
                    varExpr)));
            for (ILogicalPlan p : g.getNestedPlans()) {
                for (LogicalOperatorReference r : p.getRoots()) {
                    OperatorManipulationUtil.substituteVarRec((AbstractLogicalOperator) r.getOperator(), ov, newVar);
                }
            }
            AbstractLogicalOperator opUnder = (AbstractLogicalOperator) g.getInputs().get(0).getOperator();
            OperatorManipulationUtil.substituteVarRec(opUnder, ov, newVar);
            // g.substituteVarInNestedPlans(ov, newVar);
            // OperatorManipulationUtil.substituteVarRec(lojoin, ov, newVar);
        }
    }

}
