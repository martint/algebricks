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

import java.util.Iterator;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.compiler.optimizer.base.OptimizationUtil;

/**
 * 
 * Looks for a nested group-by plan ending in
 * 
 * subplan {
 * 
 * ...
 * 
 * }
 * 
 * select (function-call: algebricks:not, Args:[function-call: algebricks:is-null,
 * Args:[...]])
 * 
 * nested tuple source -- |UNPARTITIONED|
 * 
 * 
 * 
 */

public class SubplanOutOfGroupRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getOperator();
        if (op0.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op0;

        Iterator<ILogicalPlan> plansIter = gby.getNestedPlans().iterator();
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
        LogicalOperatorReference op1Ref = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op1Ref.getOperator();
        boolean found = false;
        while (op1.getInputs().size() > 0) {
            if (op1.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getOperator();
                if (OptimizationUtil.isNullTest(op2)) {
                    found = true;
                    break;
                }
            }
            op1Ref = op1.getInputs().get(0);
            op1 = (AbstractLogicalOperator) op1Ref.getOperator();
        }
        if (!found) {
            return false;
        }

        ILogicalOperator subplan = op1;
        ILogicalOperator op2 = op1.getInputs().get(0).getOperator();
        op1Ref.setOperator(op2);
        LogicalOperatorReference opUnderRef = gby.getInputs().get(0);
        ILogicalOperator opUnder = opUnderRef.getOperator();
        subplan.getInputs().clear();
        subplan.getInputs().add(new LogicalOperatorReference(opUnder));
        opUnderRef.setOperator(subplan);
        fixNtsTo((SubplanOperator) subplan, opUnderRef);

        return true;
    }

    private void fixNtsTo(AbstractOperatorWithNestedPlans subplan, LogicalOperatorReference opUnderRef) {
        for (ILogicalPlan plan : subplan.getNestedPlans()) {
            for (LogicalOperatorReference r : plan.getRoots()) {
                fixNtsRec((AbstractLogicalOperator) r.getOperator(), opUnderRef);
            }
        }
    }

    private void fixNtsRec(AbstractLogicalOperator op, LogicalOperatorReference opUnderRef) {
        for (LogicalOperatorReference r : op.getInputs()) {
            fixNtsRec((AbstractLogicalOperator) r.getOperator(), opUnderRef);
        }
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
            nts.setDataSourceReference(opUnderRef);
        }
    }
}
