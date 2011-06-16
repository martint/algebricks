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

import java.util.HashSet;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.expressions.ConstantExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;

public class SimpleUnnestToProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        List<LogicalOperatorReference> ins = op.getInputs();

        if (ins == null || ins.isEmpty()) {
            return false;
        }

        LogicalOperatorReference opRef2 = ins.get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getOperator();

        if (!descOrSelfIsSourceScan(op2)) {
            return false;
        }

        HashSet<LogicalVariable> varsUsedInUnnest = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, varsUsedInUnnest);
        if (varsUsedInUnnest.size() == 0) {
            // if (AnalysisUtil.independent(unnestOuter.getExpression(),
            // unnestInner.getExpression())) {
            InnerJoinOperator product = new InnerJoinOperator(new LogicalExpressionReference(ConstantExpression.TRUE));

            LogicalOperatorReference emptySrc = new LogicalOperatorReference(new EmptyTupleSourceOperator());
            List<LogicalOperatorReference> opInpList = op.getInputs();
            opInpList.clear();
            opInpList.add(emptySrc);
            product.getInputs().add(opRef2); // outer branch
            product.getInputs().add(new LogicalOperatorReference(op));
            opRef.setOperator(product); // plug the product in the plan
            return true;
        } else {
            return false;
        }
    }

    private boolean descOrSelfIsSourceScan(AbstractLogicalOperator op2) {
        if (op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            return true;
        }
        for (LogicalOperatorReference cRef : op2.getInputs()) {
            AbstractLogicalOperator alo = (AbstractLogicalOperator) cRef.getOperator();
            if (descOrSelfIsSourceScan(alo)) {
                return true;
            }
        }
        return false;
    }

}
