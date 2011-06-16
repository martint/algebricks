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
package edu.uci.ics.algebricks.compiler.optimizer.base;

import java.util.Collection;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.algebricks.compiler.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.algebricks.compiler.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.algebricks.config.AlgebricksConfig;

public abstract class AbstractRuleController {

    protected IOptimizationContext context;
    private LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();

    public AbstractRuleController() {
    }

    public AbstractRuleController(IOptimizationContext context) {
        this.context = context;
    }

    public void setContext(IOptimizationContext context) {
        this.context = context;
    }

    /**
     * Each rewriting strategy may differ in the
     * 
     * @param root
     * @param ruleClasses
     * @return true iff one of the rules in the collection fired
     */
    public abstract boolean rewriteWithRuleCollection(LogicalOperatorReference root,
            Collection<IAlgebraicRewriteRule> rules) throws AlgebricksException;

    /**
     * @param opRef
     * @param rule
     * @return true if any rewrite was fired, either on opRef or any operator
     *         under it.
     */
    protected boolean rewriteOperatorRef(LogicalOperatorReference opRef, IAlgebraicRewriteRule rule)
            throws AlgebricksException {
        return rewriteOperatorRef(opRef, rule, true, false);
    }

    private void printRuleApplication(IAlgebraicRewriteRule rule, LogicalOperatorReference opRef) throws AlgebricksException {
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine(">>>> Rule " + rule.getClass() + " fired.\n");
        StringBuilder sb = new StringBuilder();
        PlanPrettyPrinter.printOperator((AbstractLogicalOperator) opRef.getOperator(), sb, pvisitor, 0);
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine(sb.toString());
    }

    protected boolean rewriteOperatorRef(LogicalOperatorReference opRef, IAlgebraicRewriteRule rule, boolean enterGbys,
            boolean fullDFS) throws AlgebricksException {

        if (rule.rewritePre(opRef, context)) {
            printRuleApplication(rule, opRef);
            return true;
        }
        boolean rewritten = false;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();

        for (LogicalOperatorReference inp : op.getInputs()) {
            if (rewriteOperatorRef(inp, rule, enterGbys, fullDFS)) {
                rewritten = true;
                if (!fullDFS) {
                    break;
                }
            }
        }

        if (op.hasNestedPlans() && (enterGbys || op.getOperatorTag() != LogicalOperatorTag.GROUP)) {
            AbstractOperatorWithNestedPlans o2 = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : o2.getNestedPlans()) {
                for (LogicalOperatorReference r : p.getRoots()) {
                    if (rewriteOperatorRef(r, rule, enterGbys, fullDFS)) {
                        rewritten = true;
                        break;
                    }
                }
                if (rewritten) {
                    break;
                }
            }
        }

        if (rule.rewritePost(opRef, context)) {
            printRuleApplication(rule, opRef);
            return true;
        }

        return rewritten;
    }
}
