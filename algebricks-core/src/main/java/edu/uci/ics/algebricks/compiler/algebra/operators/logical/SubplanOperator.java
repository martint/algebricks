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
package edu.uci.ics.algebricks.compiler.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

/*
 * Author: Guangqiang Li
 * Created on Jul 31, 2009 
 */
public class SubplanOperator extends AbstractOperatorWithNestedPlans {

    public SubplanOperator() {
        super();
    }

    public SubplanOperator(List<ILogicalPlan> plans) {
        super(plans);
    }

    public SubplanOperator(ILogicalOperator planRoot) {
        ArrayList<LogicalOperatorReference> roots = new ArrayList<LogicalOperatorReference>(1);
        roots.add(new LogicalOperatorReference(planRoot));
        nestedPlans.add(new ALogicalPlanImpl(roots));
    }

    public void setRootOp(LogicalOperatorReference opRef) {
        ILogicalPlan p = new ALogicalPlanImpl(opRef);
        nestedPlans.add(p);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SUBPLAN;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ADDNEWVARIABLES;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSubplanOperator(this, arg);
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        // do nothing
    }
}
