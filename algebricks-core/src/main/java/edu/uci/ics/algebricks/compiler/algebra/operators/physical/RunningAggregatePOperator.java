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
package edu.uci.ics.algebricks.compiler.algebra.operators.physical;

import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.ILogicalExpressionJobGen;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.base.IRunningAggregateFunctionFactory;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.runtime.hyracks.operators.std.RunningAggregateRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class RunningAggregatePOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RUNNING_AGGREGATE;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getOperator();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        IPartitioningProperty pp = null;
        RunningAggregateOperator ragg = (RunningAggregateOperator) op;
        for (LogicalExpressionReference exprRef : ragg.getExpressions()) {
            StatefulFunctionCallExpression f = (StatefulFunctionCallExpression) exprRef.getExpression();
            IPartitioningProperty p = f.getRequiredPartitioningProperty();
            if (p != null) {
                if (pp == null) {
                    pp = p;
                } else {
                    throw new IllegalStateException("Two stateful functions want to set partitioning requirements: "
                            + pp + " and " + p);
                }
            }
        }
        StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp, null) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        RunningAggregateOperator ragg = (RunningAggregateOperator) op;
        List<LogicalVariable> variables = ragg.getVariables();
        List<LogicalExpressionReference> expressions = ragg.getExpressions();
        int[] outColumns = new int[variables.size()];
        for (int i = 0; i < outColumns.length; i++) {
            outColumns[i] = opSchema.findVariable(variables.get(i));
        }
        IRunningAggregateFunctionFactory[] runningAggFuns = new IRunningAggregateFunctionFactory[expressions.size()];
        ILogicalExpressionJobGen exprJobGen = context.getExpressionJobGen();
        for (int i = 0; i < runningAggFuns.length; i++) {
            StatefulFunctionCallExpression expr = (StatefulFunctionCallExpression) expressions.get(i).getExpression();
            runningAggFuns[i] = exprJobGen.createRunningAggregateFunctionFactory(expr, inputSchemas, context);
        }

        // TODO push projections into the operator
        int[] projectionList = JobGenHelper.projectAllVariables(opSchema);

        RunningAggregateRuntimeFactory runtime = new RunningAggregateRuntimeFactory(outColumns, runningAggFuns,
                projectionList);
        setVarTypes(variables, expressions, context);

        // contribute one Asterix framewriter
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(opSchema, context);
        builder.contributeMicroOperator(ragg, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = ragg.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, ragg, 0);

    }

    private void setVarTypes(List<LogicalVariable> vars, List<LogicalExpressionReference> exprs, JobGenContext context)
            throws AlgebricksException {
        int n = vars.size();
        for (int i = 0; i < n; i++) {
            context.setVarType(vars.get(i), context.getType(exprs.get(i).getExpression()));
        }
    }
}
