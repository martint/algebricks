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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.ILogicalExpressionJobGen;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.base.IAggregateFunctionFactory;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg.AggregateRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class AggregatePOperator extends AbstractPhysicalOperator {

    public AggregatePOperator() {
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.AGGREGATE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getOperator();
        IPhysicalPropertiesVector childProps = op2.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProps.getPartitioningProperty(),
                new ArrayList<ILocalStructuralProperty>(0));
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AggregateOperator aggOp = (AggregateOperator) op;
        List<LogicalVariable> variables = aggOp.getVariables();
        List<LogicalExpressionReference> expressions = aggOp.getExpressions();
        int[] outColumns = new int[variables.size()];
        for (int i = 0; i < outColumns.length; i++) {
            outColumns[i] = opSchema.findVariable(variables.get(i));
        }
        IAggregateFunctionFactory[] aggFactories = new IAggregateFunctionFactory[expressions.size()];
        ILogicalExpressionJobGen exprJobGen = context.getExpressionJobGen();
        for (int i = 0; i < aggFactories.length; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) expressions.get(i)
                    .getExpression();
            aggFactories[i] = exprJobGen.createAggregateFunctionFactory(aggFun, inputSchemas, context);
        }

        AggregateRuntimeFactory runtime = new AggregateRuntimeFactory(aggFactories);
        setVarTypes(variables, expressions, context);

        // contribute one Asterix framewriter
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(opSchema, context);
        builder.contributeMicroOperator(aggOp, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = aggOp.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, aggOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    private void setVarTypes(List<LogicalVariable> vars, List<LogicalExpressionReference> exprs, JobGenContext context)
            throws AlgebricksException {
        int n = vars.size();
        for (int i = 0; i < n; i++) {
            ILogicalExpression expr = exprs.get(i).getExpression();
            Object t = context.getType(expr);
            context.setVarType(vars.get(i), t);
        }
    }

}
