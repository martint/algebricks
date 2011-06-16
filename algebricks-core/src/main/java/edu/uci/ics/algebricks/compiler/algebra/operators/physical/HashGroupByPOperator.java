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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.ILogicalExpressionJobGen;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.LocalGroupingProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.base.IAggregateFunctionFactory;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;

public class HashGroupByPOperator extends AbstractPhysicalOperator {

    private int tableSize;
    private List<LogicalVariable> columnSet = new ArrayList<LogicalVariable>();

    public HashGroupByPOperator(List<Pair<LogicalVariable, LogicalExpressionReference>> gbyList, int tableSize) {
        this.tableSize = tableSize;
        computeColumnSet(gbyList);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HASH_GROUP_BY;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnSet;
    }

    public List<LogicalVariable> getGbyColumns() {
        return columnSet;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();

        GroupByOperator gOp = (GroupByOperator) op;
        HashSet<LogicalVariable> columnSet = new HashSet<LogicalVariable>();

        if (!columnSet.isEmpty()) {
            propsLocal.add(new LocalGroupingProperty(columnSet));
        }
        for (ILogicalPlan p : gOp.getNestedPlans()) {
            for (LogicalOperatorReference r : p.getRoots()) {
                ILogicalOperator rOp = r.getOperator();
                propsLocal.addAll(rOp.getDeliveredPhysicalProperties().getLocalProperties());
            }
        }

        ILogicalOperator op2 = op.getInputs().get(0).getOperator();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(), propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new UnorderedPartitionedProperty(new HashSet<LogicalVariable>(
                    columnSet), null), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnSet, inputSchemas[0]);
        GroupByOperator gby = (GroupByOperator) op;
        for (Pair<LogicalVariable, LogicalExpressionReference> p : gby.getGroupByList()) {
            ILogicalExpression expr = p.second.getExpression();
            if (p.first != null) {
                context.setVarType(p.first, context.getType(expr));
            }
        }

        int numFds = gby.getDecorList().size();
        int fdColumns[] = new int[numFds];
        int j = 0;
        for (Pair<LogicalVariable, LogicalExpressionReference> p : gby.getDecorList()) {
            ILogicalExpression expr = p.second.getExpression();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new AlgebricksException("pre-sorted group-by expects variable references.");
            }
            if (p.first != null) {
                context.setVarType(p.first, context.getType(p.second.getExpression()));
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchemas[0].findVariable(decor);
        }

        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "Hash group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "Hash group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        LogicalOperatorReference r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getOperator();

        int n = aggOp.getExpressions().size();
        IAggregateFunctionFactory[] aff = new IAggregateFunctionFactory[n];
        int i = 0;
        ILogicalExpressionJobGen exprJobGen = context.getExpressionJobGen();
        for (LogicalExpressionReference exprRef : aggOp.getExpressions()) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getExpression();
            aff[i++] = exprJobGen.createAggregateFunctionFactory(aggFun, inputSchemas, context);
        }

        IAccumulatingAggregatorFactory aggregatorFactory = new SimpleAlgebricksAccumulatingAggregatorFactory(aff, keys,
                fdColumns);
        compileSubplans(inputSchemas[0], gby, opSchema, context);
        JobSpecification spec = builder.getJobSpec();
        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                columnSet, context);
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(opSchema, context);
        IBinaryHashFunctionFactory[] hashFunctionFactories = JobGenHelper.variablesToBinaryHashFunctionFactories(
                columnSet, context);

        ITuplePartitionComputerFactory tpcf = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories);
        HashGroupOperatorDescriptor gbyOpDesc = new HashGroupOperatorDescriptor(spec, keys, tpcf, comparatorFactories,
                aggregatorFactory, recordDescriptor, tableSize);

        contributeOpDesc(builder, gby, gbyOpDesc);

        ILogicalOperator src = op.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    public void computeColumnSet(List<Pair<LogicalVariable, LogicalExpressionReference>> gbyList) {
        columnSet.clear();
        for (Pair<LogicalVariable, LogicalExpressionReference> p : gbyList) {
            ILogicalExpression expr = p.second.getExpression();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                columnSet.add(v.getVariableReference());
            }
        }
    }

}
