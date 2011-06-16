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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.EquivalenceClass;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.properties.FunctionalDependency;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILocalStructuralProperty.PropertyType;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.LocalGroupingProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.LocalOrderProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.OrderColumn;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.PropertiesUtil;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.base.AlgebricksPipeline;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.runtime.hyracks.operators.aggreg.NestedPlansAccumulatingAggregatorFactory;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;

public class PreClusteredGroupByPOperator extends AbstractPhysicalOperator {

    private List<LogicalVariable> columnList;

    public PreClusteredGroupByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnList;
    }

    public List<LogicalVariable> getGbyColumns() {
        return columnList;
    }

    public void setGbyColumns(List<LogicalVariable> gByColumns) {
        this.columnList = gByColumns;
    }

    // Obs: We don't propagate properties corresponding to decors, since they
    // are func. dep. on the group-by variables.
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();
        GroupByOperator gby = (GroupByOperator) op;
        ILogicalOperator op2 = gby.getInputs().get(0).getOperator();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        IPartitioningProperty pp = childProp.getPartitioningProperty();
        List<ILocalStructuralProperty> childLocals = childProp.getLocalProperties();
        if (childLocals != null) {
            for (ILocalStructuralProperty lsp : childLocals) {
                boolean failed = false;
                switch (lsp.getPropertyType()) {
                    case LOCAL_GROUPING_PROPERTY: {
                        LocalGroupingProperty lgp = (LocalGroupingProperty) lsp;
                        Set<LogicalVariable> colSet = new HashSet<LogicalVariable>();
                        for (LogicalVariable v : lgp.getColumnSet()) {
                            LogicalVariable v2 = getLhsGbyVar(gby, v);
                            if (v2 != null) {
                                colSet.add(v2);
                            } else {
                                failed = true;
                            }
                        }
                        if (!failed) {
                            propsLocal.add(new LocalGroupingProperty(colSet));
                        }
                        break;
                    }
                    case LOCAL_ORDER_PROPERTY: {
                        LocalOrderProperty lop = (LocalOrderProperty) lsp;
                        OrderColumn oc = lop.getOrderColumn();
                        LogicalVariable v2 = getLhsGbyVar(gby, oc.getColumn());
                        if (v2 != null) {
                            propsLocal.add(new LocalOrderProperty(new OrderColumn(v2, oc.getOrder())));
                        } else {
                            failed = true;
                        }
                        break;
                    }
                    default: {
                        throw new IllegalStateException();
                    }
                }
                if (failed) {
                    break;
                }
            }
        }
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
        List<ILocalStructuralProperty> localProps = null;

        localProps = new ArrayList<ILocalStructuralProperty>(1);
        Set<LogicalVariable> gbvars = new HashSet<LogicalVariable>(columnList);
        localProps.add(new LocalGroupingProperty(gbvars, columnList));

        if (reqdByParent != null) {
            List<ILocalStructuralProperty> lpPar = reqdByParent.getLocalProperties();
            // List<LogicalVariable> covered = new ArrayList<LogicalVariable>();
            GroupByOperator gby = (GroupByOperator) op;
            if (lpPar != null) {
                boolean allOk = true;
                List<ILocalStructuralProperty> props = new ArrayList<ILocalStructuralProperty>(lpPar.size());
                for (ILocalStructuralProperty prop : lpPar) {
                    if (prop.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
                        allOk = false;
                        break;
                    }
                    LocalOrderProperty lop = (LocalOrderProperty) prop;
                    LogicalVariable ord = lop.getColumn();
                    Pair<LogicalVariable, LogicalExpressionReference> p = getGbyPairByRhsVar(gby, ord);
                    if (p == null) {
                        p = getDecorPairByRhsVar(gby, ord);
                        if (p == null) {
                            allOk = false;
                            break;
                        }
                    }
                    ILogicalExpression e = p.second.getExpression();
                    if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new IllegalStateException(
                                "Right hand side of group by assignment should have been normalized to a variable reference.");
                    }
                    LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
                    // covered.add(v);
                    props.add(new LocalOrderProperty(new OrderColumn(v, lop.getOrder())));
                }
                List<FunctionalDependency> fdList = new ArrayList<FunctionalDependency>();
                for (Pair<LogicalVariable, LogicalExpressionReference> decorPair : gby.getDecorList()) {
                    List<LogicalVariable> hd = gby.getGbyVarList();
                    List<LogicalVariable> tl = new ArrayList<LogicalVariable>(1);
                    tl.add(((VariableReferenceExpression) decorPair.second.getExpression()).getVariableReference());
                    fdList.add(new FunctionalDependency(hd, tl));
                }
                if (allOk
                        && PropertiesUtil.matchLocalProperties(localProps, props,
                                new HashMap<LogicalVariable, EquivalenceClass>(), fdList)) {
                    localProps = props;
                }
            }
        }

        IPartitioningProperty pp = null;
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            pp = new UnorderedPartitionedProperty(new HashSet<LogicalVariable>(columnList), null);
        }
        pv[0] = new StructuralPropertiesVector(pp, localProps);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchemas[0]);
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
                context.setVarType(p.first, context.getType(expr));
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchemas[0].findVariable(decor);
        }
        // compile subplans and set the gby op. schema accordingly
        AlgebricksPipeline[] subplans = compileSubplans(inputSchemas[0], gby, opSchema, context);
        IAccumulatingAggregatorFactory aggregatorFactory = new NestedPlansAccumulatingAggregatorFactory(subplans, keys,
                fdColumns);

        JobSpecification spec = builder.getJobSpec();
        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                columnList, context);
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(opSchema, context);

        PreclusteredGroupOperatorDescriptor opDesc = new PreclusteredGroupOperatorDescriptor(spec, keys,
                comparatorFactories, aggregatorFactory, recordDescriptor);

        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src = op.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    private static Pair<LogicalVariable, LogicalExpressionReference> getGbyPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getGroupByList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static Pair<LogicalVariable, LogicalExpressionReference> getDecorPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getDecorList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static LogicalVariable getLhsGbyVar(GroupByOperator gby, LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getGroupByList()) {
            ILogicalExpression e = ve.second.getExpression();
            if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new IllegalStateException(
                        "Right hand side of group by assignment should have been normalized to a variable reference.");
            }
            LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
            if (v == var) {
                return ve.first;
            }
        }
        return null;
    }

}
