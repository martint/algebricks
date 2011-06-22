package edu.uci.ics.algebricks.compiler.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.OperatorAnnotations;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IDataSource;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.LimitOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.OrderOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.AggregatePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.AssignPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.DataSourceScanPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.EmptyTupleSourcePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.ExternalGroupByPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.HashGroupByPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.InMemoryStableSortPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.NestedTupleSourcePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.PreClusteredGroupByPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.PreSortedDistinctByPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.ReplicatePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.RunningAggregatePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.SinkWritePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StableSortPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StreamLimitPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StreamSelectPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StringStreamingScriptPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.SubplanPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.UnionAllPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.UnnestPOperator;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.compiler.optimizer.base.JoinUtils;
import edu.uci.ics.algebricks.compiler.optimizer.base.PhysicalOptimizationsUtil;
import edu.uci.ics.algebricks.utils.Pair;

public class SetAlgebricksPhysicalOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        // if (context.checkIfInDontApplySet(this, op)) {
        // return false;
        // }
        if (op.getPhysicalOperator() != null) {
            return false;
        }

        computeDefaultPhysicalOp(op, true, context);
        // context.addToDontApplySet(this, op);
        return true;
    }

    private static void setPhysicalOperators(ILogicalPlan plan, boolean topLevelOp, IOptimizationContext context)
            throws AlgebricksException {
        for (LogicalOperatorReference root : plan.getRoots()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) root.getOperator(), topLevelOp, context);
        }
    }

    @SuppressWarnings("unchecked")
    private static void computeDefaultPhysicalOp(AbstractLogicalOperator op, boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (op.getPhysicalOperator() == null) {
            switch (op.getOperatorTag()) {
                case AGGREGATE: {
                    op.setPhysicalOperator(new AggregatePOperator());
                    break;
                }
                case ASSIGN: {
                    op.setPhysicalOperator(new AssignPOperator());
                    break;
                }
                case EMPTYTUPLESOURCE: {
                    op.setPhysicalOperator(new EmptyTupleSourcePOperator());
                    break;
                }
                case EXCHANGE: {
                    if (op.getPhysicalOperator() == null) {
                        throw new AlgebricksException("Implementation for EXCHANGE operator was not set.");
                    }
                    // implem. choice for exchange should be set by a parent op.
                    break;
                }
                case GROUP: {

                    GroupByOperator gby = (GroupByOperator) op;

                    if (gby.getNestedPlans().size() == 1) {
                        ILogicalPlan p0 = gby.getNestedPlans().get(0);
                        if (p0.getRoots().size() == 1) {
                            if (gby.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY) == Boolean.TRUE) {
                                // LogicalOperatorReference r0 =
                                // p0.getRoots().get(0);
                                // AbstractLogicalOperator op1 =
                                // (AbstractLogicalOperator) r0.getOperator();
                                // if (op1.getOperatorTag() ==
                                // LogicalOperatorTag.AGGREGATE) {
                                // AbstractLogicalOperator op2 =
                                // (AbstractLogicalOperator)
                                // op1.getInputs().get(0)
                                // .getOperator();
                                // if (op2.getOperatorTag() ==
                                // LogicalOperatorTag.NESTEDTUPLESOURCE) {
                                HashGroupByPOperator hashGby = new HashGroupByPOperator(
                                        ((GroupByOperator) op).getGroupByList(),
                                        PhysicalOptimizationsUtil.DEFAULT_HASH_GROUP_TABLE_SIZE);
                                op.setPhysicalOperator(hashGby);
                                break;
                                // }
                                // }
                            }
                            if (gby.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY) == Boolean.TRUE) {
                                // LogicalOperatorReference r0 =
                                // p0.getRoots().get(0);
                                // AbstractLogicalOperator op1 =
                                // (AbstractLogicalOperator) r0.getOperator();
                                // if (op1.getOperatorTag() ==
                                // LogicalOperatorTag.AGGREGATE) {
                                // AbstractLogicalOperator op2 =
                                // (AbstractLogicalOperator)
                                // op1.getInputs().get(0)
                                // .getOperator();
                                // if (op2.getOperatorTag() ==
                                // LogicalOperatorTag.NESTEDTUPLESOURCE) {
                                Boolean localGby = (Boolean) gby.getAnnotations().get(OperatorAnnotations.LOCAL_GBY);
                                if (localGby == null)
                                    localGby = false;
                                HashGroupByPOperator externalGby = new ExternalGroupByPOperator(
                                        ((GroupByOperator) op).getGroupByList(),
                                        PhysicalOptimizationsUtil.DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, localGby);
                                op.setPhysicalOperator(externalGby);
                                break;
                                // }
                                // }
                            }
                        }
                    }

                    List<Pair<LogicalVariable, LogicalExpressionReference>> gbyList = gby.getGroupByList();
                    List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                    for (Pair<LogicalVariable, LogicalExpressionReference> p : gbyList) {
                        ILogicalExpression expr = p.second.getExpression();
                        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                            columnList.add(varRef.getVariableReference());
                        }
                    }
                    op.setPhysicalOperator(new PreClusteredGroupByPOperator(columnList));
                    break;
                }
                case INNERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((InnerJoinOperator) op, context);
                    break;
                }
                case LEFTOUTERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((LeftOuterJoinOperator) op, context);
                    break;
                }
                case LIMIT: {
                    LimitOperator opLim = (LimitOperator) op;
                    op.setPhysicalOperator(new StreamLimitPOperator(opLim.isTopmostLimitOp()));
                    break;
                }
                case NESTEDTUPLESOURCE: {
                    LogicalOperatorReference nestedPlanCreator = ((NestedTupleSourceOperator) op)
                            .getDataSourceReference();
                    op.setPhysicalOperator(new NestedTupleSourcePOperator(nestedPlanCreator));
                    break;
                }
                case ORDER: {
                    OrderOperator oo = (OrderOperator) op;
                    for (Pair<IOrder, LogicalExpressionReference> p : oo.getOrderExpressions()) {
                        ILogicalExpression e = p.second.getExpression();
                        if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            throw new AlgebricksException("Order expression " + e + " has not been normalized.");
                        }
                    }
                    if (topLevelOp) {
                        op.setPhysicalOperator(new StableSortPOperator(
                                PhysicalOptimizationsUtil.MAX_FRAMES_EXTERNAL_SORT));
                    } else {
                        op.setPhysicalOperator(new InMemoryStableSortPOperator());
                    }
                    break;
                }
                case PROJECT: {
                    op.setPhysicalOperator(new StreamProjectPOperator());
                    break;
                }
                case RUNNINGAGGREGATE: {
                    op.setPhysicalOperator(new RunningAggregatePOperator());
                    break;
                }
                case REPLICATE: {
                    op.setPhysicalOperator(new ReplicatePOperator());
                    break;
                }
                case SCRIPT: {
                    op.setPhysicalOperator(new StringStreamingScriptPOperator());
                    break;
                }
                case SELECT: {
                    op.setPhysicalOperator(new StreamSelectPOperator());
                    break;
                }
                case SUBPLAN: {
                    op.setPhysicalOperator(new SubplanPOperator());
                    break;
                }
                case UNIONALL: {
                    op.setPhysicalOperator(new UnionAllPOperator());
                    break;
                }

                case UNNEST: {
                    op.setPhysicalOperator(new UnnestPOperator());
                    break;
                }
                case DATASOURCESCAN: {
                    DataSourceScanOperator scan = (DataSourceScanOperator) op;
                    IDataSource dataSource = scan.getDataSource();
                    DataSourceScanPOperator dss = new DataSourceScanPOperator(dataSource);
                    IMetadataProvider mp = context.getMetadataProvider();
                    if (mp.scannerOperatorIsLeaf(dataSource)) {
                        dss.disableJobGenBelowMe();
                    }
                    op.setPhysicalOperator(dss);
                    break;
                }
                case WRITE: {
                    op.setPhysicalOperator(new SinkWritePOperator());
                    break;
                }
                case DISTINCT: {
                    DistinctOperator distinct = (DistinctOperator) op;
                    distinct.setPhysicalOperator(new PreSortedDistinctByPOperator(distinct.getDistinctByVarList()));
                    break;
                }
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                setPhysicalOperators(p, false, context);
            }
        }
        for (LogicalOperatorReference opRef : op.getInputs()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) opRef.getOperator(), topLevelOp, context);
        }
    }

}
