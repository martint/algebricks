package edu.uci.ics.algebricks.compiler.algebra.operators.physical;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.algebricks.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.InsertDeleteOperator.Kind;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.LocalOrderProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.OrderColumn;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class IndexInsertDeletePOperator extends AbstractPhysicalOperator {

    private List<LogicalVariable> primaryKeys;
    private List<LogicalVariable> secondaryKeys;

    public IndexInsertDeletePOperator(List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys) {
        this.primaryKeys = primaryKeys;
        this.secondaryKeys = secondaryKeys;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INDEX_INSERT_DELETE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getOperator();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        IPartitioningProperty pp = new UnorderedPartitionedProperty(new HashSet<LogicalVariable>(primaryKeys), null);
        List<ILocalStructuralProperty> orderProps = new LinkedList<ILocalStructuralProperty>();
        for (LogicalVariable k : secondaryKeys) {
            orderProps.add(new LocalOrderProperty(new OrderColumn(k, OrderKind.ASC)));
        }
        StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp,
                orderProps) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IndexInsertDeleteOperator insertDeleteOp = (IndexInsertDeleteOperator) op;
        IMetadataProvider<?, ?> mp = context.getMetadataProvider();

        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(op.getInputs().get(0).getOperator(),
                inputSchemas[0], context);

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints = null;
        if (insertDeleteOp.getOperation() == Kind.INSERT)
            runtimeAndConstraints = mp.getIndexInsertRuntime(insertDeleteOp.getDatasetName(),
                    insertDeleteOp.getIndexName(), propagatedSchema, primaryKeys, secondaryKeys, inputDesc, context,
                    spec);
        else
            runtimeAndConstraints = mp.getIndexDeleteRuntime(insertDeleteOp.getDatasetName(),
                    insertDeleteOp.getIndexName(), propagatedSchema, primaryKeys, secondaryKeys, inputDesc, context,
                    spec);

        builder.contributeHyracksOperator(insertDeleteOp, runtimeAndConstraints.first);
        builder.contributeAlgebricksPartitionConstraint(runtimeAndConstraints.first, runtimeAndConstraints.second);
        ILogicalOperator src = insertDeleteOp.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, insertDeleteOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

}
