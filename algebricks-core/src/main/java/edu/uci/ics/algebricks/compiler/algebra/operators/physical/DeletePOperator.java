package edu.uci.ics.algebricks.compiler.algebra.operators.physical;

import java.util.List;

import edu.uci.ics.algebricks.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.InsertOperator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class DeletePOperator extends AbstractPhysicalOperator {

    private List<LogicalVariable> keys;

    public DeletePOperator(List<LogicalVariable> keys) {
        this.keys = keys;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.DELETE;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return null;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op2 = op.getInputs().get(0).getOperator();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        InsertOperator insertOp = (InsertOperator) op;
        IMetadataProvider<?, ?> mp = context.getMetadataProvider();

        JobSpecification spec = builder.getJobSpec();
        
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(op.getInputs().get(0).getOperator(),
                inputSchemas[0], context);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints = mp.getDeleteRuntime(
                insertOp.getDatasetName(), propagatedSchema, keys, inputDesc, context, spec);

        builder.contributeHyracksOperator(insertOp, runtimeAndConstraints.first);
        builder.contributeAlgebricksPartitionConstraint(runtimeAndConstraints.first, runtimeAndConstraints.second);
        ILogicalOperator src = insertOp.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, insertOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

}
