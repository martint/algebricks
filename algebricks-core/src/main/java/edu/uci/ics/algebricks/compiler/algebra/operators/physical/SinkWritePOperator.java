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

import edu.uci.ics.algebricks.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.algebricks.api.data.IPrinterFactory;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.exceptions.NotImplementedException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IDataSink;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.WriteOperator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.base.IPushRuntimeFactory;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class SinkWritePOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SINK_WRITE;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ILogicalOperator op2 = op.getInputs().get(0).getOperator();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        WriteOperator write = (WriteOperator) op;
        IDataSink sink = write.getDataSink();
        IPartitioningProperty pp = sink.getPartitioningProperty();
        StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp, null) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        WriteOperator write = (WriteOperator) op;
        int[] printColumns = new int[write.getExpressions().size()];
        int i = 0;
        for (LogicalExpressionReference exprRef : write.getExpressions()) {
            ILogicalExpression expr = exprRef.getExpression();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new NotImplementedException("Only writing variable expressions is supported.");
            }
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable v = varRef.getVariableReference();
            printColumns[i++] = inputSchemas[0].findVariable(v);
        }
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(op, propagatedSchema, context);
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(op.getInputs().get(0).getOperator(),
                inputSchemas[0], context);

        IPrinterFactory[] pf = JobGenHelper.mkPrinterFactories(inputSchemas[0], context.getTypeEnvironment(op),
                context, printColumns);

        IMetadataProvider<?, ?> mp = context.getMetadataProvider();

        Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> runtime = mp.getWriteFileRuntime(write.getDataSink(),
                printColumns, pf, inputDesc);

        builder.contributeMicroOperator(write, runtime.first, recDesc, runtime.second);
        ILogicalOperator src = write.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, write, 0);
    }

}
