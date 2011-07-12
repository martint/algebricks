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

import java.util.HashSet;
import java.util.List;

import edu.uci.ics.algebricks.api.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.properties.INodeDomain;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPartitioningProperty;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder.TargetConstraint;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;

public class HashPartitionExchangePOperator extends AbstractExchangePOperator {

    private List<LogicalVariable> hashFields;
    private INodeDomain domain;

    public HashPartitionExchangePOperator(List<LogicalVariable> hashFields, INodeDomain domain) {
        this.hashFields = hashFields;
        this.domain = domain;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HASH_PARTITION_EXCHANGE;
    }

    public List<LogicalVariable> getHashFields() {
        return hashFields;
    }

    public INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty p = new UnorderedPartitionedProperty(new HashSet<LogicalVariable>(hashFields), domain);
        this.deliveredProperties = new StructuralPropertiesVector(p, null);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + hashFields;
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(JobSpecification spec,
            IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int[] keys = new int[hashFields.size()];
        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[hashFields.size()];
        int i = 0;
        IBinaryHashFunctionFactoryProvider hashFunProvider = context.getBinaryHashFunctionFactoryProvider();
        for (LogicalVariable v : hashFields) {
            keys[i] = opSchema.findVariable(v);
            hashFunctionFactories[i] = hashFunProvider.getBinaryHashFunctionFactory(context.getVarType(v));
            ++i;
        }
        ITuplePartitionComputerFactory tpcf = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories);
        IConnectorDescriptor conn = new MToNHashPartitioningConnectorDescriptor(spec, tpcf);
        return new Pair<IConnectorDescriptor, TargetConstraint>(conn, null);
    }

}
