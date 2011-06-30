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
package edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.algebricks.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.algebricks.api.data.IBinaryBooleanInspector;
import edu.uci.ics.algebricks.api.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.algebricks.api.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.algebricks.api.data.IBinaryIntegerInspector;
import edu.uci.ics.algebricks.api.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.algebricks.api.data.IPrinterFactoryProvider;
import edu.uci.ics.algebricks.api.data.ISerializerDeserializerProvider;
import edu.uci.ics.algebricks.api.data.ITypeTraitProvider;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionEvalSizeComputer;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.ILogicalExpressionJobGen;
import edu.uci.ics.algebricks.api.expr.IPartialAggregationTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;

public class JobGenContext implements IVariableTypeEnvironment {
    private final IOperatorSchema outerFlowSchema;
    private final Map<ILogicalOperator, IOperatorSchema> schemaMap = new HashMap<ILogicalOperator, IOperatorSchema>();
    private final Map<LogicalVariable, Object> varTypeMap = new HashMap<LogicalVariable, Object>();
    private final ISerializerDeserializerProvider serializerDeserializerProvider;
    private final IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider;
    private final IBinaryComparatorFactoryProvider comparatorFactoryProvider;
    private final IPrinterFactoryProvider printerFactoryProvider;
    private final ITypeTraitProvider typeTraitProvider;
    private final IMetadataProvider<?, ?> metadataProvider;
    private final INullWriterFactory nullWriterFactory;
    private final INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider;
    private final Object appContext;
    private final IBinaryBooleanInspector booleanInspector;
    private final IBinaryIntegerInspector integerInspector;
    private final ILogicalExpressionJobGen exprJobGen;
    private final IExpressionTypeComputer expressionTypeComputer;
    private final IExpressionEvalSizeComputer expressionEvalSizeComputer;
    private final IPartialAggregationTypeComputer partialAggregationTypeComputer;
    private final int frameSize;
    private AlgebricksPartitionConstraint clusterLocations;
    private int varCounter;

    public JobGenContext(IOperatorSchema outerFlowSchema, IMetadataProvider<?, ?> metadataProvider, Object appContext,
            ISerializerDeserializerProvider serializerDeserializerProvider,
            IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider,
            IBinaryComparatorFactoryProvider comparatorFactoryProvider, ITypeTraitProvider typeTraitProvider,
            IBinaryBooleanInspector booleanInspector, IBinaryIntegerInspector integerInspector,
            IPrinterFactoryProvider printerFactoryProvider, INullWriterFactory nullWriterFactory,
            INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider,
            ILogicalExpressionJobGen exprJobGen, IExpressionTypeComputer expressionTypeComputer,
            IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IPartialAggregationTypeComputer partialAggregationTypeComputer, int frameSize,
            AlgebricksPartitionConstraint clusterLocations) {
        this.outerFlowSchema = outerFlowSchema;
        this.metadataProvider = metadataProvider;
        this.appContext = appContext;
        this.serializerDeserializerProvider = serializerDeserializerProvider;
        this.hashFunctionFactoryProvider = hashFunctionFactoryProvider;
        this.comparatorFactoryProvider = comparatorFactoryProvider;
        this.typeTraitProvider = typeTraitProvider;
        this.booleanInspector = booleanInspector;
        this.integerInspector = integerInspector;
        this.printerFactoryProvider = printerFactoryProvider;
        this.clusterLocations = clusterLocations;
        this.normalizedKeyComputerFactoryProvider = normalizedKeyComputerFactoryProvider;
        this.nullWriterFactory = nullWriterFactory;
        this.exprJobGen = exprJobGen;
        this.expressionTypeComputer = expressionTypeComputer;
        this.expressionEvalSizeComputer = expressionEvalSizeComputer;
        this.partialAggregationTypeComputer = partialAggregationTypeComputer;
        this.frameSize = frameSize;
        this.varCounter = 0;
    }

    public IOperatorSchema getOuterFlowSchema() {
        return outerFlowSchema;
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        return clusterLocations;
    }

    @Override
    public IMetadataProvider<?, ?> getMetadataProvider() {
        return metadataProvider;
    }

    public Object getAppContext() {
        return appContext;
    }

    public ISerializerDeserializerProvider getSerializerDeserializerProvider() {
        return serializerDeserializerProvider;
    }

    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return hashFunctionFactoryProvider;
    }

    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return comparatorFactoryProvider;
    }

    public ITypeTraitProvider getTypeTraitProvider() {
        return typeTraitProvider;
    }

    public IBinaryBooleanInspector getBinaryBooleanInspector() {
        return booleanInspector;
    }

    public IBinaryIntegerInspector getBinaryIntegerInspector() {
        return integerInspector;
    }

    public IPrinterFactoryProvider getPrinterFactoryProvider() {
        return printerFactoryProvider;
    }

    public ILogicalExpressionJobGen getExpressionJobGen() {
        return exprJobGen;
    }

    public IOperatorSchema getSchema(ILogicalOperator op) {
        return schemaMap.get(op);
    }

    public void putSchema(ILogicalOperator op, IOperatorSchema schema) {
        schemaMap.put(op, schema);
    }

    @Override
    public Object getVarType(LogicalVariable var) {
        return varTypeMap.get(var);
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        varTypeMap.put(var, type);
    }

    public LogicalVariable createNewVar() {
        varCounter++;
        LogicalVariable var = new LogicalVariable(-varCounter);
        return var;
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        return expressionTypeComputer.getType(expr, this);
    }

    public INullWriterFactory getNullWriterFactory() {
        return nullWriterFactory;
    }

    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return normalizedKeyComputerFactoryProvider;
    }

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return expressionEvalSizeComputer;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public IPartialAggregationTypeComputer getPartialAggregationTypeComputer() {
        return partialAggregationTypeComputer;
    }

}
