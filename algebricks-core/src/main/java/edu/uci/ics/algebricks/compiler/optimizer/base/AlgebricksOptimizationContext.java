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
package edu.uci.ics.algebricks.compiler.optimizer.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.uci.ics.algebricks.api.expr.IExpressionEvalSizeComputer;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.IMergeAggregationExpressionFactory;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableEvalSizeEnvironment;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.EquivalenceClass;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.algebra.properties.FunctionalDependency;
import edu.uci.ics.algebricks.compiler.algebra.properties.ILogicalPropertiesVector;

public class AlgebricksOptimizationContext implements IOptimizationContext {

    private int varCounter;
    private final IExpressionEvalSizeComputer expressionEvalSizeComputer;
    private final IMergeAggregationExpressionFactory mergeAggregationExpressionFactory;
    private final PhysicalOptimizationConfig physicalOptimizationConfig;
    private final IVariableEvalSizeEnvironment varEvalSizeEnv = new IVariableEvalSizeEnvironment() {

        Map<LogicalVariable, Integer> varSizeMap = new HashMap<LogicalVariable, Integer>();

        @Override
        public void setVariableEvalSize(LogicalVariable var, int size) {
            varSizeMap.put(var, size);
        }

        @Override
        public int getVariableEvalSize(LogicalVariable var) {
            return varSizeMap.get(var);
        }
    };

    private Map<ILogicalOperator, IVariableTypeEnvironment> typeEnvMap = new HashMap<ILogicalOperator, IVariableTypeEnvironment>();

    private Map<ILogicalOperator, HashSet<ILogicalOperator>> alreadyCompared = new HashMap<ILogicalOperator, HashSet<ILogicalOperator>>();
    private Map<IAlgebraicRewriteRule, HashSet<ILogicalOperator>> dontApply = new HashMap<IAlgebraicRewriteRule, HashSet<ILogicalOperator>>();
    private Map<LogicalVariable, FunctionalDependency> recordToPrimaryKey = new HashMap<LogicalVariable, FunctionalDependency>();

    @SuppressWarnings("unchecked")
    private IMetadataProvider metadataProvider;
    private HashSet<LogicalVariable> notToBeInlinedVars = new HashSet<LogicalVariable>();

    protected final Map<ILogicalOperator, List<FunctionalDependency>> fdGlobalMap = new HashMap<ILogicalOperator, List<FunctionalDependency>>();
    protected final Map<ILogicalOperator, Map<LogicalVariable, EquivalenceClass>> eqClassGlobalMap = new HashMap<ILogicalOperator, Map<LogicalVariable, EquivalenceClass>>();

    protected final Map<ILogicalOperator, ILogicalPropertiesVector> logicalProps = new HashMap<ILogicalOperator, ILogicalPropertiesVector>();
    private final int frameSize;
    private final IExpressionTypeComputer expressionTypeComputer;
    private final INullableTypeComputer nullableTypeComputer;

    public AlgebricksOptimizationContext(int varCounter, int frameSize,
            IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
            PhysicalOptimizationConfig physicalOptimizationConfig) {
        this.varCounter = varCounter;
        this.frameSize = frameSize;
        this.expressionEvalSizeComputer = expressionEvalSizeComputer;
        this.mergeAggregationExpressionFactory = mergeAggregationExpressionFactory;
        this.expressionTypeComputer = expressionTypeComputer;
        this.nullableTypeComputer = nullableTypeComputer;
        this.physicalOptimizationConfig = physicalOptimizationConfig;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public void setVarCounter(int varCounter) {
        this.varCounter = varCounter;
    }

    public LogicalVariable newVar() {
        varCounter++;
        LogicalVariable var = new LogicalVariable(varCounter);
        return var;
    }

    @SuppressWarnings("unchecked")
    public IMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

    public void setMetadataDeclarations(IMetadataProvider<?, ?> metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    public boolean checkIfInDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op) {
        HashSet<ILogicalOperator> operators = dontApply.get(rule);
        if (operators == null) {
            return false;
        } else {
            return operators.contains(op);
        }
    }

    public void addToDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op) {
        HashSet<ILogicalOperator> operators = dontApply.get(rule);
        if (operators == null) {
            HashSet<ILogicalOperator> os = new HashSet<ILogicalOperator>();
            os.add(op);
            dontApply.put(rule, os);
        } else {
            operators.add(op);
        }

    }

    /*
     * returns true if op1 and op2 have already been compared
     */
    public boolean checkAndAddToAlreadyCompared(ILogicalOperator op1, ILogicalOperator op2) {
        HashSet<ILogicalOperator> ops = alreadyCompared.get(op1);
        if (ops == null) {
            HashSet<ILogicalOperator> newEntry = new HashSet<ILogicalOperator>();
            newEntry.add(op2);
            alreadyCompared.put(op1, newEntry);
            return false;
        } else {
            if (ops.contains(op2)) {
                return true;
            } else {
                ops.add(op2);
                return false;
            }
        }
    }

    public void addNotToBeInlinedVar(LogicalVariable var) {
        notToBeInlinedVars.add(var);
    }

    public boolean shouldNotBeInlined(LogicalVariable var) {
        return notToBeInlinedVars.contains(var);
    }

    public void addPrimaryKey(FunctionalDependency pk) {
        assert (pk.getTail().size() == 1);
        LogicalVariable recordVar = pk.getTail().get(0);
        recordToPrimaryKey.put(recordVar, pk);
    }

    public List<LogicalVariable> findPrimaryKey(LogicalVariable recordVar) {
        FunctionalDependency fd = recordToPrimaryKey.get(recordVar);
        if (fd == null) {
            return null;
        }
        return fd.getHead();
    }

    @Override
    public Map<LogicalVariable, EquivalenceClass> getEquivalenceClassMap(ILogicalOperator op) {
        return eqClassGlobalMap.get(op);
    }

    @Override
    public List<FunctionalDependency> getFDList(ILogicalOperator op) {
        return fdGlobalMap.get(op);
    }

    @Override
    public void putEquivalenceClassMap(ILogicalOperator op, Map<LogicalVariable, EquivalenceClass> eqClassMap) {
        this.eqClassGlobalMap.put(op, eqClassMap);
    }

    @Override
    public void putFDList(ILogicalOperator op, List<FunctionalDependency> fdList) {
        this.fdGlobalMap.put(op, fdList);
    }

    @Override
    public ILogicalPropertiesVector getLogicalPropertiesVector(ILogicalOperator op) {
        return logicalProps.get(op);
    }

    @Override
    public void putLogicalPropertiesVector(ILogicalOperator op, ILogicalPropertiesVector v) {
        logicalProps.put(op, v);
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return expressionEvalSizeComputer;
    }

    @Override
    public IVariableEvalSizeEnvironment getVariableEvalSizeEnvironment() {
        return varEvalSizeEnv;
    }

    @Override
    public int getFrameSize() {
        return frameSize;
    }

    public IMergeAggregationExpressionFactory getMergeAggregationExpressionFactory() {
        return mergeAggregationExpressionFactory;
    }

    public PhysicalOptimizationConfig getPhysicalOptimizationConfig() {
        return physicalOptimizationConfig;
    }

    @Override
    public IVariableTypeEnvironment getTypeEnvironment(ILogicalOperator op) {
        return typeEnvMap.get(op);
    }

    @Override
    public void setTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env) {
        typeEnvMap.put(op, env);
    }

    @Override
    public IExpressionTypeComputer getExpressionTypeComputer() {
        return expressionTypeComputer;
    }

    @Override
    public INullableTypeComputer getNullableTypeComputer() {
        return nullableTypeComputer;
    }

}