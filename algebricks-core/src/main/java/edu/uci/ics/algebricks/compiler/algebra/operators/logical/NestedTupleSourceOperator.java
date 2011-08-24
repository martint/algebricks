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
package edu.uci.ics.algebricks.compiler.algebra.operators.logical;

import java.util.ArrayList;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.TypePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypeEnvPointer;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypingContext;
import edu.uci.ics.algebricks.compiler.algebra.typing.OpRefTypeEnvPointer;
import edu.uci.ics.algebricks.compiler.algebra.typing.PropagatingTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class NestedTupleSourceOperator extends AbstractLogicalOperator {
    private LogicalOperatorReference dataSourceReference;

    public NestedTupleSourceOperator() {
    }

    public NestedTupleSourceOperator(LogicalOperatorReference dataSourceReference) {
        this.dataSourceReference = dataSourceReference;
    }

    public ILogicalOperator getSourceOperator() {
        return dataSourceReference.getOperator().getInputs().get(0).getOperator();
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.NESTEDTUPLESOURCE;
    }

    public LogicalOperatorReference getDataSourceReference() {
        return dataSourceReference;
    }

    public void setDataSourceReference(LogicalOperatorReference dataSourceReference) {
        this.dataSourceReference = dataSourceReference;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        ILogicalOperator topOp = dataSourceReference.getOperator();
        for (LogicalOperatorReference i : topOp.getInputs()) {
            schema.addAll(i.getOperator().getSchema());
        }
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitNestedTupleSourceOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ITypeEnvPointer[] p = new ITypeEnvPointer[1];
        p[0] = new OpRefTypeEnvPointer(dataSourceReference.getOperator().getInputs().get(0), ctx);
        return new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getNullableTypeComputer(),
                TypePropagationPolicy.ALL, p);
    }

}