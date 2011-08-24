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
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalExpression;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypingContext;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class EmptyTupleSourceOperator extends AbstractLogicalOperator {

    // public final static EmptyTupleSourceOperator INSTANCE = new
    // EmptyTupleSourceOperator();

    public EmptyTupleSourceOperator() {
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.EMPTYTUPLESOURCE;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.NONE;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitEmptyTupleSourceOperator(this, arg);
    }

    //
    // @Override
    // public void computeConstraintsAndEquivClasses() {
    // }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeTypeEnvironment(final ITypingContext ctx) throws AlgebricksException {
        return new IVariableTypeEnvironment() {

            @Override
            public void setVarType(LogicalVariable var, Object type) {
                throw new IllegalStateException();
            }

            @Override
            public Object getVarType(LogicalVariable var) throws AlgebricksException {
                return null;
            }

            @Override
            public Object getType(ILogicalExpression expr) throws AlgebricksException {
                return ctx.getExpressionTypeComputer().getType(expr, this);
            }
        };
    }

}