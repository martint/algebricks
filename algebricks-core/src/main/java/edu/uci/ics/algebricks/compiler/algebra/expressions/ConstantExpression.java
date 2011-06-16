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
package edu.uci.ics.algebricks.compiler.algebra.expressions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionVisitor;

public final class ConstantExpression extends AbstractLogicalExpression {
    private Object value;

    public final static ConstantExpression TRUE = new ConstantExpression(TrueLiteral.INSTANCE);
    public final static ConstantExpression FALSE = new ConstantExpression(FalseLiteral.INSTANCE);
    public final static ConstantExpression NULL = new ConstantExpression(NullLiteral.INSTANCE);

    private Map<Object, IExpressionAnnotation> annotationMap = new HashMap<Object, IExpressionAnnotation>();

    public ConstantExpression(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public LogicalExpressionTag getExpressionTag() {
        return LogicalExpressionTag.CONSTANT;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        // do nothing
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConstantExpression)) {
            return false;
        } else {
            return value.equals(((ConstantExpression) obj).getValue());
        }
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitConstantExpression(this, arg);
    }

    @Override
    public AbstractLogicalExpression cloneExpression() {
        Map<Object, IExpressionAnnotation> m = new HashMap<Object, IExpressionAnnotation>();
        for (Object k : annotationMap.keySet()) {
            IExpressionAnnotation annot2 = annotationMap.get(k).copy();
            m.put(k, annot2);
        }
        ConstantExpression c = new ConstantExpression(value);
        c.annotationMap = m;
        return c;
    }

    public Map<Object, IExpressionAnnotation> getAnnotations() {
        return annotationMap;
    }

    @Override
    public boolean splitIntoConjuncts(List<LogicalExpressionReference> conjs) {
        return false;
    }
}