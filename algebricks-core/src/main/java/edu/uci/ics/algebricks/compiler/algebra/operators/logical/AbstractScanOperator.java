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
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;

public abstract class AbstractScanOperator extends AbstractLogicalOperator {
    protected ArrayList<LogicalVariable> variables;

    public AbstractScanOperator(ArrayList<LogicalVariable> variables) {
        this.variables = variables;
    }

    public ArrayList<LogicalVariable> getVariables() {
        return variables;
    }

    public void setVariables(ArrayList<LogicalVariable> variables) {
        this.variables = variables;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getOperator().getSchema());
        schema.addAll(variables);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

}
