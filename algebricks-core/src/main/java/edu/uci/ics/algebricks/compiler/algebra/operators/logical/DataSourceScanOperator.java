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
import java.util.Collection;
import java.util.List;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IDataSource;
import edu.uci.ics.algebricks.compiler.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.algebricks.compiler.algebra.visitors.ILogicalOperatorVisitor;

public class DataSourceScanOperator extends AbstractScanOperator {

    private IDataSource<?> dataSource;

    private List<LogicalVariable> projectVars = new ArrayList<LogicalVariable>();

    private boolean projectPushed = false;

    public DataSourceScanOperator(ArrayList<LogicalVariable> variables, IDataSource<?> dataSource) {
        super(variables);
        this.dataSource = dataSource;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.DATASOURCESCAN;
    }

    public IDataSource<?> getDataSource() {
        return dataSource;
    }

    @Override
    public <R, S> R accept(ILogicalOperatorVisitor<R, S> visitor, S arg) throws AlgebricksException {
        return visitor.visitDataScanOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    public void addProjectVariables(Collection<LogicalVariable> vars) {
        projectVars.addAll(vars);
        projectPushed = true;
    }

    public List<LogicalVariable> getProjectVariables() {
        return projectVars;
    }

    public boolean isProjectPushed() {
        return projectPushed;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                List<LogicalVariable> outputVariables = projectPushed ? projectVars : variables;
                for (LogicalVariable v : outputVariables) {
                    target.addVariable(v);
                }
            }
        };
    }

}
