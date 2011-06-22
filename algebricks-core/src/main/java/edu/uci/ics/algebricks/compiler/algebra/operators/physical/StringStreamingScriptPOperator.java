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

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.algebricks.compiler.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.algebricks.compiler.algebra.properties.PhysicalRequirements;
import edu.uci.ics.algebricks.compiler.algebra.scripting.IScriptDescription;
import edu.uci.ics.algebricks.compiler.algebra.scripting.IScriptDescription.ScriptKind;
import edu.uci.ics.algebricks.compiler.algebra.scripting.StringStreamingScriptDescription;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.base.IHyracksJobBuilder;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenHelper;
import edu.uci.ics.algebricks.runtime.hyracks.operators.std.StringStreamingRuntimeFactory;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class StringStreamingScriptPOperator extends AbstractPropagatePropertiesForUsedVariablesPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STRING_STREAM_SCRIPT;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        ScriptOperator scriptOp = (ScriptOperator) op;
        IScriptDescription scriptDesc = scriptOp.getScriptDescription();
        if (scriptDesc.getKind() != ScriptKind.STRING_STREAMING) {
            throw new IllegalStateException();
        }
        StringStreamingScriptDescription sssd = (StringStreamingScriptDescription) scriptDesc;
        for (Pair<LogicalVariable, Object> p : scriptDesc.getVarTypePairs()) {
            context.setVarType(p.first, p.second);
        }
        StringStreamingRuntimeFactory runtime = new StringStreamingRuntimeFactory(sssd.getCommand(),
                sssd.getPrinterFactories(), sssd.getFieldDelimiter(), sssd.getParserFactory());
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(propagatedSchema, context);
        builder.contributeMicroOperator(scriptOp, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = scriptOp.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, scriptOp, 0);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ScriptOperator s = (ScriptOperator) op;
        computeDeliveredPropertiesForUsedVariables(s, s.getInputVariables());
    }

}
