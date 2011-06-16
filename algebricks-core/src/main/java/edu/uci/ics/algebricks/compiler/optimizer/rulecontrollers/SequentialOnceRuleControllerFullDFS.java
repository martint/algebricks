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
package edu.uci.ics.algebricks.compiler.optimizer.rulecontrollers;

import java.util.Collection;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.optimizer.base.AbstractRuleController;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;

public class SequentialOnceRuleControllerFullDFS extends AbstractRuleController {

    public SequentialOnceRuleControllerFullDFS() {
    }

    public SequentialOnceRuleControllerFullDFS(IOptimizationContext context) {
        super(context);
    }

    @Override
    public boolean rewriteWithRuleCollection(LogicalOperatorReference root, Collection<IAlgebraicRewriteRule> rules)
            throws AlgebricksException {
        boolean fired = false;
        for (IAlgebraicRewriteRule rule : rules) {
            if (rewriteOperatorRef(root, rule, true, true)) {
                fired = true;
            }
        }
        return fired;
    }

}
