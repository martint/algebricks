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
package edu.uci.ics.algebricks.compiler.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorTag;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.AssignOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.visitors.IsomorphismUtilities;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.AssignPOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.OneToOneExchangePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.ReplicatePOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;

public class ExtractCommonOperatorsRule implements IAlgebraicRewriteRule {

    private List<LogicalOperatorReference> previousCandidates = new ArrayList<LogicalOperatorReference>();
    private List<LogicalOperatorReference> candidates = new ArrayList<LogicalOperatorReference>();
    private HashMap<LogicalOperatorReference, List<LogicalOperatorReference>> childrenToParents = new HashMap<LogicalOperatorReference, List<LogicalOperatorReference>>();
    private List<LogicalOperatorReference> roots = new ArrayList<LogicalOperatorReference>();
    private List<LogicalOperatorReference> joins = new ArrayList<LogicalOperatorReference>();

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT) {
            return false;
        }
        if (!roots.contains(op))
            roots.add(new LogicalOperatorReference(op));
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT) {
            return false;
        }
        boolean rewritten = false;
        boolean changed = false;
        if (roots.size() > 0) {
            do {
                changed = false;
                // applying the rewriting until fixpoint
                topDownMaterialization(roots);
                removeNonJoinBuildBranchCandidates();
                genCandidates();
                removeTrivialShare();
                removeNonJoinBuildBranchCandidates();
                if (candidates.size() > 0)
                    changed = rewrite();
                if (!rewritten)
                    rewritten = changed;
                previousCandidates.clear();
                candidates.clear();
                childrenToParents.clear();
                joins.clear();
            } while (changed);
            roots.clear();
        }
        return rewritten;
    }

    private void removeTrivialShare() {
        for (int i = candidates.size() - 1; i >= 0; i--) {
            LogicalOperatorReference opRef = candidates.get(i);
            AbstractLogicalOperator aop = (AbstractLogicalOperator) opRef.getOperator();
            if (aop.getOperatorTag() == LogicalOperatorTag.EXCHANGE)
                aop = (AbstractLogicalOperator) aop.getInputs().get(0).getOperator();
            if (aop.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE)
                candidates.remove(i);
        }
    }

    private void removeNonJoinBuildBranchCandidates() {
        for (int i = candidates.size() - 1; i >= 0; i--) {
            LogicalOperatorReference opRef = candidates.get(i);
            boolean reserve = false;
            for (LogicalOperatorReference join : joins)
                if (isInJoinBuildBranch(join, opRef)) {
                    reserve = true;
                }
            if (!reserve)
                candidates.remove(i);
        }
    }

    private boolean isInJoinBuildBranch(LogicalOperatorReference joinRef, LogicalOperatorReference opRef) {
        LogicalOperatorReference buildBranch = joinRef.getOperator().getInputs().get(1);
        do {
            if (buildBranch.equals(opRef)) {
                return true;
            } else {
                AbstractLogicalOperator aop = (AbstractLogicalOperator) buildBranch.getOperator();
                if (aop.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                        || aop.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN
                        || buildBranch.getOperator().getInputs().size() == 0)
                    return false;
                else
                    buildBranch = buildBranch.getOperator().getInputs().get(0);
            }
        } while (true);
    }

    private boolean rewrite() throws AlgebricksException {
        List<LogicalOperatorReference> group = new ArrayList<LogicalOperatorReference>();
        boolean rewritten = false;
        while (candidates.size() > 0) {
            group.clear();
            LogicalOperatorReference candidate = candidates.remove(candidates.size() - 1);
            group.add(candidate);
            for (int i = candidates.size() - 1; i >= 0; i--) {
                LogicalOperatorReference peer = candidates.get(i);
                if (IsomorphismUtilities.isOperatorIsomorphic(candidate.getOperator(), peer.getOperator())) {
                    group.add(peer);
                    candidates.remove(i);
                }
            }
            AbstractLogicalOperator rop = new ReplicateOperator(group.size());
            rop.setPhysicalOperator(new ReplicatePOperator());
            rop.setExecutionMode(ExecutionMode.PARTITIONED);
            LogicalOperatorReference ropRef = new LogicalOperatorReference(rop);
            AbstractLogicalOperator aopCandidate = (AbstractLogicalOperator) candidate.getOperator();

            if (aopCandidate.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                rop.getInputs().add(candidate);
            } else {
                AbstractLogicalOperator beforeExchange = new ExchangeOperator();
                beforeExchange.setPhysicalOperator(new OneToOneExchangePOperator());
                beforeExchange.getInputs().add(candidate);
                rop.getInputs().add(new LogicalOperatorReference(beforeExchange));
            }

            List<LogicalOperatorReference> parents = childrenToParents.get(candidate);
            for (LogicalOperatorReference parentRef : parents) {
                AbstractLogicalOperator parent = (AbstractLogicalOperator) parentRef.getOperator();
                int index = parent.getInputs().indexOf(candidate);
                AbstractLogicalOperator exchange = new ExchangeOperator();
                exchange.setPhysicalOperator(new OneToOneExchangePOperator());
                if (parent.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    parent.getInputs().set(index, ropRef);
                } else {
                    exchange.getInputs().add(ropRef);
                    parent.getInputs().set(index, new LogicalOperatorReference(exchange));
                }
            }

            List<LogicalVariable> liveVarsNew = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(candidate.getOperator(), liveVarsNew);
            ArrayList<LogicalExpressionReference> assignExprs = new ArrayList<LogicalExpressionReference>();
            for (LogicalVariable liveVar : liveVarsNew)
                assignExprs.add(new LogicalExpressionReference(new VariableReferenceExpression(liveVar)));
            for (LogicalOperatorReference ref : group) {
                if (ref.equals(candidate))
                    continue;
                ArrayList<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                Map<LogicalVariable, LogicalVariable> variableMappingBack = new HashMap<LogicalVariable, LogicalVariable>();
                IsomorphismUtilities.mapVariablesTopDown(ref.getOperator(), candidate.getOperator(),
                        variableMappingBack);
                for (int i = 0; i < liveVarsNew.size(); i++) {
                    liveVars.add(variableMappingBack.get(liveVarsNew.get(i)));
                }

                AbstractLogicalOperator assignOperator = new AssignOperator(liveVars, assignExprs);
                assignOperator.setPhysicalOperator(new AssignPOperator());
                AbstractLogicalOperator projectOperator = new ProjectOperator(liveVars);
                projectOperator.setPhysicalOperator(new StreamProjectPOperator());
                AbstractLogicalOperator exchOp = new ExchangeOperator();
                exchOp.setPhysicalOperator(new OneToOneExchangePOperator());
                exchOp.getInputs().add(ropRef);

                assignOperator.getInputs().add(new LogicalOperatorReference(exchOp));
                projectOperator.getInputs().add(new LogicalOperatorReference(assignOperator));

                List<LogicalOperatorReference> parentOpList = childrenToParents.get(ref);
                for (LogicalOperatorReference parentOpRef : parentOpList) {
                    AbstractLogicalOperator parentOp = (AbstractLogicalOperator) parentOpRef.getOperator();
                    int index = parentOp.getInputs().indexOf(ref);
                    if (parentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                        AbstractLogicalOperator parentOpNext = (AbstractLogicalOperator) childrenToParents
                                .get(parentOpRef).get(0).getOperator();
                        if (parentOpNext.isMap()) {
                            index = parentOpNext.getInputs().indexOf(parentOpRef);
                            parentOp = parentOpNext;
                        }
                    }

                    AbstractLogicalOperator exchg = new ExchangeOperator();
                    exchg.setPhysicalOperator(new OneToOneExchangePOperator());

                    ILogicalOperator childOp = parentOp.getOperatorTag() == LogicalOperatorTag.PROJECT ? assignOperator
                            : projectOperator;
                    if (parentOp.isMap()) {
                        parentOp.getInputs().set(index, new LogicalOperatorReference(childOp));
                    } else {
                        exchg.getInputs().add(new LogicalOperatorReference(childOp));
                        parentOp.getInputs().set(index, new LogicalOperatorReference(exchg));
                    }
                }
            }
            rewritten = true;
        }
        return rewritten;
    }

    private void genCandidates() throws AlgebricksException {
        List<LogicalOperatorReference> currentLevelOpRefs = new ArrayList<LogicalOperatorReference>();
        prune();
        while (candidates.size() > 0) {
            for (LogicalOperatorReference opRef : candidates) {
                List<LogicalOperatorReference> refs = childrenToParents.get(opRef);
                if (refs != null)
                    currentLevelOpRefs.addAll(refs);
            }
            if (currentLevelOpRefs.size() == 0)
                break;
            candidatesGrow(currentLevelOpRefs);
            prune();
        }
        if (candidates.size() == 0)
            candidates.addAll(previousCandidates);
    }

    private void topDownMaterialization(List<LogicalOperatorReference> tops) {
        List<LogicalOperatorReference> nextLevel = new ArrayList<LogicalOperatorReference>();
        for (LogicalOperatorReference op : tops) {
            AbstractLogicalOperator aop = (AbstractLogicalOperator) op.getOperator();
            if ((aop.getOperatorTag() == LogicalOperatorTag.INNERJOIN || aop.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN)
                    && !joins.contains(op)) {
                joins.add(op);
            }
            for (LogicalOperatorReference opRef : op.getOperator().getInputs()) {
                List<LogicalOperatorReference> opRefList = childrenToParents.get(opRef);
                if (opRefList == null) {
                    opRefList = new ArrayList<LogicalOperatorReference>();
                    childrenToParents.put(opRef, opRefList);
                    nextLevel.add(opRef);
                }
                opRefList.add(op);
            }
            if (op.getOperator().getInputs().size() == 0)
                candidates.add(op);
        }
        if (nextLevel.size() > 0) {
            topDownMaterialization(nextLevel);
        }
    }

    private void candidatesGrow(List<LogicalOperatorReference> opList) {
        previousCandidates.clear();
        previousCandidates.addAll(candidates);
        candidates.clear();
        boolean validCandidate = false;
        for (LogicalOperatorReference op : opList) {
            for (LogicalOperatorReference inputRef : op.getOperator().getInputs()) {
                validCandidate = false;
                // if current input is in candidates
                for (LogicalOperatorReference candidate : previousCandidates)
                    if (inputRef.getOperator().equals(candidate.getOperator()))
                        validCandidate = true;
                // if one input is not in candidates
                if (!validCandidate)
                    break;
            }
            if (!validCandidate)
                continue;
            candidates.add(op);
        }
    }

    private void prune() throws AlgebricksException {
        boolean[] reserved = new boolean[candidates.size()];
        for (int i = 0; i < reserved.length; i++)
            reserved[i] = false;
        for (int i = candidates.size() - 1; i >= 0; i--) {
            ILogicalOperator candidate = candidates.get(i).getOperator();
            for (int j = i - 1; j >= 0; j--) {
                ILogicalOperator peer = candidates.get(j).getOperator();
                if (IsomorphismUtilities.isOperatorIsomorphic(candidate, peer)) {
                    reserved[i] = true;
                    reserved[j] = true;
                }
            }
        }
        for (int i = candidates.size() - 1; i >= 0; i--) {
            if (!reserved[i]) {
                candidates.remove(i);
            }
        }
    }

}
