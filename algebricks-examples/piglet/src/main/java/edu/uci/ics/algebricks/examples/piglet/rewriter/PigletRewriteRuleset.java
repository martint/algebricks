package edu.uci.ics.algebricks.examples.piglet.rewriter;

import java.util.LinkedList;

import edu.uci.ics.algebricks.compiler.optimizer.base.HeuristicOptimizer;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.BreakSelectIntoConjunctsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ComplexJoinInferenceRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ConsolidateAssignsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ConsolidateSelectsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.EliminateSubplanRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.EnforceStructuralPropertiesRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ExtractCommonOperatorsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ExtractGbyExpressionsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.FactorRedundantGroupAndDecorVarsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.InferTypesRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.InlineVariablesRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.IntroduceGroupByForStandaloneAggregRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.IsolateHyracksOperatorsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PullSelectOutOfEqJoin;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PushLimitDownRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PushProjectDownRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PushProjectIntoDataSourceScanRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PushSelectDownRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.PushSelectIntoJoinRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.ReinferAllTypesRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.RemoveRedundantProjectionRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.RemoveUnusedAssignAndAggregateRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.SetAlgebricksPhysicalOperatorsRule;
import edu.uci.ics.algebricks.compiler.optimizer.rules.SetExecutionModeRule;

public class PigletRewriteRuleset {
    public final static LinkedList<IAlgebraicRewriteRule> TYPE_INFERENCE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        TYPE_INFERENCE.add(new InferTypesRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> NORMALIZATION = new LinkedList<IAlgebraicRewriteRule>();
    static {
        NORMALIZATION.add(new EliminateSubplanRule());
        NORMALIZATION.add(new IntroduceGroupByForStandaloneAggregRule());
        NORMALIZATION.add(new BreakSelectIntoConjunctsRule());
        NORMALIZATION.add(new PushSelectIntoJoinRule());
        NORMALIZATION.add(new ExtractGbyExpressionsRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> COND_PUSHDOWN_AND_JOIN_INFERENCE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new PushSelectDownRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new InlineVariablesRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new FactorRedundantGroupAndDecorVarsRule());
        COND_PUSHDOWN_AND_JOIN_INFERENCE.add(new EliminateSubplanRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> JOIN_INFERENCE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        JOIN_INFERENCE.add(new InlineVariablesRule());
        // LOAD_FIELDS.add(new RemoveUnusedAssignAndAggregateRule());
        JOIN_INFERENCE.add(new ComplexJoinInferenceRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> OP_PUSHDOWN = new LinkedList<IAlgebraicRewriteRule>();
    static {
        OP_PUSHDOWN.add(new PushProjectDownRule());
        OP_PUSHDOWN.add(new PushSelectDownRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> DATA_EXCHANGE = new LinkedList<IAlgebraicRewriteRule>();
    static {
        DATA_EXCHANGE.add(new SetExecutionModeRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> CONSOLIDATION = new LinkedList<IAlgebraicRewriteRule>();
    static {
        CONSOLIDATION.add(new RemoveRedundantProjectionRule());
        CONSOLIDATION.add(new ConsolidateSelectsRule());
        CONSOLIDATION.add(new ConsolidateAssignsRule());
        CONSOLIDATION.add(new RemoveUnusedAssignAndAggregateRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> PHYSICAL_PLAN_REWRITES = new LinkedList<IAlgebraicRewriteRule>();
    static {
        PHYSICAL_PLAN_REWRITES.add(new PullSelectOutOfEqJoin());
        PHYSICAL_PLAN_REWRITES.add(new SetAlgebricksPhysicalOperatorsRule());
        PHYSICAL_PLAN_REWRITES.add(new EnforceStructuralPropertiesRule());
        PHYSICAL_PLAN_REWRITES.add(new PushProjectDownRule());
        PHYSICAL_PLAN_REWRITES.add(new SetAlgebricksPhysicalOperatorsRule());
        PHYSICAL_PLAN_REWRITES.add(new PushLimitDownRule());
    }

    public final static LinkedList<IAlgebraicRewriteRule> PREPARE_FOR_JOBGEN = new LinkedList<IAlgebraicRewriteRule>();
    static {
        PREPARE_FOR_JOBGEN.add(new IsolateHyracksOperatorsRule(
                HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
        PREPARE_FOR_JOBGEN.add(new ExtractCommonOperatorsRule());
        PREPARE_FOR_JOBGEN.add(new PushProjectIntoDataSourceScanRule());
        PREPARE_FOR_JOBGEN.add(new ReinferAllTypesRule());
    }

}