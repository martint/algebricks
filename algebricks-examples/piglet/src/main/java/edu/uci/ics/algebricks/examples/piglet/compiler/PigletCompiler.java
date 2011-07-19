package edu.uci.ics.algebricks.examples.piglet.compiler;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.algebricks.api.compiler.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.algebricks.api.compiler.ICompiler;
import edu.uci.ics.algebricks.api.compiler.ICompilerFactory;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalOperator;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalExpressionReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalOperatorReference;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.WriteOperator;
import edu.uci.ics.algebricks.compiler.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.algebricks.compiler.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.algebricks.compiler.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.algebricks.compiler.optimizer.base.AbstractRuleController;
import edu.uci.ics.algebricks.compiler.optimizer.base.IAlgebraicRewriteRule;
import edu.uci.ics.algebricks.compiler.optimizer.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.algebricks.compiler.optimizer.rulecontrollers.SequentialOnceRuleControllerFullDFS;
import edu.uci.ics.algebricks.examples.piglet.ast.ASTNode;
import edu.uci.ics.algebricks.examples.piglet.ast.AssignmentNode;
import edu.uci.ics.algebricks.examples.piglet.ast.DumpNode;
import edu.uci.ics.algebricks.examples.piglet.ast.LoadNode;
import edu.uci.ics.algebricks.examples.piglet.ast.RelationNode;
import edu.uci.ics.algebricks.examples.piglet.metadata.PigletFileDataSink;
import edu.uci.ics.algebricks.examples.piglet.metadata.PigletFileDataSource;
import edu.uci.ics.algebricks.examples.piglet.metadata.PigletMetadataProvider;
import edu.uci.ics.algebricks.examples.piglet.parser.ParseException;
import edu.uci.ics.algebricks.examples.piglet.parser.PigletParser;
import edu.uci.ics.algebricks.examples.piglet.rewriter.PigletRewriteRuleset;
import edu.uci.ics.algebricks.examples.piglet.types.Schema;
import edu.uci.ics.algebricks.examples.piglet.types.Type;
import edu.uci.ics.algebricks.utils.Pair;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PigletCompiler {
    private static final Logger LOGGER = Logger.getLogger(PigletCompiler.class.getName());

    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> DEFAULT_LOGICAL_REWRITES = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> DEFAULT_PHYSICAL_REWRITES = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
    static {
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(true);
        SequentialOnceRuleControllerFullDFS seqOnceCtrl = new SequentialOnceRuleControllerFullDFS();
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                PigletRewriteRuleset.NORMALIZATION));
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                PigletRewriteRuleset.COND_PUSHDOWN_AND_JOIN_INFERENCE));
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                PigletRewriteRuleset.LOAD_FIELDS));
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                PigletRewriteRuleset.OP_PUSHDOWN));
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                PigletRewriteRuleset.DATA_EXCHANGE));
        DEFAULT_LOGICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                PigletRewriteRuleset.CONSOLIDATION));

        DEFAULT_PHYSICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                PigletRewriteRuleset.PHYSICAL_PLAN_REWRITES));
        DEFAULT_PHYSICAL_REWRITES.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                PigletRewriteRuleset.ISOLATE_HYRAX_OPS));
    }

    private final ICompilerFactory cFactory;

    private final PigletMetadataProvider metadataProvider;

    private int varCounter;

    public PigletCompiler() {
        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder();
        builder.setLogicalRewrites(DEFAULT_LOGICAL_REWRITES);
        builder.setPhysicalRewrites(DEFAULT_PHYSICAL_REWRITES);
        cFactory = builder.create();
        metadataProvider = new PigletMetadataProvider();
    }

    public List<ASTNode> parse(Reader in) throws ParseException {
        PigletParser parser = new PigletParser(in);
        List<ASTNode> statements = parser.Statements();
        return statements;
    }

    public JobSpecification compile(List<ASTNode> ast) throws AlgebricksException {
        ILogicalPlan plan = translate(ast);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Translated Plan:");
            LOGGER.info(getPrettyPrintedPlan(plan));
        }
        ICompiler compiler = cFactory.createCompiler(plan, metadataProvider, varCounter);
        compiler.optimize();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Optimized Plan:");
            LOGGER.info(getPrettyPrintedPlan(plan));
        }
        return compiler.createJob(null);
    }

    private ILogicalPlan translate(List<ASTNode> ast) {
        Map<String, Relation> symMap = new HashMap<String, Relation>();
        List<LogicalOperatorReference> roots = new ArrayList<LogicalOperatorReference>();
        for (ASTNode an : ast) {
            switch (an.getTag()) {
                case DUMP: {
                    DumpNode dn = (DumpNode) an;
                    Relation input = symMap.get(dn.getAlias());
                    List<LogicalExpressionReference> expressions = new ArrayList<LogicalExpressionReference>();
                    for (LogicalVariable v : input.schema.values()) {
                        expressions.add(new LogicalExpressionReference(new VariableReferenceExpression(v)));
                    }
                    PigletFileDataSink dataSink = new PigletFileDataSink(dn.getFile());
                    ILogicalOperator op = new WriteOperator(expressions, dataSink);
                    op.getInputs().add(new LogicalOperatorReference(input.op));
                    roots.add(new LogicalOperatorReference(op));
                }
                    break;

                case ASSIGNMENT: {
                    AssignmentNode asn = (AssignmentNode) an;
                    String alias = asn.getAlias();
                    RelationNode rn = asn.getRelation();
                    Relation rel = translate(rn, symMap);
                    rel.alias = alias;
                    symMap.put(alias, rel);
                }
                    break;
            }
        }
        return new ALogicalPlanImpl(roots);
    }

    private Relation translate(RelationNode rn, Map<String, Relation> symMap) {
        switch (rn.getTag()) {
            case LOAD: {
                LoadNode ln = (LoadNode) rn;
                String file = ln.getDataFile();
                Schema schema = ln.getSchema();
                List<Pair<String, Type>> fieldsSchema = schema.getSchema();
                List<LogicalVariable> variables = new ArrayList<LogicalVariable>();
                List<Object> types = new ArrayList<Object>();
                Relation rel = new Relation();
                for (Pair<String, Type> p : fieldsSchema) {
                    LogicalVariable v = new LogicalVariable(varCounter++);
                    rel.schema.put(p.first, v);
                    variables.add(v);
                    types.add(p.second);
                }
                PigletFileDataSource ds = new PigletFileDataSource(file, types.toArray());
                rel.op = new DataSourceScanOperator(variables, ds);
                rel.op.getInputs().add(new LogicalOperatorReference(new EmptyTupleSourceOperator()));
                return rel;
            }
        }
        throw new IllegalArgumentException("Unknown node: " + rn.getTag() + " encountered");
    }

    private static class Relation {
        String alias;
        ILogicalOperator op;
        final Map<String, LogicalVariable> schema;

        public Relation() {
            schema = new LinkedHashMap<String, LogicalVariable>();
        }
    }

    private String getPrettyPrintedPlan(ILogicalPlan plan) throws AlgebricksException {
        LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor();
        StringBuilder buffer = new StringBuilder();
        PlanPrettyPrinter.printPlan(plan, buffer, v, 0);
        return buffer.toString();
    }
}