package edu.uci.ics.algebricks.examples.piglet.runtime.functions;

import edu.uci.ics.algebricks.compiler.algebra.functions.FunctionIdentifier;
import edu.uci.ics.algebricks.runtime.hyracks.base.IEvaluatorFactory;

public interface IPigletFunctionEvaluatorFactoryBuilder {
    public IEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, IEvaluatorFactory[] arguments);
}