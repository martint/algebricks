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
package edu.uci.ics.algebricks.api.compiler;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.IExpressionEvalSizeComputer;
import edu.uci.ics.algebricks.api.expr.IExpressionTypeComputer;
import edu.uci.ics.algebricks.api.expr.IMergeAggregationExpressionFactory;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.compiler.algebra.base.ILogicalPlan;
import edu.uci.ics.algebricks.compiler.algebra.metadata.IMetadataProvider;
import edu.uci.ics.algebricks.compiler.optimizer.base.AlgebricksOptimizationContext;
import edu.uci.ics.algebricks.compiler.optimizer.base.HeuristicOptimizer;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContext;
import edu.uci.ics.algebricks.compiler.optimizer.base.IOptimizationContextFactory;
import edu.uci.ics.algebricks.compiler.optimizer.base.PhysicalOptimizationConfig;
import edu.uci.ics.algebricks.config.AlgebricksConfig;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.JobGenContext;
import edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl.PlanCompiler;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class HeuristicCompilerFactoryBuilder extends AbstractCompilerFactoryBuilder {

    public static class DefaultOptimizationContextFactory implements IOptimizationContextFactory {

        public static final DefaultOptimizationContextFactory INSTANCE = new DefaultOptimizationContextFactory();

        private DefaultOptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter, int frameSize,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                PhysicalOptimizationConfig physicalOptimizationConfig) {
            return new AlgebricksOptimizationContext(varCounter, frameSize, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                    physicalOptimizationConfig);
        }
    }

    private IOptimizationContextFactory optCtxFactory;

    public HeuristicCompilerFactoryBuilder() {
        this.optCtxFactory = DefaultOptimizationContextFactory.INSTANCE;
    }

    public HeuristicCompilerFactoryBuilder(IOptimizationContextFactory optCtxFactory) {
        this.optCtxFactory = optCtxFactory;
    }

    @Override
    public ICompilerFactory create() {
        return new ICompilerFactory() {
            @Override
            public ICompiler createCompiler(final ILogicalPlan plan, final IMetadataProvider<?, ?> metadata,
                    int varCounter) {
                IOptimizationContext oc = optCtxFactory.createOptimizationContext(varCounter, frameSize,
                        expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                        nullableTypeComputer, physicalOptimizationConfig);
                oc.setMetadataDeclarations(metadata);
                final HeuristicOptimizer opt = new HeuristicOptimizer(plan, logicalRewrites, physicalRewrites, oc);
                return new ICompiler() {

                    @Override
                    public void optimize() throws AlgebricksException {
                        opt.optimize();
                    }

                    @Override
                    public JobSpecification createJob(Object appContext) throws AlgebricksException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting Job Generation.\n");
                        JobGenContext context = new JobGenContext(null, metadata, appContext,
                                serializerDeserializerProvider, hashFunctionFactoryProvider, comparatorFactoryProvider,
                                typeTraitProvider, binaryBooleanInspector, binaryIntegerInspector, printerProvider,
                                nullWriterFactory, normalizedKeyComputerFactoryProvider, exprJobGen,
                                expressionTypeComputer, expressionEvalSizeComputer, partialAggregationTypeComputer,
                                frameSize, clusterLocations);
                        PlanCompiler pc = new PlanCompiler(context);
                        return pc.compilePlan(plan, null);
                    }
                };
            }
        };
    }

}
