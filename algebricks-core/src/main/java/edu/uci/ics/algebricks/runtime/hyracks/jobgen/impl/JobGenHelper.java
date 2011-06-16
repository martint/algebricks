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
package edu.uci.ics.algebricks.runtime.hyracks.jobgen.impl;

import java.util.Collection;

import edu.uci.ics.algebricks.api.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.algebricks.api.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.algebricks.api.data.IPrinterFactory;
import edu.uci.ics.algebricks.api.data.IPrinterFactoryProvider;
import edu.uci.ics.algebricks.api.data.ISerializerDeserializerProvider;
import edu.uci.ics.algebricks.api.data.ITypeTraitProvider;
import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.algebricks.compiler.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;

public final class JobGenHelper {

    public static IBTreeInteriorFrameFactory createNSMInteriorFrameFactory(ITypeTrait[] typeTraits) {
        return new NSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    public static IBTreeLeafFrameFactory createNSMLeafFrameFactory(ITypeTrait[] typeTraits) {
        return new NSMLeafFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor mkRecordDescriptor(IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        ISerializerDeserializer[] fields = new ISerializerDeserializer[opSchema.getSize()];
        ISerializerDeserializerProvider sdp = context.getSerializerDeserializerProvider();
        int i = 0;
        for (LogicalVariable var : opSchema) {
            Object t = context.getVarType(var);
            fields[i] = sdp.getSerializerDeserializer(t);
            i++;
        }
        return new RecordDescriptor(fields);
    }

    public static IPrinterFactory[] mkPrinterFactories(IOperatorSchema opSchema, JobGenContext context,
            int[] printColumns) throws AlgebricksException {
        IPrinterFactory[] pf = new IPrinterFactory[printColumns.length];
        IPrinterFactoryProvider pff = context.getPrinterFactoryProvider();
        for (int i = 0; i < pf.length; i++) {
            LogicalVariable v = opSchema.getVariable(printColumns[i]);
            Object t = context.getVarType(v);
            pf[i] = pff.getPrinterFactory(t);
        }
        return pf;
    }

    public static int[] variablesToFieldIndexes(Collection<LogicalVariable> varLogical, IOperatorSchema opSchema) {
        int[] tuplePos = new int[varLogical.size()];
        int i = 0;
        for (LogicalVariable var : varLogical) {
            tuplePos[i] = opSchema.findVariable(var);
            i++;
        }
        return tuplePos;
    }

    public static IBinaryHashFunctionFactory[] variablesToBinaryHashFunctionFactories(
            Collection<LogicalVariable> varLogical, JobGenContext context) throws AlgebricksException {
        IBinaryHashFunctionFactory[] funFactories = new IBinaryHashFunctionFactory[varLogical.size()];
        int i = 0;
        IBinaryHashFunctionFactoryProvider bhffProvider = context.getBinaryHashFunctionFactoryProvider();
        for (LogicalVariable var : varLogical) {
            Object type = context.getVarType(var);
            funFactories[i++] = bhffProvider.getBinaryHashFunctionFactory(type);
        }
        return funFactories;
    }

    public static IBinaryComparatorFactory[] variablesToAscBinaryComparatorFactories(
            Collection<LogicalVariable> varLogical, JobGenContext context) throws AlgebricksException {
        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[varLogical.size()];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = context.getVarType(v);
            compFactories[i++] = bcfProvider.getBinaryComparatorFactory(type, OrderKind.ASC);
        }
        return compFactories;
    }

    public static ITypeTrait[] variablesToTypeTraits(Collection<LogicalVariable> varLogical, JobGenContext context)
            throws AlgebricksException {
        ITypeTrait[] typeTraits = new ITypeTrait[varLogical.size()];
        ITypeTraitProvider typeTraitProvider = context.getTypeTraitProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = context.getVarType(v);
            typeTraits[i++] = typeTraitProvider.getTypeTrait(type);
        }
        return typeTraits;
    }

    public static int[] projectAllVariables(IOperatorSchema opSchema) {
        int[] projectionList = new int[opSchema.getSize()];
        int k = 0;
        for (LogicalVariable v : opSchema) {
            projectionList[k++] = opSchema.findVariable(v);
        }
        return projectionList;
    }

}
