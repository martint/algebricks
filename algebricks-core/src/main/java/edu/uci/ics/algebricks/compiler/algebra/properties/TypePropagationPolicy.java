package edu.uci.ics.algebricks.compiler.algebra.properties;

import edu.uci.ics.algebricks.api.exceptions.AlgebricksException;
import edu.uci.ics.algebricks.api.expr.INullableTypeComputer;
import edu.uci.ics.algebricks.api.expr.IVariableTypeEnvironment;
import edu.uci.ics.algebricks.compiler.algebra.base.LogicalVariable;
import edu.uci.ics.algebricks.compiler.algebra.typing.ITypeEnvPointer;

public abstract class TypePropagationPolicy {
    public static final TypePropagationPolicy ALL = new TypePropagationPolicy() {

        @Override
        public Object getVarType(LogicalVariable var, INullableTypeComputer ntc, ITypeEnvPointer... typeEnvs)
                throws AlgebricksException {
            for (ITypeEnvPointer p : typeEnvs) {
                IVariableTypeEnvironment env = p.getTypeEnv();
                if (env == null) {
                    throw new AlgebricksException("Null environment for pointer " + p + " in getVarType for var=" + var);
                }
                Object t = env.getVarType(var);
                if (t != null) {
                    return t;
                }
            }
            return null;
        }
    };

    public static final TypePropagationPolicy LEFT_OUTER = new TypePropagationPolicy() {

        @Override
        public Object getVarType(LogicalVariable var, INullableTypeComputer ntc, ITypeEnvPointer... typeEnvs)
                throws AlgebricksException {
            int n = typeEnvs.length;
            for (int i = 0; i < n; i++) {
                Object t = typeEnvs[i].getTypeEnv().getVarType(var);
                if (t != null) {
                    if (i == 0) { // inner branch
                        return t;
                    } else { // outer branch
                        return ntc.makeNullableType(t);
                    }
                }
            }
            return null;
        }
    };

    public abstract Object getVarType(LogicalVariable var, INullableTypeComputer ntc, ITypeEnvPointer... typeEnvs)
            throws AlgebricksException;
}
