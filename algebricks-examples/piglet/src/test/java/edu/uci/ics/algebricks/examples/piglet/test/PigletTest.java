package edu.uci.ics.algebricks.examples.piglet.test;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.algebricks.examples.piglet.ast.ASTNode;
import edu.uci.ics.algebricks.examples.piglet.compiler.PigletCompiler;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PigletTest {
    @Test
    public void test01() throws Exception {
        FileReader in = new FileReader("testcases/q1.piglet");
        try {
            PigletCompiler c = new PigletCompiler();

            List<ASTNode> ast = c.parse(in);
            JobSpecification jobSpec = c.compile(ast);

            System.err.println(jobSpec.toJSON());
        } finally {
            in.close();
        }
    }
}