package edu.uci.ics.algebricks.examples.piglet.test;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import edu.uci.ics.algebricks.examples.piglet.ast.ASTNode;
import edu.uci.ics.algebricks.examples.piglet.compiler.PigletCompiler;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PigletTest {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        File dir = new File("testcases");
        findAndAddTests(suite, dir);

        return suite;
    }

    private static void findAndAddTests(TestSuite suite, File dir) {
        for (final File f : dir.listFiles()) {
            if (f.getName().startsWith(".")) {
                continue;
            }
            if (f.isDirectory()) {
                findAndAddTests(suite, f);
            } else if (f.getName().endsWith(".piglet")) {
                suite.addTest(new TestCase(f.getPath()) {
                    @Override
                    public void run(TestResult result) {
                        try {
                            FileReader in = new FileReader(f);
                            try {
                                PigletCompiler c = new PigletCompiler();

                                List<ASTNode> ast = c.parse(in);
                                JobSpecification jobSpec = c.compile(ast);

                                System.err.println(jobSpec.toJSON());
                            } finally {
                                in.close();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        }
    }
}