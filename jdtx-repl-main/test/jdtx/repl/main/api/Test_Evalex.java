package jdtx.repl.main.api;

import com.udojava.evalex.*;
import org.junit.*;

import java.math.*;
import java.util.*;

public class Test_Evalex {

    @Test
    public void testBase() {
        Expression ex = new Expression("3*x");
        System.out.println(ex.toString());

        ex.setVariable("x", new BigDecimal("7"));
        System.out.println(ex.getUsedVariables().toString() + ", " + ex.toString() + " -> " + ex.eval());

        ex.setVariable("x", new BigDecimal("3"));
        System.out.println(ex.getUsedVariables().toString() + ", " + ex.toString() + " -> " + ex.eval());

        //
        System.out.println("");

        //
        Expression exBool = new Expression("3>x || y>x");
        System.out.println(exBool.toString());

        exBool.setVariable("x", new BigDecimal("7"));
        exBool.setVariable("y", new BigDecimal("7"));
        System.out.println(ex.getUsedVariables().toString() + ", " + exBool.toString() + " -> " + exBool.eval());

        exBool.setVariable("x", new BigDecimal("2"));
        exBool.setVariable("y", new BigDecimal("1"));
        System.out.println(ex.getUsedVariables().toString() + ", " + exBool.toString() + " -> " + exBool.eval());


        exBool.setVariable("x", new BigDecimal("2"));
        exBool.setVariable("y", new BigDecimal("10"));
        System.out.println(ex.getUsedVariables().toString() + ", " + exBool.toString() + " -> " + exBool.eval());
    }

    @Test
    public void testResultType() {
        Expression exBoolTrue = new Expression("true");
        System.out.println(exBoolTrue.toString() + " -> " + exBoolTrue.eval());
        System.out.println(exBoolTrue.eval().equals(1));
        System.out.println(exBoolTrue.eval().equals(0));
        System.out.println(exBoolTrue.eval().equals(BigDecimal.ONE));
    }

    @Test
    public void testVarsCase() {
        Expression exBoolCompare = new Expression("2 = rec_valueX");
        //
        exBoolCompare.setVariable("REC_ValueX", "0");
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        exBoolCompare.setVariable("REC_ValueX", new BigDecimal(0));
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        //
        exBoolCompare.setVariable("rec_valueX", "0");
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        exBoolCompare.setVariable("rec_valueX", new BigDecimal(0));
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        //
        exBoolCompare.setVariable("REC_ValueX", "2");
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        exBoolCompare.setVariable("REC_ValueX", new BigDecimal(2));
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        //
        exBoolCompare.setVariable("rec_valueX", "2");
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
        exBoolCompare.setVariable("rec_valueX", new BigDecimal(2));
        System.out.println(exBoolCompare.toString() + " -> " + exBoolCompare.eval());
    }

    @Test
    public void testAddMyFunction() {
        Expression ex = new Expression("MyFunc(x*x)");
        //
        Function myFunction = new MyFunctionImplementation("MyFunc", 1);
        ex.addFunction(myFunction);

        //
        ex.setVariable("x", "0");
        System.out.println(ex.toString() + " -> " + ex.eval());
        //
        ex.setVariable("x", "1");
        System.out.println(ex.toString() + " -> " + ex.eval());
        //
        ex.setVariable("x", "2");
        System.out.println(ex.toString() + " -> " + ex.eval());
    }

    private class MyFunctionImplementation extends AbstractFunction {

        protected MyFunctionImplementation(String name, int numParams) {
            super(name, numParams);
        }

        @Override
        public BigDecimal eval(List<BigDecimal> parameters) {
            return parameters.get(0).multiply(new BigDecimal(2));
        }

    }

}
