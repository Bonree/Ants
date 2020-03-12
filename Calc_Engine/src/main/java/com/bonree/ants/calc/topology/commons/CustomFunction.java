package com.bonree.ants.calc.topology.commons;

import com.bonree.ants.commons.enums.DataType;
import com.bonree.ants.commons.enums.Delim;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月26日 下午6:18:00
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 自定义函数计算
 * ****************************************************************************
 */
public class CustomFunction {

    /**
     * 概述：自定义函数计算
     *
     * @param exprStr 字符串表达式如：(3+4)*5
     * @param type    计算结果的类型(long, double)
     * @return 计算结果
     */
    public static Object eval(String exprStr, DataType type) throws Exception {
        exprStr = exprStr.replaceAll(" ", "") + "#";// 去掉空格, #为结束标记
        List<Object> list = preprocessExpress(exprStr);
        return functionCalc(list, type);
    }

    /**
     * 概述：表达式计算
     *
     * @param list 整理好的计算集合
     * @param type 类型(long, double)
     * @return 计算结果
     */
    private static Object functionCalc(List<Object> list, DataType type) throws Exception {
        Stack<Object> stack = new Stack<>();// 栈, 先进后出
        for (Object s : list) {
            if (isOperator(s)) {// 操作运算符+-*/
                Object d1 = stack.pop();// 将栈中当前元素出栈
                Object d2 = stack.pop();// 将栈中当前元素出栈
                Object res = doCalc(d2, d1, s.toString(), type);
                stack.push(res);
            } else {
                stack.push(s);
            }
        }
        if (stack.size() == 1) {
            return stack.pop();
        } else {
            throw new Exception("Function calc is not correct!" + list);
        }
    }

    /**
     * 概述：执行计算
     *
     * @param dataA 数据值1
     * @param dataB 数据值2
     * @param oper  操作符号
     * @param type  类型(long, double)
     * @return 计算结果
     */
    private static Object doCalc(Object dataA, Object dataB, String oper, DataType type) throws Exception {
        switch (oper.charAt(0)) {
            case '+':
                return add(dataA, dataB, type);
            case '-':
                return subtract(dataA, dataB, type);
            case '*':
                return multiply(dataA, dataB, type);
            case '/':
                return divide(dataA, dataB, type);
            case '>':
                return greaterThan(dataA, dataB);
            case '<':
                return lessThan(dataA, dataB);
            case '=':
                return dataA.equals(dataB);
            case '|':
                return (boolean) dataA || (boolean) dataB;
            case '&':
                return (boolean) dataA && (boolean) dataB;
            default:
                throw new Exception("Unsupported operator:'" + oper + "'");
        }
    }

    /**
     * 概述：加计算
     */
    private static Object add(Object dataA, Object dataB, DataType type) {
        if (DataType.DOUBLE.equals(type)) {
            return (double) type.convert(dataA) + (double) type.convert(dataB);
        } else {
            return (long) type.convert(dataA) + (long) type.convert(dataB);
        }
    }

    /**
     * 概述：减计算
     */
    private static Object subtract(Object dataA, Object dataB, DataType type) {
        if (DataType.DOUBLE.equals(type)) {
            return (double) type.convert(dataA) - (double) type.convert(dataB);
        } else {
            return (long) type.convert(dataA) - (long) type.convert(dataB);
        }
    }

    /**
     * 概述：乘计算
     */
    private static Object multiply(Object dataA, Object dataB, DataType type) {
        if (DataType.DOUBLE.equals(type)) {
            return (double) type.convert(dataA) * (double) type.convert(dataB);
        } else {
            return (long) type.convert(dataA) * (long) type.convert(dataB);
        }
    }

    /**
     * 概述：除计算
     */
    private static Object divide(Object dataA, Object dataB, DataType type) {
        if (DataType.DOUBLE.equals(type)) {
            double tmpB = type.convert(dataB);
            return tmpB == 0 ? 0 : (double) type.convert(dataA) / tmpB;
        } else {
            long tmpB = type.convert(dataB);
            return tmpB == 0 ? 0 : (long) type.convert(dataA) / tmpB;
        }
    }

    /**
     * 概述：大于比较
     */
    private static Object greaterThan(Object dataA, Object dataB) {
        DataType type = dataA.equals(0L) || dataA.equals(0) || dataA.equals("0") ? parseType(dataB) : parseType(dataA);
        if (DataType.DOUBLE.equals(type)) {
            return (double) type.convert(dataA) > (double) type.convert(dataB);
        } else {
            return (long) type.convert(dataA) > (long) type.convert(dataB);
        }
    }

    /**
     * 概述：小于比较
     */
    private static Object lessThan(Object dataA, Object dataB) {
        DataType type = dataA.equals(0L) || dataA.equals(0) || dataA.equals("0") ? parseType(dataB) : parseType(dataA);
        if (DataType.DOUBLE.equals(type)) {
            return (double) type.convert(dataA) < (double) type.convert(dataB);
        } else {
            return (long) type.convert(dataA) < (long) type.convert(dataB);
        }
    }

    /**
     * 概述：预处理计算表达式
     *
     * @param exprStr 计算表达式
     * @return 集合[ 3, 4, +, 5, *],表示:(3+4)*5
     */
    private static ArrayList<Object> preprocessExpress(String exprStr) throws Exception {
        if (!exprStr.endsWith("#")) {
            throw new Exception("The last character of the expression must be '#'");
        }
        ArrayList<Object> list = new ArrayList<>();
        Stack<String> stack = new Stack<>(); // 后进先出
        stack.push("#");// 作为标记，压入栈
        char last, ch;
        StringBuffer sb;
        for (int i = 0; i < exprStr.length(); i++) {
            ch = exprStr.charAt(i);
            switch (ch) {
                case '*':
                case '/':
                case '+':
                case '-':
                case '>':
                case '<':
                case '&': // 表示java条件判断运算符中的并: &&
                case '|': // 表示java条件判断运算符中的或: ||
                case '=': // 表示java条件判断运算符中的等于: ==
                    last = stack.peek().charAt(0);
                    if (last != '(' && priority(last) >= priority(ch))
                        list.add(stack.pop());
                    stack.push("" + ch);
                    break;
                case '(':
                    stack.push("(");
                    break;
                case ')':
                    while (!stack.isEmpty() && stack.peek().charAt(0) != '(') {
                        list.add(stack.pop());
                    }
                    if (stack.isEmpty() || stack.size() == 1) {
                        throw new Exception("')' Parentheses are not paired!");
                    }
                    stack.pop();
                    break;
                case '#':
                    while (stack.size() > 1 && stack.peek().charAt(0) != '(') {
                        list.add(stack.pop());
                    }
                    if (stack.size() > 1) {
                        throw new Exception("'(' Parentheses are not paired!");
                    }
                    break;
                default:
                    sb = new StringBuffer();
                    sb.append(ch);
                    while (validate(exprStr.charAt(i + 1))) {
                        char bb = exprStr.charAt(++i);
                        sb.append(bb);
                    }
                    list.add(parseObj(sb.toString()));
                    break;
            }
        }
        return list;
    }

    /**
     * 概述：验证字符
     *
     * @param chars 字符
     * @return 是否合法
     */
    private static boolean validate(char chars) {
        return (chars <= '9' && chars >= '0') || (chars <= 'z' && chars >= 'a') || (chars <= 'Z' && chars >= 'A') || chars == '.';
    }

    /**
     * 概述：操作符优先级
     *
     * @param ch 操作符
     */
    private static int priority(char ch) {
        switch (ch) {
            case '=':
            case '>':
            case '<':
                return 3;
            case '&':
                return 2;
            case '|':
                return 1;
            case '+':
            case '-':
                return 4;
            case '*':
            case '/':
                return 5;
            case '#':
                return 0;
            default:
                return 0;
        }
    }

    /**
     * 概述：解析obj类型
     *
     * @param object 数据内容
     */
    private static DataType parseType(Object object) {
        if (object instanceof Long) {
            return DataType.LONG;
        } else if (object instanceof Double) {
            return DataType.DOUBLE;
        } else {
            return DataType.STRING;
        }
    }

    /**
     * 概述：转换obj类型
     *
     * @param value 数据内容
     */
    private static Object parseObj(String value) {
        if (value.contains(Delim.DOT.value())) {
            return Double.parseDouble(value);
        } else if (StringUtils.isNumeric(value)) {
            return Long.parseLong(value);
        }
        return value;
    }

    /**
     * 概述：判断操作符号
     *
     * @param str 操作符
     */
    private static boolean isOperator(Object str) {
        return "=".equals(str) || ">".equals(str) || "<".equals(str) || "&".equals(str) || "|".equals(str)
                || "+".equals(str) || "-".equals(str) || "*".equals(str) || "/".equals(str);
    }

    /**
     * 处理表达式中的属性
     */
    public static class Expression {
        String data;

        public Expression(String data) {
            this.data = data;
        }

        public void put(String key, Object value) {
            data = data.replaceAll(key, value.toString());
        }

        public String toString() {
            return data.replaceAll("'", "");
        }
    }
}
