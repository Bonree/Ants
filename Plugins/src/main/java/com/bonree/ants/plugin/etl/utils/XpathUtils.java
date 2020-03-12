package com.bonree.ants.plugin.etl.utils;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.StringReader;
import java.text.NumberFormat;
import java.text.ParseException;


/**
 * 辅助获得xml中内容
 */
public enum XpathUtils {

    instance;

    private transient XPath oXpath;

    private transient DocumentBuilder builder;

    XpathUtils() {
        XPathFactory factory = XPathFactory.newInstance();
        oXpath = factory.newXPath();

        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        try {
            builder = domFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * 概述：创建文档对象
     *
     * @param input 文件内容
     */
    public Document createDoc(InputStream input) throws Exception {
        InputSource is = new InputSource(input);
        return builder.parse(is);
    }

    /**
     * 概述：创建文档对象
     *
     * @param path 文件路径
     */
    public Document createDoc(String path) throws Exception {
        return builder.parse(path);
    }

    private Object getResult(Node node, QName returnType, String expression) throws XPathExpressionException {
        return oXpath.compile(expression).evaluate(node, returnType);
    }

    /**
     * 概述：获取文档中指定标签下的text值
     *
     * @param doc        待解析文档
     * @param expression xpath表达式
     */
    public String getString(Document doc, String expression) throws XPathExpressionException {
        return (String) getResult(doc, XPathConstants.STRING, expression);
    }

    /**
     * 概述：获取文档NodeList对象
     *
     * @param doc        待解析文档或节点
     * @param expression xpath表达式
     */

    public NodeList getNodeList(Document doc, String expression) throws XPathExpressionException {
        return (NodeList) getResult(doc, XPathConstants.NODESET, expression);
    }

    /**
     * 概述：获取文档中的的Node对象
     *
     * @param doc        待解析文档或节点
     * @param expression xpath表达式
     */
    public Node getNode(Node doc, String expression) throws XPathExpressionException {
        return (Node) getResult(doc, XPathConstants.NODE, expression);
    }

    /**
     * 概述：获取节点的属性值
     *
     * @param doc    节点对象
     * @param abName 属性名称
     */
    public String getStringAttri(Node doc, String abName) {
        String value = "";
        if (doc != null && doc.getAttributes().getNamedItem(abName) != null) {
            value = doc.getAttributes().getNamedItem(abName).getNodeValue();
        }
        return value;
    }

    /**
     * 概述：获取节点的属性值
     *
     * @param doc    节点对象
     * @param abName 属性名称
     */
    public Number getNumberAttri(Node doc, String abName) throws ParseException {
        String value = getStringAttri(doc, abName);
        if (value == null) {
            return 0;
        }
        return NumberFormat.getInstance().parse(value);
    }

    /**
     * 概述：获取当前节点下指定标签的text值
     *
     * @param node 待解析文档或节点
     * @param name 属性名称
     */
    public String getStringText(Node node, String name) throws XPathExpressionException {
        return (String) getResult(node, XPathConstants.STRING, "./ ".concat(name));
    }

    /**
     * 概述：获取当前节点下指定标签的text值
     *
     * @param node 待解析文档或节点
     * @param name 属性名称
     */
    public Number getNumberText(Node node, String name) throws XPathExpressionException {
        return (Number) getResult(node, XPathConstants.NUMBER, "./ ".concat(name));
    }
}
