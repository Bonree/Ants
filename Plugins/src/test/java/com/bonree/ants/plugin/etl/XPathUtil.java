package com.bonree.ants.plugin.etl;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;


/**
 * 辅助获得xml中内容
 */
public enum XPathUtil {

    instance;

    private transient XPathFactory xPathFactory;

    private transient DocumentBuilder builder;

    XPathUtil() {
        xPathFactory = XPathFactory.newInstance();

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
     * @param reader 文件内容
     */
    public Document createDoc(StringReader reader) throws Exception {
        InputSource is = new InputSource(reader);
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

    private XPathExpression getPathExpression(String expression) throws XPathExpressionException {
        return xPathFactory.newXPath().compile(expression);
    }

    private Object getResult(Document doc, QName returnType, String expression) throws XPathExpressionException {
        if (xPathFactory == null) {
            xPathFactory = XPathFactory.newInstance();
        }
        XPathExpression pathExpression = getPathExpression(expression);
        return pathExpression.evaluate(doc, returnType);
    }

    /**
     * 概述：解析得到的String
     *
     * @param doc        待解析文档
     * @param expression xpath表达式
     */
    public String getString(Document doc, String expression) throws XPathExpressionException {
        return (String) getResult(doc, XPathConstants.STRING, expression);
    }

    /**
     * 概述：解析得到的NodeList
     *
     * @param doc        待解析文档
     * @param expression xpath表达式
     */

    public NodeList getNodeList(Document doc, String expression) throws XPathExpressionException {
        return (NodeList) getResult(doc, XPathConstants.NODESET, expression);
    }

    /**
     * 概述：解析得到的Node
     *
     * @param doc        待解析文档
     * @param expression xpath表达式
     */
    public Node getNode(Document doc, String expression) throws XPathExpressionException {
        return (Node) getResult(doc, XPathConstants.NODE, expression);
    }

    /**
     * 概述：获取节点的属性值
     *
     * @param node   节点对象
     * @param abName 属性名称
     */
    public String getAttributeValue(Node node, String abName) {
        String value = "";
        if (node != null && node.getAttributes().getNamedItem(abName) != null) {
            value = node.getAttributes().getNamedItem(abName).getNodeValue();
        }
        return value;
    }

    public static void main(String[] args) throws Exception {

//		String path = "com/bonree/basedata/xml/zhibiao.xml";
        //LogUtil.debugSys(path);
        //builder.p
//		Document doc = DocUtil.createDoc(path);
//		//LogUtil.debugSys(DocUtil.docToString(doc));
//		NodeList node=getNodeList(doc, "//*/normItem");
//		for (int i=0;i<node.getLength();i++) {
//			String key=node.item(i).getAttributes().getNamedItem("key").getNodeValue();
//			String value=node.item(i).getAttributes().getNamedItem("value").getNodeValue();
//			LogUtil.debugSys("map.put(\""+key+"\",\""+value+"\");");
//		}


        //LogUtil.debugSys(node.getAttributes().item(0));
        //LogUtil.debugSys(DocUtil.NodetoString(node));;
        //LogUtil.debugSys(getString(doc, "//*[@id='city_netservice_role']/bar"));
        //LogUtil.debugSys(getString(doc, "//norm[@name='Nav']/table[@type='comm']").trim());

        //LogUtil.debugSys("--"+getNode(doc, "//norm[@name='Nav']//normItem[@value='DOWN_SPEED']/field[@order]").getAttributes().getNamedItem("order").getNodeValue());

    }

}
