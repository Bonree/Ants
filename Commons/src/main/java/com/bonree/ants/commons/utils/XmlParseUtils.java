package com.bonree.ants.commons.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2017-4-6 下午3:42:20
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 加载xml配置文件公共类
 ******************************************************************************/
public class XmlParseUtils {

    private static Logger log = LoggerFactory.getLogger(XmlParseUtils.class);

    private Document doc;

    /**
     * 初始配置信息对象
     *
     * @param file 配置信息文件对象
     */
    public XmlParseUtils(File file) throws Exception {
        try {
            doc = Jsoup.parse(file, "utf-8");
        } catch (Exception ex) {
            throw new Exception("Load config error: ", ex);
        }
    }

    /**
     * 初始配置信息对象
     *
     * @param content 配置信息
     */
    public XmlParseUtils(String content) throws Exception {
        try {
            doc = Jsoup.parse(content, "utf-8");
        } catch (Exception ex) {
            throw new Exception("Load config error: ", ex);
        }
    }

    /**
     * 概述：添加xml内容到指定元素中
     *
     * @param element 元素对象
     * @param html    待添加的配置文件
     */
    public void addElement(Element element, String html) {
        element.append(html);
    }

    /**
     * 概述：添加xml内容到指定元素中
     *
     * @param sourceElement 元素对象
     * @param elements    待填加的元素对象集合
     */
    public void addElements(Element sourceElement, Elements elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }
        for (Element element : elements) {
            addElement(sourceElement, element.toString());
        }
    }

    /**
     * 概述：获取xml元素对象
     *
     * @param cssQuery 获取元素对象的表达式
     */
    public Element getElement(String cssQuery) {
        Element ele = doc.select(cssQuery).first();
        if (ele == null) {
            throw new NullPointerException("获取配置文件中" + cssQuery + "键对应的element对象时，值为空(null)!");
        }
        return ele;
    }

    /**
     * 概述：获取xml元素对象集合
     *
     * @param cssQuery 获取元素对象的表达式
     */
    public Elements getElements(String cssQuery) {
        Elements ele = doc.select(cssQuery);
        if (ele == null) {
            throw new NullPointerException("获取配置文件中" + cssQuery + "键对应的element对象时，值为空(null)!");
        }
        return ele;
    }

    /**
     * 概述：获取指定属性名称的元素对象
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public Element getElement(Element element, String attrName) {
        if (element == null) {
            throw new NullPointerException("获取配置文件中 " + attrName + "对应的值时，参数element对象为空(null)!");
        }
        return element.select("[name=" + attrName + "]").first();
    }

    /**
     * 概述：获取指定元素的属性值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public String getAttr(Element element, String attrName) {
        if (element == null) {
            throw new NullPointerException("获取配置文件中 element 对应的属性值时，参数element对象为空(null)!");
        }
        if (!element.hasAttr(attrName)) {
            return "";
        }
        String value = element.attr(attrName);
        if (value == null) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        log.info("Load config info, key: {}, value: {}", attrName, value);
        return value;
    }

    /**
     * 概述：获取指定元素按分隔符分隔后的属性值數組
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     * @param regex    分隔符
     */
    public String[] getAttrArray(Element element, String attrName, String regex) {
        String attrValue = getAttr(element, attrName);
        if (attrValue == null || "".equals(attrValue)) {
            return new String[]{};
        }
        return attrValue.split(regex);
    }

    /**
     * 概述：获取指定元素的text值
     *
     * @param element 元素对象
     */
    public String getText(Element element) {
        if (element == null) {
            throw new NullPointerException("获取配置文件中element的text对应的值时，element对象为空(null)!");
        }
        String value = element.text();
        if (value == null || value.trim().equals("")) {
            throw new NullPointerException("获取配置文件中" + element.nodeName() + "键对应的值时，值为空(null)!");
        }
        log.info("Load config info, key: {}, value: {}", element.nodeName(), value);
        return value;
    }

    /**
     * 概述：获取指定属性名称对应的value属性的值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public String getValue(Element element, String attrName) {
        String value = getElement(element, attrName).val();
        log.info("Load config info, key: {}, value: {}", attrName, value);
        return value;
    }

    /**
     * 概述：获取String类型的属性值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public String getStringValue(Element element, String attrName) {
        String value = getValue(element, attrName);
        // 若参数值为空，则抛出异常
        if (value == null || value.trim().equals("")) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        return value;
    }

    /**
     * 概述：获取Integer类型的属性值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public int getIntegerValue(Element element, String attrName) {
        String value = getValue(element, attrName);
        // 若参数值为空，则抛出异常
        if (value == null || value.trim().equals("")) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        return Integer.parseInt(value);
    }

    /**
     * 概述：获取Boolean类型的属性值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public boolean getBooleanValue(Element element, String attrName) {
        String value = getValue(element, attrName);
        // 若参数值为空，则抛出异常
        if (value == null || value.trim().equals("")) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        return Boolean.valueOf(value);
    }

    /**
     * 概述：获取double类型的属性值
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public double getDoubleValue(Element element, String attrName) {
        String value = getValue(element, attrName);
        // 若参数值为空，则抛出异常
        if (value == null || value.trim().equals("")) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        return Double.valueOf(value);
    }

    /**
     * 概述：获取指定元素对象的children集合的text值集合
     *
     * @param element 元素对象
     */
    public List<String> getChildrenListText(Element element) {
        List<String> list = new ArrayList<>();
        for (Element propertyElement : element.children()) {
            list.add(propertyElement.text());
        }
        return list;
    }

    /**
     * 概述：获取特定元素集合的text值拼装成字符串的值(特定标签的处理)
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     */
    public String getListText(Element element, String attrName) {
        Element subElement = element.select("[name=" + attrName + "] list").first();
        // 若参数值为空，则抛出异常
        if (subElement == null) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        List<String> textlist = getChildrenListText(subElement);
        // 若参数值为空，则抛出异常
        if (textlist == null || textlist.isEmpty()) {
            throw new NullPointerException("获取配置文件中" + attrName + "键对应的值时，值为空(null)或该键不存在!");
        }
        log.info("Load config info, key: {}, value: {}", attrName, textlist);
        return list2Str(textlist);
    }

    /**
     * 概述：集合转字符串
     */
    private String list2Str(List<String> list) {
        String listStr = "";
        for (String text : list) {
            if ("".equals(listStr)) {
                listStr = text;
            } else {
                listStr += "," + text;
            }
        }
        return listStr;
    }

    /**
     * 概述：获取特定元素的值用","分割成的int数组(特定处理)
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     * @param regex    分隔符
     */
    public Integer[] getIntArray(Element element, String attrName, String regex) {
        String[] array = getStringArray(element, attrName, regex);
        if (array == null || array.length == 0) {
            return new Integer[]{};
        }
        Integer[] newArray = new Integer[array.length];
        for (int i = 0; i < array.length; i++) {
            newArray[i] = Integer.valueOf(array[i]);
        }
        return newArray;
    }

    /**
     * 概述：获取特定元素的值用","分割成的int数组(特定处理)
     *
     * @param element  元素对象
     * @param attrName 元素的属性名称
     * @param regex    分隔符
     */
    public String[] getStringArray(Element element, String attrName, String regex) {
        String value = getValue(element, attrName);
        if (value == null || "".equals(value)) {
            return new String[]{};
        }
        return value.split(regex);
    }

    public String toString() {
        return doc.toString();
    }
}
