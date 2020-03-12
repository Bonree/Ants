package com.bonree.bigdata;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.XmlParseUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {

    public static void main(String[] args) throws Exception {
//        String calcedValue = String.format(GranuleFinal.CALCED_TIME_VALUE, System.currentTimeMillis());
//        String calcedValue = RedisGlobals.CALCED_FLAG;
//        System.out.println(calcedValue.split(Delim.SIGN.value()).length);
//        System.out.println(calcedValue.startsWith("-1"));

//        long granuleTime = System.currentTimeMillis();
//
//        String timeStr = DateFormatUtils.format(new Date(granuleTime), GlobalContext.SEC_PATTERN);
//        Date date = DateFormatUtils.dayDiff(new Date(granuleTime), -1);
//        Date date1 = DateFormatUtils.monthDiff(new Date(granuleTime), -1);
//        String timeStr1 = DateFormatUtils.format(date, GlobalContext.SEC_PATTERN);
//        String timeStr2 = DateFormatUtils.format(date1, GlobalContext.SEC_PATTERN);
//        System.out.println(timeStr +"==" + timeStr1 +"=="+ timeStr2);
//        System.out.println(getCycleTime(5, 300, granuleTime));


//        String monthTime = DateFormatUtils.format(DateFormatUtils.setMonth(granuleTime), GlobalContext.SEC_PATTERN);

//        System.out.println("=="+monthTime);

//        try {
//            test10(2);
//        } catch (SQLException e){
//            System.out.println(e.getMessage());
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }

//        test4();
        test3();

    }

    public static <T> T gatherRecordsToSet(T oldValue, T curValue) {
        Set<T> tmpObj;
        if (oldValue instanceof Set<?>) {
            tmpObj = (Set<T>) oldValue;
        } else {
            tmpObj = new HashSet<>();
        }
        if (curValue instanceof Set<?>) {
            tmpObj.addAll((Set<T>) curValue);
        } else {
            tmpObj.add(curValue);
        }
        return (T) tmpObj;
    }
//
//    private static void test14(){
//        Granule gra = new Granule();
//        gra.setGranule(2);
//        gra.setBucket(3);
//        gra.setSkip(false);
//        gra.setLast(2);
//        gra.setNext(3);
//        gra.setTimeout(232);
//
//        GranuleMap granuleMap = new GranuleMap();
//        granuleMap.put(gra.getGranule(), gra);
//
//        String str = JSON.toJSONString(granuleMap);
//
//        System.out.println(str);
//        GranuleMap map = JSON.parseObject(str, GranuleMap.class);
//
//        System.out.println(map);
//
//        System.out.println(map.firstValue().getGranule());
//        System.out.println(map.firstGra());
//    }

//    private static void test13() throws Exception {
////        String path = "D:\\MyIdea\\Bonree_Ants_0.0.8\\conf\\schema.xml";
//        String path = "C:\\Users\\Administrator\\Desktop\\aa.xml";
//        Document doc = XPathUtil.instance.createDoc(path);
//        Node node = XPathUtil.instance.getNode(doc, "/sdk/header");
////        System.out.println(XpathUtils.instance.getNumberText(node, "sdkVersion"));
////        System.out.println(XpathUtils.instance.getStringText(node, "appId"));
////        System.out.println(XpathUtils.instance.getString(doc, "/sdk/header/appId"));
//
////        InputStream in = new ByteArrayInputStream();
//
//        Node node2 = XPathUtil.instance.getNode(doc, "/sdk/header//deviceId");
////        System.out.println(node2.getAttributes().getNamedItem("type"));
//        System.out.println();
////        NodeList node1 = XPathUtil.instance.getNodeList(doc, "/sdk/kafka//property");
////        System.out.println(XPathUtil.instance.getAttributeValue(node1, "value"));
//
//
//    }
//
//    private static List<String> getCycleTime(int cycle, int granule, long time){
//        // 根据粒度转移时间
//        Date granuleDate = GranuleCommons.granuleCalc(granule, time);
//        List<String> cycleList = new ArrayList<>();
//        // 根据周期计算出对比周期的时间集合
//        for (int i = 1; i <=cycle; i++) {
//            Date diffDate = DateFormatUtils.dayDiff(granuleDate, -i);
//            cycleList.add(DateFormatUtils.format(diffDate));
//        }
//        return cycleList;
//    }
//
//    private static void test12(){
//        List<Map<String, Object>> data= new ArrayList<>();
//        Map<String, Object> row1 = new HashMap<>();
//
//        int r1=(int)(1+Math.random()*(3000000));
//        row1.put("APPID",r1);
//        row1.put("APP_VER_ID",r1);
//        row1.put("DISTRICT",r1);
//        row1.put("NETSERVICE_ID", r1);
//        row1.put("IEMI_NUM","93");
//        row1.put("USERNAME","br_lq");
//        row1.put("VIEW_ID",r1);
//        row1.put("F_FLAG",r1);
//        row1.put("F_ID",r1);
//        data.add(row1);
//
//
//        Map<String ,Object> datas=new HashMap<String,Object>();
//        datas.put("monitorTime", System.currentTimeMillis());
//        datas.put("data", JSONArray.toJSONString(data));
//
//
//        JSONObject sdkresponse=JSON.parseObject(JSON.toJSONString(datas));
//
//        JSONArray sdkreponselist= JSONArray.parseArray(sdkresponse.getString("data"));
//    }
//
//    private static void test11(){
//        Calendar c = Calendar.getInstance();//可以对每个时间域单独修改
//
//        int year = c.get(Calendar.YEAR);
//        int month = c.get(Calendar.MONTH);
//        int day = c.get(Calendar.DATE);
//        int hour = c.get(Calendar.HOUR_OF_DAY);
//        int min = c.get(Calendar.MINUTE);
//        System.out.println(day);
//        System.out.println(hour);
//        System.out.println(min);
//    }
//
//    public static void test10(int a) throws Exception{
//
//        if (a == 1) {
//            throw new SQLException("=====sqlexception=====");
//        }
//        if (a == 2) {
//            throw new Exception("====Exception=====");
//        }
//
//        Records<Object> records = new Records<>();
//
//        Set<Object> sets = new HashSet<>();
//        sets.add("asfs");
//        sets.add("312");
//        records.put("sss", sets);
//
//        Set<String> setss = (Set<String>) records.get("sss");
//        System.out.println(setss);
//
//        String[] arr = setss.toArray(new String[0]);
//        for (int i = 0; i < arr.length; i++) {
//            System.out.println("==="+arr[i]);
//        }
//    }
//
//
//    public static<T> T test9(T[] test) throws Exception {
//
//        AntsConfig<Object> config = new AntsConfig<>();
//        config.put("aa",90);
//        config.put("bb", false);
//        config.put("cc", 9.9);
//        config.put("dd", 90L);
//        config.put("ee", "test");
//
//        int aa = (int)config.getNumber("aa");
//        boolean bb = config.getBoolean("bb");
//        double cc = (double) config.getNumber("cc");
//        long dd = (long)config.getNumber("dd");
//        System.out.println("==="+aa);
//        System.out.println("==="+bb);
//        System.out.println("==="+cc);
//        System.out.println("==="+dd);
//
//        List<String> topics = new ArrayList<>();
//        topics.add("1231");
//        topics.add("wre");
//        topics.add("vxv");
//        topics.add("6786");
//
//        System.out.println(topics);
//        for (int i = 0; i < topics.size(); i++) {
//            String topoic = GlobalContext.CONFIG.getNamespace() + Delim.UNDERLINE.value() + topics.get(i);
//            topics.set(i, topoic);
//        }
//        System.out.println(topics);
//
//
//        if (test[0] instanceof Long) {
//            System.out.println("long");
//        }
//        return (T) test[0];
//    }
//
//
//
//    @SuppressWarnings("unchecked")
//    public static void test8() throws Exception {
//
//        List<Records<?>> cycleList = new ArrayList<>();
//        Records<Object> re = new Records<>();
//        re.put("aa",42);
//        re.put("ab",43);
//        System.out.println(JSON.toJSONString(re));
//        cycleList.add(re);
//        JSONObject result = new JSONObject();
//        result.put("bizName", "qq");
//        result.put("time", "bb");
//        result.put("record", cycleList);
//        System.out.println(result.toJSONString());
//
//        List<String> ac = new ArrayList<>();
//        ac.add("rwerw");
//        System.out.println(JSON.toJSONString(ac));
//    }
//
//    public static void test7() throws Exception {
//        File file = new File("D:\\MyIdea\\Bonree_Ants_0.0.8\\conf\\schema.xml");
//        XmlParseUtils schemaConfig1 = new XmlParseUtils(file);
//        Element antsElement = schemaConfig1.getElement("ants");
//
//        File file1 = new File("D:\\MyWorkspaces\\BiaData\\Bonree_Ants_0.0.4\\conf\\app.xml");
//        XmlParseUtils appConfig = new XmlParseUtils(file1);
//
//        Element storageElement = appConfig.getElement("storage-plugin"); // storage信息
//        schemaConfig1.addElement(antsElement, storageElement.toString());
//        String tesst = schemaConfig1.toString();
//        System.out.println(tesst);
//
//        XmlParseUtils schemaConfig = new XmlParseUtils(tesst);
//        Element descElement = schemaConfig.getElement("data-desc");
//        String path = schemaConfig.getAttr(descElement, "type");
//        System.out.println("===="+path);
//        Map<String, String> descMap = new HashMap<>();
//        Elements datas = descElement.children();
//        for (Element ele : datas) {
//            String bizName = schemaConfig.getAttr(ele, "path");
//            String text = schemaConfig.getText(ele);
//            descMap.put(text, bizName);
//        }
//
//        System.out.println(descMap);
//        Map<String, Object> valueMap = new HashMap<>();
//        byte[] records = FileUtils.file2Byte("C:\\Users\\Administrator\\Desktop\\interact.data");
//        JSONObject jsonObj = JSON.parseObject(new String(records, "utf-8"));
//        System.out.println(jsonObj);
//        for (Entry<String, String> entry : descMap.entrySet()) {
//            Object value = getJosnValue(entry.getValue(), jsonObj);
//            if (value instanceof Number) {
//                System.out.println("number:" + value);
//            } else {
//                System.out.println("other:" + value);
//            }
//            valueMap.put(entry.getKey(), value);
//        }
//
//        System.out.println(valueMap);
//    }
//
//    public static Object getJosnValue(String path, JSONObject jsonObj) {
//
//        String[] pathArr = path.split("\\.");
//        JSONObject tmpObj = jsonObj;
//        for (int i = 0; i < pathArr.length; i++) {
//            String key = pathArr[i];
//            if (i < pathArr.length - 1) {
//                tmpObj = tmpObj.getJSONObject(key);
//            } else {
//                return tmpObj.get(key);
//            }
//        }
//
//        return null;
//    }
//
//    public static void test6() {
//        String cs = "((ad+cg_s)*2-adg/dfs)";
//
//        final int sz = cs.length();
//        StringBuilder sb = null;
//        for (int i = 0; i < sz; i++) {
//            char chars = cs.charAt(i);
//            if (sb == null) {
//                sb = new StringBuilder();
//            }
//            if (chars == '+' || chars == '-' || chars == '*' || chars == '/' || chars == '(' || chars == ')') {
//                if (sb.length() > 0 && !StringUtils.isNumeric(sb.toString())) {
//                    System.out.println("===" + sb);
//                }
//                sb = null;
//            } else {
//                sb.append(chars);
//            }
//        }
//
//    }
//
//    public static void test5() {
//        Map<String, Map<String, SchemaFields>> map = new HashMap<>();
//
//        Map<String, SchemaFields> bizmap = new HashMap<>();
//
//        SchemaFields ss = new SchemaFields();
//        ss.setName("aa");
//        ss.setValueType("long");
//        ss.setType("metric");
//        ss.setExpr("sum");
//        bizmap.put("host", ss);
//
//        map.put("20180507", bizmap);
//
//        String json = JSON.toJSONString(map);
//
//        Map<String, Map<String, SchemaData>> mm = JSON.parseObject(json, new TypeReference<Map<String, Map<String, SchemaData>>>() {
//        });
//        System.out.println("==" + mm);
//    }
//
//    public static void test4() {
//         byte[] records = FileUtils.file2Byte("C:\\Users\\Administrator\\Desktop\\e2c816a1-c24b-424a-9666-3bab47f15188");
//
//         DataResult data = KryoUtils.readFromByteArray(records);
//         for (Records record : data.get()) {
//         System.out.println("===" + record);
//         }

    //        CustomData snapshot = new CustomData();
//
//        List<CustomData.File> list = new ArrayList<>();
//        CustomData.File file = new CustomData.File();
//        file.setContent("asdfasdf".getBytes());
//        list.add(file);
//        snapshot.setDataList(list);
//        byte[] bytes = KryoUtils.writeToByteArray(snapshot);
//
//        CustomData list2 = KryoUtils.readFromByteArray(bytes);
//        System.out.println(list2);
//    }
//
    public static void test3() throws Exception {
//
        Schema schema = new Schema();
//        try {
            File file = new File("D:\\MyIdea\\Ants_Engine\\conf\\schema\\");
//            // File file = new File("D:\\MyWorkspaces\\BiaData\\Bigdata_Engine_0.0.4\\conf\\app.xml");
//
//            // Element element = schemaConfig.getElement("bigdata");
//            // String namespace = schemaConfig.getAttr(element, "namespace");
//            //
//            // Element storageElement = schemaConfig.getElement("storage"); // storage信息
//            // String temp = schemaConfig.getAttr(storageElement, "use");
//            // 1.加载业务描述信息
            File[] files = file.listFiles();
            if (files == null || files.length <= 0) {
                System.out.println("file dir if empty!");
                return;
            }
            for (File itemFile : files) {
                XmlParseUtils schemaConfig = new XmlParseUtils(itemFile);
                Element schemaElement = schemaConfig.getElement("schema");
                Elements datas = schemaElement.children();
                Elements fieldEle;
                SchemaFields fields;
                SchemaData data = null;
                Map<String, SchemaData> tempMap = new HashMap<>();
                for (Element ele : datas) {
                    String bizName = schemaConfig.getAttr(ele, "name");
                    String isGranule = schemaConfig.getAttr(ele, "granule");
                    String part = schemaConfig.getAttr(ele, "part");
                    if (!"".equals(part)) {
                        bizName = bizName + Delim.SIGN.value() + part;
                    }
                    if (!tempMap.containsKey(bizName)) {
                        tempMap.put(bizName, new SchemaData());
                    }
                    data = tempMap.get(bizName);
                    if (StringUtils.isNotEmpty(isGranule)) {
                        data.setGranule("true".equals(isGranule));
                    }
                    data.setBizName(bizName);
//                    fieldEle = ele.children();
//                    for (Element field : fieldEle) {
//                        fields = new SchemaFields();
//                        fields.setName(schemaConfig.getAttr(field, "name"));
//                        fields.setType(schemaConfig.getAttr(field, "type"));
//                        fields.setSource(schemaConfig.getAttr(field, "source").toLowerCase());
//                        fields.setValueType(schemaConfig.getAttr(field, "valueType"));
//                        String function = schemaConfig.getAttr(field, "expr").replace("()", "");
//                        fields.setExpr(function);
//                        String[] funArr = function.split("\\$");
//                        if (funArr.length == 2) {
//                            fields.setExpressList(getExpressFields(funArr[1]));
//                        }
//                        if (funArr[0].startsWith(AggType.MEDIAN.type())) {
//                            SchemaFields clone1 = Commons.clone(fields);
//                            clone1.setExpr(funArr.length == 2 ? "count$" + funArr[1] :"count$" );
//                            clone1.setName(clone1.getName() + "_count");
//                            data.addFields(clone1);
//
//                            SchemaFields clone2 = Commons.clone(fields);
//                            clone2.setExpr("sum");
//                            clone2.setName(clone2.getName() + "_sum");
//                            data.addFields(clone2);
//                        }
//                        data.addFields(fields);
//                    }
                    schema.putData(data);
//                }
            }
            System.out.println(JSON.toJSONString(schema));
//
//            StringBuilder sql = null;
//            SchemaData sd;
//            for (Entry<String, SchemaData> entry : schema.getData().entrySet()) {
//                sd = entry.getValue();
//                sql = new StringBuilder();
//                sql.append("INSERT INTO ");
//                sql.append(sd.getBizName());
//                sql.append("%s");
//                StringBuilder fieldStr = null;
//                StringBuilder valueStr = null;
//                for (SchemaFields field : sd.getFieldsList()) {
//                    if (SchemaGLobals.HIDDEN.equals(field.getType())) {
//                        continue;
//                    }
//                    if (fieldStr == null) {
//                        fieldStr = new StringBuilder(" (monitor_time,");
//                        valueStr = new StringBuilder(") VALUES (?,");
//                    } else {
//                        fieldStr.append(",");
//                        valueStr.append(",");
//                    }
//                    fieldStr.append(field.getName());
//                    valueStr.append("?");
//                }
//                valueStr.append(");");
//                sql.append(fieldStr);
//                sql.append(valueStr);
//                System.out.println(sql.toString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        long currTime2 = DateFormatUtils.dayDiff(new Date(), 1).getTime();
//        long currTime1 = DateFormatUtils.getDay(new Date()).getTime();
//
//        schema.setCreateTime(currTime1);
//        SchemaGLobals.SCHEMA_QUEUE.offer(schema);
//
//        String schemaJson = JSON.toJSONString(SchemaGLobals.SCHEMA_QUEUE);
//        System.out.println(schemaJson);
//        byte[] bb = schemaJson.getBytes("utf-8");
//
//        JSON.parseObject(new String(bb, "utf-8"), new TypeReference<LimitQueue<Schema>>() {
//        });
//
//        Map<Long, Schema> SCHEMA_MAP = new ConcurrentHashMap<>();
//        SCHEMA_MAP.put(currTime2, schema);
//        SCHEMA_MAP.put(currTime1, schema);
//        for (Entry<Long, Schema> entry : SCHEMA_MAP.entrySet()) {
//            System.out.println(entry.getKey());
//        }
//
//        List<Entry<Long, Schema>> sortList = new ArrayList<>(SCHEMA_MAP.entrySet());
//        Collections.sort(sortList, new Comparator<Entry<Long, Schema>>() {
//            @Override
//            public int compare(Entry<Long, Schema> o1, Entry<Long, Schema> o2) {
//                Long start1 = o1.getKey();
//                Long start2 = o2.getKey();
//                return start1.compareTo(start2); // 按key进行排序
//            }
//        });
//        System.out.println("===================");
//        for (Entry<Long, Schema> entry : sortList) {
//            System.out.println(entry.getKey());
//
//        }
//        long timeKey = sortList.get(0).getKey();
//        SCHEMA_MAP.remove(timeKey);
//        System.out.println("===================");
//        for (Entry<Long, Schema> entry : SCHEMA_MAP.entrySet()) {
//            System.out.println(entry.getKey());
//        }
    }
//
//    /**
//     * 概述：获取自定义函数表达工中的字段
//     * @param expressStr
//     * @return
//     */
//    private static List<String> getExpressFields(String expressStr) {
//        List<String> expressList = new ArrayList<String>();
//        final int sz = expressStr.length();
//        StringBuilder sb = null;
//        for (int i = 0; i < sz; i++) {
//            char chars = expressStr.charAt(i);
//            if (sb == null) {
//                sb = new StringBuilder();
//            }
//            if (chars == '&' || chars == '|' || chars == '=' || chars == '+' || chars == '-' || chars == '*' || chars == '/' || chars == '(' || chars == ')') {
//                if (sb.length() > 0) {
//                    expressList.add(sb.toString());
//                }
//                sb = null;
//            } else {
//                sb.append(chars);
//            }
//        }
//        return expressList;
//    }
//
//    @SuppressWarnings("unchecked")
//    public static void test2() {
//
//        Records<Object> re1 = new Records<Object>();
//        re1.put("12", 123);
//        re1.put("132", 123);
//        Records<Object> re = new Records<Object>();
//        re.put("aa", re1);
//        DataResult data = new DataResult();
//        data.setBizName("ww");
//        Collection<Object> tmp = re.get().values();
//        for (Object obj : tmp) {
//            data.add((Records<Object>) obj);
//        }
//        // byte[] bytes = KryoUitls.writeToByteArray(data);
//
//        // DataResult tmp1 = KryoUitls.readFromByteArray(bytes);
//
//        System.out.println(re.get());
//    }
//
//    public static void test1() {
//        DataSet data = new DataSet();
//        Records<Object> record = new Records<Object>();
//        DataResult result = new DataResult();
//        record.put("bb", 123);
//        data.addRecords("aaa", record);
//        result.add(record);
//        byte[] bytes = KryoUtils.writeToByteArray(result);
//        Records<Object> dd = KryoUtils.readFromByteArray(bytes);
//        System.out.println("===" + dd.get());
//    }
//
//    public static void test0() throws Exception {
//
//        String Str1 = "(a='SS'&c=10|d>e)";
//        CustomFunction.Expression map = new CustomFunction.Expression(Str1);
//        map.put("a", "SS");
//        map.put("c", 12);
//        map.put("d", 4);
//        map.put("e", 3);
//        System.out.println("替换变量后：" + map.toString());
//        System.out.println("计算结果：" + CustomFunction.eval(map.toString(), DataType.LONG));
//        System.out.println("======================================================");


//        String Str = "(a-b)*c+b-(c+b)";
//        CustomFunction.Expression r = new CustomFunction.Expression(Str);
//        r.put("a", 10);
//        r.put("b", 4.3);
//        r.put("c", 3);
//
//        System.out.println("替换变量后：" + r.toString());
//        System.out.println("计算结果：" + CustomFunction.eval(r.toString(), AggType.DOUBLE));
//        System.out.println("======================================================");
//
//        String Str2 = "a";
//        CustomFunction.Expression r2 = new CustomFunction.Expression(Str2);
//        r2.put("a", "1q2312wqrw");
//
//        System.out.println("替换变量后：" + r2.toString());
//        System.out.println("计算结果：" + CustomFunction.eval(r2.toString(), AggType.STRING));
    }
//
//    public static void test() {
//        Map<String, Object> map = new HashMap<String, Object>();
//        map.put("aa", 123);
//        map.put("bb", "234");
//        for (int i = 0; i < 100000; i++) {
//            if (i % 2 == 0) {
//                map.put("bb" + i, "234" + i);
//            } else {
//                map.put("aa" + i, 123 + i);
//            }
//        }
//
//        long start = System.currentTimeMillis();
//        for (Entry<String, Object> entry : map.entrySet()) {
//            Object value = entry.getValue();
//            if (value instanceof Integer) {
//                value = (int) value;
//            } else {
//                Integer.valueOf(value.toString());
//            }
//        }
//        System.out.println(System.currentTimeMillis() - start);
//    }

}
