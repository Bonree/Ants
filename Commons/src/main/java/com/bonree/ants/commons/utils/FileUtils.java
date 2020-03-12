package com.bonree.ants.commons.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018-4-9 下午06:41:37
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 文件操作工具类
 ******************************************************************************/
public class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 概述：验证文件是否存在
     *
     * @param filePath 文件路径
     * @param fileName 文件名称
     */
    public static boolean exists(String filePath, String fileName) {
        File file;
        try {
            File dir = new File(filePath);
            if (!dir.exists()) {
                return false;
            }
            file = new File(filePath + File.separator + fileName);
            return file.exists();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    /**
     * 概述：byte[]转成file写到指定路径下
     *
     * @param buf      源字节数组
     * @param filePath 保存文件的路径
     * @param fileName 保存文件的名称
     */
    public static void byte2File(byte[] buf, String filePath, String fileName) {
        if (buf == null || buf.length == 0) {
            log.warn("byte is null or empty! path: {}", filePath);
            return;
        }
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file;
        try {
            File dir = new File(filePath);
            if (!dir.exists()) { // 文件目录不存在则创建
                if (!dir.mkdirs()) {
                    log.error("Create directory failed! directory: " + dir.getPath());
                }
            }
            file = new File(filePath + File.separator + fileName);
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(buf);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 概述：读取指定路径的文件
     *
     * @param filePath 文件完整路径(包含文件名)
     */
    public static byte[] file2Byte(String filePath) {
        byte[] buffer = null;
        FileInputStream fis = null;
        ByteArrayOutputStream bos = null;
        try {
            fis = new FileInputStream(new File(filePath));
            bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            buffer = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(fis, bos);
        }

        return buffer;
    }

    /**
     * 概述：读取指定路径的文件
     *
     * @param file 文件对象
     */
    public static byte[] file2Byte(File file) {
        byte[] buffer = null;
        FileInputStream fis = null;
        ByteArrayOutputStream bos = null;
        try {
            fis = new FileInputStream(file);
            bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            buffer = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(fis, bos);
        }
        return buffer;
    }

    /**
     * 概述：保存错误文件
     *
     * @param data     文件内容
     * @param rootPath 文件根目录
     * @param filePath 文件路径
     * @param fileName 文件名称
     */
    public static void saveErrorFile(byte[] data, String rootPath, String filePath, String fileName) {
        FileUtils.byte2File(data, rootPath.concat(filePath), fileName);
    }

    private static void close(FileInputStream fis, ByteArrayOutputStream bos){
        if (fis != null){
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (bos != null){
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
