/*
 *
 * Copyright (c) 2015-2020  Bonree Company
 * 北京博睿宏远科技发展有限公司  版权所有 2015-2020
 *
 * PROPRIETARY RIGHTS of Bonree Company are involved in the
 * subject matter of this material.  All manufacturing, reproduction, use,
 * and sales rights pertaining to this subject matter are governed by the
 * license agreement.  The recipient of this software implicitly accepts
 * the terms of the license.
 * 本软件文档资料是博睿公司的资产,任何人士阅读和使用本资料必须获得
 * 相应的书面授权,承担保密责任和接受相应的法律约束.
 *
 */
package com.bonree.ants.commons.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Utils {

    private static final String myStr = "0123456789abcdef";

    public static String getMD5Str(String value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(value.getBytes(StandardCharsets.UTF_8));
            byte[] byteArray = messageDigest.digest();
            StringBuilder md5StrBuff = new StringBuilder();
            for (byte aByteArray : byteArray) {
                if (Integer.toHexString(0xFF & aByteArray).length() == 1) {
                    md5StrBuff.append("0").append(Integer.toHexString(0xFF & aByteArray));
                } else {
                    md5StrBuff.append(Integer.toHexString(0xFF & aByteArray));
                }
            }
            return md5StrBuff.toString();
        } catch (NoSuchAlgorithmException e) {
            return value;
        }
    }

    public static long stringTolong(String str) {
        str = str.toLowerCase();
        long result = 0;
        int nr = 1;
        int length = myStr.length();
        for (int i = str.length() - 1; i >= 0; i--) {
            char param = str.charAt(i);
            for (int j = 0; j < length; j++) {
                if (myStr.charAt(j) == param) {
                    result += j * nr;
                    break;
                }
            }
            nr *= 16;
        }
        return result;
    }

    public static long getUrlNum(String url, long appid) {
        if (url == null)
            return -1L;
        long result = 0;
        try {
            if (url.contains("?")) {
                url = url.substring(0, url.indexOf("?"));
            }
            String md5 = Md5Utils.getMD5Str(url);
            String[] md5str = {md5.substring(0, 8), md5.substring(8, 16), md5.substring(16, 24), md5.substring(24, 32)};
            long[] long4 = {Md5Utils.stringTolong(md5str[0]), Md5Utils.stringTolong(md5str[1]), Md5Utils.stringTolong(md5str[2]), Md5Utils.stringTolong(md5str[3])};
            long chengji = long4[0] * long4[1] * long4[2] * long4[3];
            long chengji32 = (chengji << 32) >> 32;
            long roleid32 = appid << 32;
            result = roleid32 + chengji32;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}
