package com.bonree.ants.commons.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司 Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights
 * Reserved.
 * 
 * @Date: Dec 17, 2014 6:16:40 PM
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 日期格式化工具类
 ******************************************************************************/
public class DateFormatUtils {

    /**
     * 同步锁
     */
    private static final Lock lock = new ReentrantLock();

    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static Map<String, ThreadLocal<SimpleDateFormat>> sdfMap = new HashMap<>();

    /**
     * 概述：创建SimpleDateFormat实例
     * @param pattern 日期格式
     * @return SimpleDateFormat实例
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static DateFormat dateFormat(final String pattern) {
        ThreadLocal<SimpleDateFormat> threadLocal = sdfMap.get(pattern);
        if (threadLocal == null) {
            lock.lock();
            try {
                threadLocal = sdfMap.get(pattern);
                if (threadLocal == null) {
                    threadLocal = new ThreadLocal<SimpleDateFormat>() {
                        @Override
                        protected SimpleDateFormat initialValue() {
                            return new SimpleDateFormat(pattern);
                        }
                    };
                    sdfMap.put(pattern, threadLocal);
                }
            } finally {
                lock.unlock();
            }
        }

        return threadLocal.get();
    }

    /**
     * 概述：解析日期字符串
     * @param dateStr 日期字符串
     * @return 日期对象
     * @throws ParseException
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date parse(final String dateStr) throws ParseException {
        return dateFormat(FORMAT).parse(dateStr);
    }

    /**
     * 概述：解析日期字符串
     * @param dateStr 日期字符串
     * @param pattern 格式化的表达式
     * @return 日期对象
     * @throws ParseException
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date parse(final String dateStr, final String pattern) throws ParseException {
        return dateFormat(pattern).parse(dateStr);
    }

    /**
     * 概述：格式化日期对象
     * @param date 日期对象
     * @return 格式化后的日期字符串
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String format(final Date date) {
        return dateFormat(FORMAT).format(date);
    }

    /**
     * 概述：格式化日期对象
     * @param date 日期对象
     * @param pattern 格式化的表达式
     * @return 格式化后的日期字符串
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String format(final Date date, final String pattern) {
        return dateFormat(pattern).format(date);
    }

    /**
     * 概述：移除内存中SimpleDateFormat的临时对象
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static void close() {
        sdfMap.get(FORMAT).remove();
    }

    /**
     * 概述：移除内存中SimpleDateFormat的临时对象
     * @param pattern 格式化的表达式
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static void close(String pattern) {
        sdfMap.get(pattern).remove();
    }

    /**
     * 概述：获取指定时间的秒
     * @param time
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int getSeconds(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.SECOND);
    }

    /**
     * 概述：获取指定时间的分钟
     * @param time
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int getMinute(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.MINUTE);
    }

    /**
     * 概述：获取指定时间的小时
     * @param time
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int getHour(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 概述：获取指定时间的天
     * @param time
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int getDay(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 概述：设置当前秒
     * @param time 需要转换的日期(日期格式:Unix时间戳)
     * @param value 需要设置的时间
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date setSeconds(long time, int value) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.SECOND, value);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 概述：设置当前整点分钟
     * @Title: setMinute
     * @param time 需要转换的日期(日期格式:Unix时间戳)
     * @param value 需要设置的分钟
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date setMinute(long time, int value) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MINUTE, value);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 概述：设置当前整点小时
     * @param time 需要转换的日期(日期格式:Unix时间戳)
     * @param value 需要设置的小时(分钟数)
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date setHour(long time, int value) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.HOUR_OF_DAY, value / 60);// 除60得到小时数
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 概述：设置当前整点天
     * @param time 需要转换的日期(日期格式:Unix时间戳)
     * @param value 需要设置的天(小时数)
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Date setDay(long time, int value) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_MONTH, value / 24 / 60);// 转换得到天
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static Date addSeconds(Date date, long secs) {
        long sum = secs * 1000 + date.getTime();
        return new Date(sum);
    }

    /***
     * 概述：时间累加
     * @param date
     * @param mins
     */
    public static long addMins(long date, long mins) {
        return mins * 60 * 1000 + date;
    }

    /**
     * 概述：时间转换成毫秒 
     * @param timeObje
     * @param timePattern
     */
    public static long getTime(String timeObje, String timePattern) throws ParseException {
        return DateFormatUtils.parse(timeObje, timePattern).getTime();
    }

    /**
     * 概述：天日期加减
     * @param date 日期
     * @param df 往后用正整数,往前用负整数,单位为天
     */
    public static Date dayDiff(Date date, int df) {
        if (date == null) {
            return new Date();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, df);
        return calendar.getTime();
    }

    /**
     * 概述： 月日期加减
     * @param date 日期
     * @param mf 往后用正整数,往前用负整数,单位为月
     */
    public static Date monthDiff(Date date, int mf) {
        if (date == null) {
            return new Date();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, mf);
        return calendar.getTime();
    }

    /**
     * 概述：获取天粒度时间
     * @param date
     */
    public static Date getDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }
}
