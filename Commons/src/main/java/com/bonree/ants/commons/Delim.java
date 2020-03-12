package com.bonree.ants.commons;
/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月12日 下午3:32:23
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 分割符
 *****************************************************************************
 */
public enum Delim {
    NONE(""), 
    UNDERLINE("_"), 
    BLANK(" "),
    LINE_FEED_LINUX("\n"), 
    LINE_FEED_WIN("\r\n"),
    ASCII_159("ƒ"),
    COMMA(","), 
    SPRIT("/"), 
    SEMICOLON(";"),
    DOT("."), 
    COLON(":"), 
    SIGN("#");
    private String delim;

    Delim(String delim) {
        this.delim = delim;
    }

    public String value() {
        return delim;
    }

    public Delim valueof(String delim) {
        if (delim == null) {
            return null;
        }
        if (NONE.value().equals(delim)) {
            return NONE;
        }
        if (UNDERLINE.value().equals(delim)) {
            return UNDERLINE;
        }
        if (BLANK.value().equals(delim)) {
            return BLANK;
        }
        if (LINE_FEED_LINUX.value().equals(delim)) {
            return LINE_FEED_LINUX;
        }
        if (LINE_FEED_WIN.value().equals(delim)) {
            return LINE_FEED_WIN;
        }
        if (ASCII_159.value().equals(delim)) {
            return ASCII_159;
        }
        if (COMMA.value().equals(delim)) {
            return COMMA;
        }
        if (SPRIT.value().equals(delim)) {
            return SPRIT;
        }
        if (SEMICOLON.value().equals(delim)) {
            return SEMICOLON;
        }
        if (DOT.value().equals(delim)) {
            return DOT;
        }
        if (COLON.value().equals(delim)) {
            return COLON;
        }
        if (SIGN.value().equals(delim)) {
            return SIGN;
        }
        return null;
    }
}
