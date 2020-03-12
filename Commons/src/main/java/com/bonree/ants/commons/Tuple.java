package com.bonree.ants.commons;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2019/3/19 14:30
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 元组对象
 * *****************************************************************************
 */
public class Tuple<L, R> {

    private L left;
    private R right;

    public Tuple(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public void setLeft(L left) {
        this.left = left;
    }

    public L getLeft() {
        return this.left;
    }

    public void setRight(R right) {
        this.right = right;
    }

    public R getRight() {
        return this.right;
    }

    @Override
    public String toString() {
        return this.left + "-" + this.right;
    }
}
