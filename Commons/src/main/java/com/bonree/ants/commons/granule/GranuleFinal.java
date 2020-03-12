package com.bonree.ants.commons.granule;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月8日 下午5:33:16
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总相关属性
 * ****************************************************************************
 */
public class GranuleFinal {

    /**
     * 汇总数据在REDIS中的KEY(rpush),normal#1#20151111160000#VIEW_1
     */
    public static final String DATA_KEY = "%s#%d#%s#%s_%s";

    /**
     * 汇总数据的时间粒度在REDIS中的KEY(hset),normal#60#time_flag, field:粒度时间,value:汇总当前粒度时间所需要的其它时间集合字符串(逗号分隔)
     */
    public static final String AGG_TIME_KEY = "%s#%d#time_flag";

    /**
     * 存储当前汇总正常的业务类型(hset),normal#60#20180426141800#curr_agg_biz,field:业务类型,value:分桶数
     */
    public static final String CURR_AGG_BIZ_KEY = "%s#%s#%s#curr_agg_biz";

    /**
     * 汇总数据的超时时间在REDIS中的KEY(hget),拓扑名称#60#timeout,field:粒度时间,value:累计的超时时间
     */
    public static final String TIMEOUT_KEY = "%s#%s#timeout";

    /**
     * 粒度汇总时最后一个粒度的计算时间间隔key
     */
    public static final String TAIL_INVERTAL_KEY = "tail_granule_invertal";

    /**
     * 存储某个拓扑计算到的时间位置(get),拓扑名称#CALC_POSITION
     */
    public static final String CALC_POSITION_KEY = "%s#calc_position";

    /**
     * 用于获取拓扑的所有粒度(lpush),拓扑名称#ALL_GRANULE
     */
    public static final String ALL_GRANULE_KEY = "%s#all_granule";

    /**
     * 存储当前正在计算的粒度(get),拓扑名称#CURR_GRANULE
     */
    public static final String CURR_GRANULE_KEY = "%s#curr_granule";

    /**
     * 用于标识当前粒度的活跃数是否正在计算(exists), active#86400##active_calced
     */
    public static final String ACTIVE_CALC_KEY = "%s#%s#active_calced";

    /**
     * 存储活跃数计算时间集合,set集合结构 active#86400#active_time
     */
    public static final String ACTIVE_AGG_KEY = "%s#%s#active_time";

    /**
     * 表示当前粒度计算完成的时间, "-1#long类型时间戳"
     */
    public static final String CALCED_TIME_VALUE = "%s#%d";

    /**
     * 标识粒度汇总的第一个粒度汇总的情况,用于控制预处理拓扑是否过滤掉重复的数据.normal#20180426141800#calc_status
     */
    public static final String GRANULE_CALC_STATUS_KEY = "%s#%s#calc_status";


    /**
     * 天粒度,主要用于日活和月活计算,单位:秒
     */
    public final static int DAY_GRANULE = 86400;

    /**
     * 天粒度计算间隔
     */
    public static final int CALC_DAY_INTERVAL = 2 * 60 * 60;

    /**
     * 汇总数据的过期时间,单位:秒
     */
    public static final Integer DATA_EXPIRE = 2 * 24 * 60 * 60;

    /**
     * 当前正在计算的业务的标识过期时间，单位：秒
     */
    public static final Integer CURRENT_CALC_EXPIRE = 60 * 60;

    /**
     * 当前粒度KEY的标识
     */
    public static final String CURR_GRANULE_FLAG = "1000000";

    /**
     * 汇总当前粒度时,最后一个时间KEY的标识
     */
    public static final String LAST_TIME_FLAG = "1000001";

    /**
     * 已经汇总过的时间集合的标识
     */
    public static final String CALCED_TIME_FLAG = "1000002";

    /**
     * 数据业务类型的标识
     */
    public static final String BIZ_TYPE_FLAG = "1000003";

    /**
     * 数据业务名称的标识
     */
    public static final String BIZ_NAME_FLAG = "1000004";

    /**
     * 当前分桶编号的标识
     */
    public static final String CURR_BUCKET_FLAG = "1000005";

    /**
     * 特殊数据标识
     */
    public static final long FLAG = 999999999;

}
