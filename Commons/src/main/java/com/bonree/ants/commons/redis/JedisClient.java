package com.bonree.ants.commons.redis;

import bonree.proxy.JedisPipelineProxy;
import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.Delim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月23日 下午2:02:21
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: jedis客户端工具类
 * ****************************************************************************
 */
public class JedisClient {

    private static final Logger log = LoggerFactory.getLogger(JedisClient.class);
    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";

    /**
     * 初始连接
     */
    public static JedisProxy init() {
        String prefix = GlobalContext.getConfig().getNamespace() + Delim.UNDERLINE.value() + RedisGlobals.PREFIX_STORM;
        return new JedisProxy(RedisGlobals.REDIS_VERSION, GlobalContext.getConfig().getRedisCluster(), prefix);
    }

    /**
     * 初始连接
     */
    public static JedisProxy init(String customPrefix) {
        String prefix = GlobalContext.getConfig().getNamespace() + Delim.UNDERLINE.value() + customPrefix;
        return new JedisProxy(RedisGlobals.REDIS_VERSION, GlobalContext.getConfig().getRedisCluster(), prefix);
    }

    /**
     * 概述：同步锁
     *
     * @param redis       redis连接对象
     * @param syncLockKey 同步锁存redis时的key
     * @return true: 获取到同步锁; false:未获取到同步锁.
     */
    private static boolean syncLock(JedisProxy redis, String syncLockKey) {
        if (redis == null || syncLockKey == null) {
            return false;
        }
        long exist = redis.setnx(syncLockKey, "");
        // 获取到同步锁
        return exist == 1;
    }

    /**
     * 概述：同步锁
     *
     * @param redis       redis连接对象
     * @param syncLockKey 同步锁存redis时的key
     * @param expire      同步锁的过期时间
     * @return true: 获取到同步锁; false:未获取到同步锁.
     */
    public static boolean syncLock(JedisProxy redis, String syncLockKey, int expire) {
        if (syncLock(redis, syncLockKey)) { // 获取到同步锁
            redis.expire(syncLockKey, expire);
            return true;
        }
        return false;
    }


    /**
     * 获取分布式锁
     *
     * @param jedis      Redis客户端
     * @param lockKey    锁
     * @param value      请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     */
    public static boolean lock(JedisProxy jedis, String lockKey, String value, int expireTime) {
        String result = jedis.set(lockKey, value, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
        return LOCK_SUCCESS.equals(result);
    }

    /**
     * 获取分布式锁
     *
     * @param jedis      Redis客户端
     * @param lockKey    锁
     * @param expireTime 超期时间
     * @return 是否获取成功
     */
    public static boolean lock(JedisProxy jedis, String lockKey, int expireTime) {
        return lock(jedis, lockKey, "znl", expireTime);
    }

    /**
     * 关闭连接
     */
    public static void close(JedisPipelineProxy pipeline) {
        try {
            if (pipeline != null) {
                pipeline.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static void close(JedisProxy redis) {
        try {
            if (redis != null) {
                redis.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static void close(JedisPipelineProxy pipeline, JedisProxy redis) {
        try {
            if (pipeline != null) {
                pipeline.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            if (redis != null) {
                redis.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 概述：删除redis上的数据
     *
     * @param redisKey
     */
    public static void deleteRedisByKey(String redisKey) {
        JedisProxy redis = null;
        try {
            redis = init();
            redis.del(redisKey);
        } catch (Exception ex) {
            log.error("deleteRedisKey error! {}", redisKey, ex);
        } finally {
            close(redis);
        }
    }

    /**
     * 概述：保存数据到redis
     *
     * @param redisKey 数据key
     * @param value    数据内容
     */
    public static void setByKey(String redisKey, byte[] value) {
        JedisProxy redis = null;
        try {
            redis = init();
            redis.set(redisKey.getBytes(StandardCharsets.UTF_8), value);
        } catch (Exception ex) {
            log.error("setRedisKey error! {}", redisKey, ex);
        } finally {
            close(redis);
        }
    }

    /**
     * 概述：保存数据到redis
     *
     * @param redisKey 数据key
     * @param value    数据内容
     */
    public static void setByKey(String redisKey, String value) {
        setByKey(redisKey, value, -1);
    }

    /**
     * 概述：保存数据到redis,如果key存储则不做任何动作
     *
     * @param redisKey 数据key
     * @param value    数据内容
     * @param expire   过期时间
     */
    public static void setnxByKey(String redisKey, String value, int expire) {
        JedisProxy redis = null;
        try {
            redis = init();
            if (redis.exists(redisKey)) {
                return;
            }
            redis.set(redisKey, value);
            if (expire != -1) {
                redis.expire(redisKey, expire);
            }
        } catch (Exception ex) {
            log.error("setRedisKey error! {}", redisKey, ex);
        } finally {
            close(redis);
        }
    }

    /**
     * 概述：保存数据到redis
     *
     * @param redisKey 数据key
     * @param value    数据内容
     * @param expire   过期时间
     */
    public static void setByKey(String redisKey, String value, int expire) {
        JedisProxy redis = null;
        try {
            redis = init();
            redis.set(redisKey, value);
            if (expire != -1) {
                redis.expire(redisKey, expire);
            }
        } catch (Exception ex) {
            log.error("setRedisKey error! {}", redisKey, ex);
        } finally {
            close(redis);
        }
    }

    /**
     * 概述：获取redis数据
     *
     * @param redisKey 数据key
     */
    public static byte[] getRedisByKey(String redisKey) {
        JedisProxy redis = null;
        try {
            redis = init();
            return redis.get(redisKey.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            log.error("getRedisKey error! {}", redisKey, ex);
        } finally {
            close(redis);
        }
        return null;
    }

}
