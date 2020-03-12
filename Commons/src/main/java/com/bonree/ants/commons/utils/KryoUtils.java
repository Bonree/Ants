package com.bonree.ants.commons.utils;

import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.etl.model.DataSet;
import com.bonree.ants.plugin.storage.model.DataResult;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018-4-9 下午03:31:04
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description:
 ******************************************************************************/
public class KryoUtils {

    private static final int bufferSize = 8192;

    // 每个线程的 Kryo 实例
    private static final ThreadLocal<Kryo> kryoLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            // 支持对象循环引用（否则会栈溢出）
            kryo.setReferences(true); // 默认值就是 true

            // 不强制要求注册类（注册行为无法保证多个 JVM 内同一个类的注册编号相同；而且业务系统中大量的 Class 也难以一一注册）
            kryo.setRegistrationRequired(false); // 默认值就是 false
            kryo.register(DataResult.class);
            kryo.register(DataSet.class);
            kryo.register(CustomData.File.class);

            // Fix the NPE bug when deserializing Collections.
            //((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            SynchronizedCollectionsSerializer.registerSerializers(kryo);
            return kryo;
        }
    };

    /**
     * 获得当前线程的 Kryo 实例
     *
     * @return 当前线程的 Kryo 实例
     */
    public static Kryo getInstance() {
        return kryoLocal.get();
    }

    /**
     * 将对象【及类型】序列化为字节数组
     *
     * @param obj 任意对象
     * @return 序列化后的字节数组
     */
    public static <T> byte[] writeToByteArray(T obj) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream, bufferSize);

        Kryo kryo = getInstance();
        kryo.writeClassAndObject(output, obj);
        output.flush();

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 将字节数组反序列化为原对象
     *
     * @param byteArray writeToByteArray 方法序列化后的字节数组
     * @return 原对象
     */
    @SuppressWarnings("unchecked")
    public static <T> T readFromByteArray(byte[] byteArray) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(byteArrayInputStream, bufferSize);

        Kryo kryo = getInstance();
        return (T) kryo.readClassAndObject(input);
    }

    /**
     * 将对象序列化为字节数组
     *
     * @param obj 任意对象
     * @return 序列化后的字节数组
     */
    public static <T> byte[] writeObjectToByteArray(T obj) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream, bufferSize);

        Kryo kryo = getInstance();
        kryo.writeObject(output, obj);
        output.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 将字节数组反序列化为原对象
     *
     * @param byteArray 方法序列化后的字节数组
     * @param clazz     原对象的 Class
     * @return 原对象
     */
    public static <T> T readObjectFromByteArray(byte[] byteArray, Class<T> clazz) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(byteArrayInputStream, bufferSize);

        Kryo kryo = getInstance();
        return kryo.readObject(input, clazz);
    }
}
