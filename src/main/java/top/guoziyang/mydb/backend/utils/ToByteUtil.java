package top.guoziyang.mydb.common;

import cn.hutool.core.util.ReflectUtil;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ToByteUtil {
    private static <T> List<T> readBufToList(ByteBuffer buf, Class<T> clazz) throws IllegalAccessException {
        ArrayList<T> ts = new ArrayList<>();
        T obj;
        while ((obj = readBufToObj(buf, clazz)) != null) {
            ts.add(obj);
        }
        return ts;
    }

    private static <T> T readBufToObj(ByteBuffer buf, Class<T> clazz) throws IllegalAccessException {
        try {
            if (!buf.hasRemaining()) {
                return null;
            }
            Map<String, Field> fieldMap = ReflectUtil.getFieldMap(clazz);
            T t = ReflectUtil.newInstance(clazz);
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
                Field field = entry.getValue();
                ReflectUtil.setAccessible(field);
                System.out.println(entry.getKey());
                String name = field.getType().getName();
                switch (name) {
                    case "int":
                        field.set(t, buf.getInt());
                        break;
                    case "long":
                        field.set(t, buf.getLong());
                        break;
                    case "top.guoziyang.mydb.common.DataBuf": {
                        int size = buf.getInt();
                        byte[] bytes = new byte[size];
                        buf.get(bytes);
                        field.set(t, new DataBuf(size, bytes));
                        break;
                    }
                }
            }
            return t;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static void putObjToBuf(Object obj, ByteBuffer buf) throws IllegalAccessException {
        Map<String, Field> fieldMap = ReflectUtil.getFieldMap(obj.getClass());
        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            Field field = entry.getValue();
            ReflectUtil.setAccessible(field);
            System.out.println(entry.getKey());
            Object o = field.get(obj);
            if (null == o) {
                continue;
            }
            String name = field.getType().getName();
            switch (name) {
                case "int":
                    buf.putInt((Integer) o);
                    break;
                case "long":
                    buf.putLong((Long) o);
                    break;
                case "top.guoziyang.mydb.common.ToByteUtil$DataBuf": {
                    DataBuf o1 = (DataBuf) o;
                    buf.putInt(o1.getSize());
                    buf.put(o1.getValue());
                    break;
                }
            }
        }
    }

    @Data
    static class People {
        // 14
        private DataBuf id = new DataBuf(10, new byte[10]);

        // 4
        private int age = 40;

        //14
        private DataBuf name = new DataBuf(10, new byte[10]);
    }

    @Data
    static class Son {
        private int age = 20;
    }

    @Data
    @AllArgsConstructor
    static class DataBuf {
        private int size;

        private byte[] value;
    }

    public static void main(String[] args) throws IllegalAccessException {
        People people = new People();
        ByteBuffer buf = ByteBuffer.allocate(1024);
        putObjToBuf(people, buf);
        putListToBuf(Arrays.asList(new Son(), new Son()), buf);
        // People people2 = new People();
        // readBufToObj(buf,people2);
        System.out.println();
    }

    private static void putListToBuf(List list, ByteBuffer buf) throws IllegalAccessException {
        for (Object o : list) {
            putObjToBuf(o, buf);
        }
    }
}
