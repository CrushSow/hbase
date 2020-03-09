package test;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * PACKAGE_NAMW   test
 * DATE      10
 * Author     Crush
 *
 * 类的反射
 * 什么是反射 反射就是把java类中的各种成分映射成一个个java对象
 *
 */
public class ClassFor {
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> aClass = Class.forName("test.Student");

        Constructor<?> constructor = aClass.getConstructor();
        System.out.println(constructor);

        Field field = aClass.getField("name");

        Object obj = aClass.getConstructor().newInstance();
        field.set(obj,"yingzijie");


        Method toString = aClass.getDeclaredMethod("toString", String.class);
        toString.invoke(obj);

        Class<?> aClass1 = Class.forName("test.student1");

        Method main = aClass1.getMethod("main", String[].class);
        main.invoke(null, (Object)new String[]{"a","b","c"});

    }
    public static String getValue(String key) throws IOException {
        Properties pro = new Properties();//获取配置文件的对象
        FileReader in = new FileReader("pro.txt");//获取输入流
        pro.load(in);//将流加载到配置文件对象中
        in.close();
        return pro.getProperty(key);//返回根据key获取的value值
    }
}
