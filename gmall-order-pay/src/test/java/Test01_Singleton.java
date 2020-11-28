// 饿汉式
public class Test01_Singleton {
    public static void main(String[] args) {
        Singleton01 instance = Singleton01.getInstance();
        Singleton01 instance2 = Singleton01.getInstance();
        System.out.println(instance.hashCode());
        System.out.println(instance2.hashCode());
    }
}

class Singleton01{
    //1.私有化构造器
    private Singleton01(){}
    //2.私有化静态属性
    private static Singleton01 instance = new Singleton01();
    //3.提供公共的静态获取方法

    public static Singleton01 getInstance() {
        return instance;
    }
}
