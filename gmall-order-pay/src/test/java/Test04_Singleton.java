// 懒汉式
public class Test04_Singleton {
    public static void main(String[] args) {
        Singleton04 instance = Singleton04.getInstance();
        Singleton04 instance2 = Singleton04.getInstance();
        System.out.println(instance.hashCode());
        System.out.println(instance2.hashCode());
    }
}

class Singleton04{
    //1.私有化构造器
    private Singleton04(){}
    //2.私有化静态内部类
    private static class InnerClass{
        private static final Singleton04 INSTANCE = new Singleton04();
    }


    //3.提供公共的静态获取方法

    public static Singleton04 getInstance() {

        return InnerClass.INSTANCE;
    }

}
