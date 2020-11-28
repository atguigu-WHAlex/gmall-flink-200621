// 懒汉式
public class Test03_Singleton {
    public static void main(String[] args) {
        Singleton02 instance = Singleton02.getInstance();
        Singleton02 instance2 = Singleton02.getInstance();
        System.out.println(instance.hashCode());
        System.out.println(instance2.hashCode());
    }
}

class Singleton03{
    //1.私有化构造器
    private Singleton03(){}
    //2.私有化静态属性
    private volatile static Singleton03 instance;
    //3.提供公共的静态获取方法

    public static Singleton03 getInstance() {
        if(instance == null){
            synchronized (""){
                if (instance == null){
                    instance = new Singleton03();
                }
            }

        }
        return instance;
    }
}
