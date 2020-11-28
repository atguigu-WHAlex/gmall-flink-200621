// 懒汉式
public class Test05_Singleton {
    public static void main(String[] args) {
        Singleton05 instance = Singleton05.SINGLETON05;
        Singleton05 instance2 = Singleton05.SINGLETON05;

        System.out.println(instance.hashCode());
        System.out.println(instance2.hashCode());
    }
}

enum Singleton05{
    SINGLETON05;

    public void say(){
        System.out.println("王义真帅");
    }
}
