public class aa {

    public static void main(String[] args) {

        int value = 1000000;

        long start = System.currentTimeMillis();

        int i = value % 77777777;
        System.out.println(i);

        long second = System.currentTimeMillis();

        int i1 = value & ((1 << 10) - 1);
        System.out.println(i1);

        long end = System.currentTimeMillis();

        System.out.println(second - start);
        System.out.println(end - second);


    }

}
