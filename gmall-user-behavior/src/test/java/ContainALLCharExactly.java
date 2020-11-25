import java.util.Arrays;

/*给定长度为m的字符串aim，以及一个长度为n的字符串str，
问能否在str中找到一个长度为m的连续子串，
使得这个子串刚好由aim的m个字符组成，顺序无所谓，
返回任意满足条件的一个子串的起始位置，未找到返回-1*/




public class ContainALLCharExactly {
    public static int containExactly1(String s, String a) {
        if (s == null || a == null || s.length() < a.length()) {
            return -1;
        }
        char[] aim = a.toCharArray();
        Arrays.sort(aim);
        String aimSort = String.valueOf(aim);

        for (int L = 0; L < s.length(); L++) {
            for (int R = L; R < s.length(); R++) {
                char[] cur = s.substring(L, R + 1).toCharArray();
                Arrays.sort(cur);
                String curSort = String.valueOf(cur);
                if (curSort.equals(aimSort)) {
                    return L;
                }
            }
        }
        return -1;
    }


    public static int containExactly2(String s, String a) {
        if (s == null || a == null || s.length() < a.length()) {
            return -1;
        }
        char[] str = s.toCharArray();
        char[] aim = a.toCharArray();

        for (int L = 0; L <= str.length - aim.length; L++) {
            if (isCountEqual(str, L, aim)) {
                return L;
            }
        }
        return -1;
    }


    private static boolean isCountEqual(char[] str, int L, char[] aim) {
        int[] count = new int[256];
        for (int i = 0; i < aim.length; i++) {
            count[aim[i]]++;
        }
        for (int i = 0; i < aim.length; i++) {
            if (count[str[L + i]]-- == 0) {
                return false;
            }
        }
        return true;
    }



    public static int containExactly3(String s, String a) {
        if (s == null || a == null || s.length() < a.length()) {
            return -1;
        }
        char[] aim = a.toCharArray();
        int[] count = new int[256];
        for (int i = 0; i < aim.length; i++) {
            count[aim[i]]++;
        }
        int M = aim.length;
        char[] str = s.toCharArray();
        int inValidTimes = 0;
        int R = 0;
        for (; R < M; R++) {
            if (count[str[R]]-- <= 0) {
                inValidTimes++;
            }
        }
        for (; R < str.length; R++) {
            if (inValidTimes == 0) {
                return R - M;
            }
            if (count[str[R]]-- <= 0) {
                inValidTimes++;
            }
            if (count[str[R - M]]++ < 0) {
                inValidTimes--;
            }
        }
        return inValidTimes == 0 ? R - M : -1;
    }

    public static String getRandomString(int possibilities, int maxSize) {
        char[] ans = new char[(int) (Math.random() + maxSize) + 1];
        for (int i = 0; i < ans.length; i++) {
            ans[i] = (char) ((int) (Math.random() * possibilities) + 'a');
        }
        return String.valueOf(ans);
    }

    public static void main(String[] args) {
        int possibilities = 26;
        int strMaxSize = 100;
        int aimMaxSize = 10;
        int testTimes = 500000;

        long begin = System.currentTimeMillis();
        for (int i = 0; i < testTimes; i++) {
            String str = getRandomString(possibilities, strMaxSize);
            String aim = getRandomString(possibilities, aimMaxSize);
            containExactly1(str, aim);
        }
        long finish = System.currentTimeMillis();
        System.out.println("方法一：" + (finish - begin));

        begin = System.currentTimeMillis();
        for (int i = 0; i < testTimes; i++) {
            String str = getRandomString(possibilities, strMaxSize);
            String aim = getRandomString(possibilities, aimMaxSize);
            containExactly2(str, aim);
        }
        finish = System.currentTimeMillis();
        System.out.println("方法二：" + (finish - begin));
        begin = System.currentTimeMillis();
        for (int i = 0; i < testTimes; i++) {
            String str = getRandomString(possibilities, strMaxSize);
            String aim = getRandomString(possibilities, aimMaxSize);
            containExactly3(str, aim);
        }
        finish = System.currentTimeMillis();
        System.out.println("方法三：" + (finish - begin));
    }
}