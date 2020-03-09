/**
 * PACKAGE_NAMW   PACKAGE_NAME
 * DATE      05
 * Author     Crush
 */
public class Test {
    public static void main(String[] args) {
        Node node = new Node();
        Node aa=node;
        Node bb=aa;
        System.out.println(aa);
        System.out.println(bb);
    }
}

class Node{
    private int name;
    private Object value;
    public Node next;
}
