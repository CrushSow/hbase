package test;

/**
 * PACKAGE_NAMW   test
 * DATE      10
 * Author     Crush
 */
public class Student {
    public String name;

    public Student() {
    }

    public Student(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                '}';
    }
}
