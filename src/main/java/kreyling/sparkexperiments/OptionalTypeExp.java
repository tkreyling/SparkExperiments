package kreyling.sparkexperiments;


import scala.None;
import scala.Option;

public class OptionalTypeExp {
    public static class MyOption {}
    public static class None extends MyOption {}
    public static class Some extends MyOption {}

    public static class Knowledge {}
    public static class Person<T extends MyOption> {}

    public static void main(String[] args) {
        Person<Some> somePerson = new Person<>();
        doStuff(somePerson);
    }

    private static void doStuff(Person<Some> nonePerson) {
    }
}
