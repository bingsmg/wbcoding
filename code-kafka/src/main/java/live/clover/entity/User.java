package live.clover.entity;

/**
 * @author weibb
 */
public class User {
    private String firstName;
    private String lastName;
    private int age;

    @Override
    public String toString() {
        return "User{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                '}';
    }
}
