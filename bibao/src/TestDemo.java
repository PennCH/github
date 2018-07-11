public class TestDemo {

    private IncreaseAge defineUser(){
        User user = new User();

        user.setAge(66);
        user.setName("laowang");
        System.out.println("22222");
        return ()->{
            System.out.println("333333");user.setAge(user.getAge()+1);System.out.println(user);};


    }

    public static void main(String[] args) {
        TestDemo demo = new TestDemo();
        System.out.println("111111");
        IncreaseAge increaseAge = demo.defineUser();
        System.out.println("44444");
        increaseAge.increaseAge();
        System.out.println("555555");
    }
}
