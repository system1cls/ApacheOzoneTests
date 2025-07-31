package org.example;

public class Conter {
    volatile int counter = 0;

    synchronized public void inc() {
        counter += 10;
        if (counter % 1000 == 0) {
            System.out.println("Cnt keys = " + counter);
        }
    }
}
