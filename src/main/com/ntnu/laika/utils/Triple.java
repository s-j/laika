package com.ntnu.laika.utils;

/**
 * @author <a href="mailto:simon.jonassen@gmail.com">Simon Jonassen</a>
 * @version $Id $.
 */
public class Triple<T1, T2, T3> {
    T1 first;
    T2 second;
    T3 third;
    
    public Triple(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public final T1 getFirst() {
        return first;
    }

    public final void setFirst(T1 first) {
        this.first = first;
    }

    public final T2 getSecond() {
        return second;
    }

    public final void setSecond(T2 second) {
        this.second = second;
    }
    
    public final T3 getThird() {
        return third;
    }

    public final void setThird(T3 third) {
        this.third = third;
    }

    @Override
    public final String toString() {
        return "Triple(" + first + ", " + second + ", " + third + " )";
    }
}
