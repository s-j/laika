package com.ntnu.laika.utils;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public class Pair<T1, T2> {
    T1 first;
    T2 second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
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

    @Override
    public final String toString() {
        return "Pair(" + first + ", " + second + ")";
    }
}
