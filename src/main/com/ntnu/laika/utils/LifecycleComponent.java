package com.ntnu.laika.utils;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public interface LifecycleComponent {

    void start();

    void restart();

    void stop();

    boolean isRunning();

    boolean isFailed();

    Exception getFailCause();

}
