package com.ntnu.laika.utils;

/**
 * @author <a href="mailto:olanatv@stud.ntnu.no">Ola Natvig</a>
 * @version $Id $.
 */
public abstract class AbstractLifecycleComponent implements LifecycleComponent {

    private Exception failCause;
    private boolean isRunning;


    @Override
    public void restart() {
        stop();
        start();
    }

    protected void setIsRunning(boolean running) {
        isRunning = running;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean isFailed() {
        return failCause == null;
    }

    protected void setFailCause(Exception e) {
        failCause = e;
    }

    @Override
    public Exception getFailCause() {
        return failCause;
    }
}
