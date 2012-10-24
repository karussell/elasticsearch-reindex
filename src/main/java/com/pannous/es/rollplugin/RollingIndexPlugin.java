package com.pannous.es.rollplugin;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * @author Peter Karich
 */
public class RollingIndexPlugin extends AbstractPlugin {

    private volatile boolean startedThread = false;
    private Thread thread;

    public String name() {
        return "rollindex";
    }

    public String description() {
        return "Rolling Index Plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(RollAction.class);
        }

        if (!startedThread) {
            startedThread = true;
            // TODO create only one thread per cluster
            // TODO how to get an instance of the action?
            // thread = new RollingThread(action);
        }
    }

    static class RollingThread extends Thread {

        @Override public void run() {
            while (true) {
                // TODO should we use quartz or cron4j or an own simple implementation?
                // what if cluster/node restarts?                
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    break;
                }
            }
        }
    }
}
