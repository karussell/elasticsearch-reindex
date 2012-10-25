package com.pannous.es.rollplugin;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * @author Peter Karich
 */
public class RollingIndexPlugin extends AbstractPlugin {

    private volatile boolean startedThread = false;
    private Thread thread;
    @Inject
    private RollAction action;
    protected final ESLogger logger = Loggers.getLogger(RollingIndexPlugin.class);

    public String name() {
        return "rollindex";
    }

    public String description() {
        return "Rolling Index Plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(RollAction.class);
            // logger.info("NOW " + action.getFeed("test"));
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
