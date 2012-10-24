package com.pannous.es.rollplugin;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * @author Peter Karich
 */
public class RollingIndexPlugin extends AbstractPlugin {

    private boolean startedThread = false;
    
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
    }
}
