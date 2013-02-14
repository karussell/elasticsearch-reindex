package com.pannous.es.reindex;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * @author Peter Karich
 */
public class ReIndexPlugin extends AbstractPlugin {
    
    protected final ESLogger logger = Loggers.getLogger(ReIndexPlugin.class);

    @Override public String name() {
        return "reindex";
    }

    @Override public String description() {
        return "ReIndex Plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(ReIndexAction.class);
            ((RestModule) module).addRestAction(ReIndexWithCreate.class);
            // logger.info("NOW " + action.getFeed("test"));
        }
    }
}
