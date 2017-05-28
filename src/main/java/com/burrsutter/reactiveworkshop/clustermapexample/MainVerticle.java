package com.burrsutter.reactiveworkshop.clustermapexample;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

/**
 * Created by burr on 5/26/17.
 */
public class MainVerticle extends AbstractVerticle {
    public void start() {

        getVertx().deployVerticle("com.burrsutter.reactiveworkshop.clustermapexample.MainClusterMap", new DeploymentOptions().setHa(true));

    }
}
