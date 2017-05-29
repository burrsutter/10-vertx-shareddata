package com.burrsutter.reactiveworkshop.clustermapexample;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.util.Set;

import static java.time.LocalDateTime.now;

public class MainClusterMap extends AbstractVerticle {

    private static final String CLUSTER_MAP_KEY = "cluster_map_";

    private static final String NAMES_KEY = "names_";

    public static void main(String[] args) {
        Config config = new XmlConfigBuilder(MainClusterMap.class.getResourceAsStream("/cluster.xml")).build();
        VertxOptions vertxOptions = new VertxOptions()
                .setHAEnabled(true)
                .setClustered(true)
                .setClusterManager(new HazelcastClusterManager(config));
        Vertx.clusteredVertx(vertxOptions, clusterHandler -> {
            if (clusterHandler.succeeded()) {
                clusterHandler.result().deployVerticle(new MainClusterMap(), handler -> {
                    System.out.println(handler
                            .map("Deployment succeeded")
                            .otherwise(t -> String.format("Deployment failed with %s", t.getMessage()))
                            .result());
                });
            } else {
                System.out.println(String.format("Failed to start cluster with cause %s", clusterHandler.cause()));
            }
        });
    }

    @Override
    public void start() {
        vertx.sharedData().getClusterWideMap(CLUSTER_MAP_KEY, mapEvent -> {
            AsyncMap<Object, Object> result = mapEvent.result();
            result.put(NAMES_KEY, ImmutableSet.of(), event -> {
            });
        });

        Router router = Router.router(vertx);
        router.get("/").handler(ctx -> ctx.response().end(String.format("Clustered? %s at %s", vertx.isClustered() ? "Yes" : "No", now())));
        router.get("/api/addstuff").handler(this::addStuff);
        router.get("/api/getstuff").handler(this::getStuff);

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void addStuff(RoutingContext ctx) {
        String name = ctx.request().getParam("name");
        if (name == null) {
            ctx.response().end("You must provide a name parameter.");
            return;
        }

        vertx.sharedData().getClusterWideMap(CLUSTER_MAP_KEY, mapEvent -> {
            AsyncMap<Object, Object> result = mapEvent.result();
            result.get(NAMES_KEY, event -> {
                if (event.succeeded()) {
                    Set<String> names = (Set<String>) event.result();
                    result.replace(NAMES_KEY, ImmutableSortedSet.naturalOrder().addAll(names).add(name).build(), putEvent -> {
                        if (putEvent.succeeded()) {
                            ctx.response().end(String.format("Name '%s' added successfully.", name));
                        } else {
                            ctx.response().end(String.format("Error adding name: '%s'", putEvent.cause()));
                        }
                    });
                }
            });
        });
    }

    private void getStuff(RoutingContext ctx) {
        vertx.sharedData().getClusterWideMap(CLUSTER_MAP_KEY, mapEvent -> {
            AsyncMap<Object, Object> result = mapEvent.result();
            result.get(NAMES_KEY, event -> {
                if (event.succeeded()) {
                    Set<String> names = (Set<String>) event.result();
                    ctx.response().end(Json.encodePrettily(names));
                } else {
                    ctx.response().end(String.format("Error getting names: '%s'", event.cause()));
                }
            });
        });
    }
}
