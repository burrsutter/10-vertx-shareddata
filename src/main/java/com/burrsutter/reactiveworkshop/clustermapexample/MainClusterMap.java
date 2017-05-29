package com.burrsutter.reactiveworkshop.clustermapexample;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
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

        readNames().compose(strings -> {
            readClusterMap().compose(asyncMap -> {
                asyncMap.replace(NAMES_KEY, ImmutableSortedSet.naturalOrder().addAll(strings).add(name).build(), putEvent -> {
                    if (putEvent.succeeded()) {
                        ctx.response().end(String.format("Name '%s' added successfully.", name));
                    } else {
                        ctx.response().end(String.format("Error adding name: '%s'", putEvent.cause()));
                    }
                });
            }, Future.future().setHandler(handler -> {
                ctx.response().end(String.format("Error adding name: '%s'", handler.cause()));
            }));
        }, Future.failedFuture("Error reading names"));
    }

    private void getStuff(RoutingContext ctx) {
        readNames().compose(strings -> {
            ctx.response().end(Json.encodePrettily(strings));
        }, Future.failedFuture("Error reading names"));
    }

    private Future<AsyncMap<Object, Object>> readClusterMap() {
        Future<AsyncMap<Object, Object>> future = Future.future();
        vertx.sharedData().getClusterWideMap(CLUSTER_MAP_KEY, future.completer());
        return future;
    }

    private Future<Set<String>> readNames() {
        Future<Set<String>> future = Future.future();
        vertx.sharedData().getClusterWideMap(CLUSTER_MAP_KEY, event -> {
            AsyncMap<Object, Object> result = event.result();
            result.get(NAMES_KEY, namesEvent -> {
                if (namesEvent.succeeded()) {
                    Set<String> names = (Set<String>) namesEvent.result();
                    future.complete(names);
                } else {
                    future.fail(namesEvent.cause());
                }
            });
        });
        return future;
    }
}
