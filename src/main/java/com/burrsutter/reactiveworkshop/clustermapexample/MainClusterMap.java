package com.burrsutter.reactiveworkshop.clustermapexample;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.ext.web.Router;

import static java.time.LocalDateTime.now;

/**
 * Created by burr on 5/26/17.
 */
public class MainClusterMap extends AbstractVerticle {
    private final String hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");

    public void start() {
        System.out.println("Start Clustered? " + vertx.isClustered());
        SharedData sd = vertx.sharedData();

        //AsyncMap<Object, Object> myClusteredMap;

        Router router = Router.router(vertx);

        router.get("/").handler(ctx -> {
            // System.out.println();
            ctx.response().end("Clustered? " + vertx.isClustered() + " index " + now());

        });

        // /api/addstuff?name=burr
        router.get("/api/addstuff").handler(ctx -> {
            String name = ctx.request().getParam("name");
            String value = "added on " + hostname + "(" + vertx.hashCode() + ")" + " at " + now();

            vertx.sharedData().getClusterWideMap("MyClusterMap1", arGetMap -> {
                AsyncMap<Object, Object> myClusteredMap = arGetMap.result();
                myClusteredMap.put(name, value, arPut -> {
                    if(arPut.succeeded()) {
                        System.out.println("Added " + name + " " + value);
                        ctx.response().end("Added " + name + " " + value);
                    } else {
                        ctx.response().end("Failed " + arPut.cause());
                    }
                });
            });
        });

        // /api/getstuff?name=burr
        router.get("/api/getstuff").handler(ctx -> {
            String name = ctx.request().getParam("name");
            System.out.println("Name: " + name);
            StringBuffer sb = new StringBuffer();

            vertx.sharedData().getClusterWideMap("MyClusterMap1", arGetMap -> {
                AsyncMap<Object, Object> myClusteredMap = arGetMap.result();
                System.out.println("Map acquired");
                myClusteredMap.size(arSize -> {
                    Integer size = arSize.result();
                    System.out.println("Size acquired " + size);
                    sb.append("Overall Size: " + arSize.result() + "\n");
                    if (size > 0) {
                        if (name != null && !name.isEmpty()) {
                            myClusteredMap.get(name, arGet -> {
                                if (arGet.succeeded()) {
                                    String value = (String) arGet.result();
                                    System.out.println("name's value acquired " + value);
                                    sb.append(name + " = " + value);
                                } else {
                                    sb.append("Failed: " + arGet.cause());
                                }
                                System.out.println("responding to http request now");
                                ctx.response().end(sb.toString());
                            }); // get
                        } // if name != null
                    } // if size > 0
                }); // size
            });  // sharedData().getClusterWideMap
        });

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }
}
