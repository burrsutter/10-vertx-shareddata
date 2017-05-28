package com.burrsutter.reactiveworkshop.localmapexample;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.shareddata.LocalMap;
import static java.time.LocalDateTime.now;
import io.vertx.ext.web.Router;

import java.util.Set;


public class MainLocalMap extends AbstractVerticle {
    private final String hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");

    @Override
    public void start() {
        System.out.println("Start");
        SharedData sd = vertx.sharedData();

        Router router = Router.router(vertx);

        router.get("/").handler(ctx -> {
            // System.out.println();
            ctx.response().end("Index " + now());
        });

        // /api/addstuff?name=burr
        router.get("/api/addstuff").handler(ctx -> {
            LocalMap<String,String> lm = sd.getLocalMap("MyMap1");

            String name = ctx.request().getParam("name");
            String value = "added on " + hostname + "(" + vertx.hashCode() + ")" + " at " + now();
            lm.put(name,value);
            ctx.response().end("Added " + name + " " + value);
        });

        // /api/getstuff
        router.get("/api/getstuff").handler(ctx -> {
            LocalMap<String,String> lm = sd.getLocalMap("MyMap1");

            StringBuilder sb = new StringBuilder();
            Set<String> keySet = lm.keySet();
            for (String key : keySet) {
                String value = lm.get(key);
                System.out.println("k: " + key + " v: " + value);
                sb.append(key + "=" + value);
                sb.append(" | ");
            } // for
            ctx.response().end(sb.toString());
        });

        // /api/clearstuff
        router.get("/api/clearstuff").handler(ctx -> {
            LocalMap<String,String> lm = sd.getLocalMap("MyMap1");
            lm.clear();
            ctx.response().end("wiped it out");
        });

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

}
