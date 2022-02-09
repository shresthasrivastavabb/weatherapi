package com.demo.weatherApp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public class MainVerticle extends AbstractVerticle {

  // 59f3f7f4b2dbbb7193795ccdb35f74b7
  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    Router router = Router.router(vertx);
    String url = "api.openweathermap.org";
    WebClient client = WebClient.create(vertx);

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()
        .setPort(3306)
        .setHost("Shrestha")
        .setDatabase("weather")
        .setUser("root")
        .setPassword("password");

    // Pool options
    PoolOptions poolOptions = new PoolOptions()
        .setMaxSize(10);

    // Create the pooled client
    MySQLPool client2 = MySQLPool.pool(vertx, connectOptions, poolOptions);

    // Get a connection from the pool
    // client2.getConnection().compose(conn -> {
    // System.out.println("Got a connection from the pool");

    // // All operations execute on the same connection
    // return conn
    // .query("SELECT * FROM weather_data")
    // .execute()
    // .compose(res -> conn
    // .query("SELECT * FROM weather_data")
    // .execute())
    // .onComplete(ar -> {
    // // Release the connection to the pool
    // conn.close();
    // });
    // }).onComplete(ar -> {
    // if (ar.succeeded()) {

    // System.out.println("Done");
    // } else {
    // System.out.println("Something went wrong " + ar.cause().getMessage());
    // }
    // });
    // client
    // .get(url,
    // "/data/2.5/weather?q=london&appid=59f3f7f4b2dbbb7193795ccdb35f74b7")
    // .as(BodyCodec.jsonObject())
    // .send()
    // .onSuccess(response -> {
    // JsonObject body = response.body();

    // System.out
    // .println("Received response with status code" + response.statusCode() + "
    // with body " + body);
    // })
    // .onFailure(err -> System.out.println("Something went wrong " +
    // err.getMessage()));
    // "INSERT INTO weather_data (name, description) VALUES (?,?);"

    router.get("/api/v1/place/:place").handler(ctx -> {

      String place = ctx.pathParam("place");
      String nextUrl = "/data/2.5/weather/?q=" + place + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();
            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");
            JsonObject coord = body2.getJsonObject("coord");
            JsonObject sys = body2.getJsonObject("sys");
            String lon = coord.getString("lon");
            String lat = coord.getString("lat");
            String country = sys.getString("country");
            String c_id = sys.getString("id");

            client2.getConnection();
            client2
                .preparedQuery(
                    "INSERT INTO weather_data (name, weather, id, lat, lon, country, c_id) VALUES (?,?,?,?,?,?,?);")
                .execute(Tuple.of(name, weather, id, lat, lon, country, c_id), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });

            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of Place");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));
    });

    router.get("/api/v1/zip/:country/:zipcode").handler(ctx -> {
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");

      String nextUrl = "/data/2.5/weather/?zip=" + zipcode + "," + countryCode
          + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();
            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");
            JsonObject coord = body2.getJsonObject("coord");
            JsonObject sys = body2.getJsonObject("sys");
            String lon = coord.getString("lon");
            String lat = coord.getString("lat");
            String country = sys.getString("country");
            String c_id = sys.getString("id");

            client2.getConnection();
            client2
                .preparedQuery(
                    "INSERT INTO weather_data (name, weather, id, lat, lon, country, c_id, c_zip) VALUES (?,?,?,?,?,?,?,?);")
                .execute(Tuple.of(name, weather, id, lat, lon, country, c_id, zipcode), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });

            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of zipcode");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));
    });

    router.get("/api/v1/latlon/:lat/:lon").handler(ctx -> {
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String nextUrl = "/data/2.5/weather/?lat=" + lat + "&lon=" + lon + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();

            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");
            JsonObject coord = body2.getJsonObject("coord");
            JsonObject sys = body2.getJsonObject("sys");
            String lo = coord.getString("lon");
            String la = coord.getString("lat");
            String country = sys.getString("country");
            String c_id = sys.getString("id");

            client2.getConnection();
            client2
                .preparedQuery(
                    "INSERT INTO weather_data (name, weather, id, lat, lon, country, c_id) VALUES (?,?,?,?,?,?,?);")
                .execute(Tuple.of(name, weather, id, la, lo, country, c_id), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });
            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of lat & long");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));

    });

    ////////////////// SEARCH APIS //////////////////////
    /////////////////////////////////////////////////////

    router.get("/api/v1/place/:place/search").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String place = ctx.pathParam("place");
      client2.getConnection();
      client2
          .preparedQuery(
              "SELECT * FROM  weather_data WHERE name=?;")
          .execute(Tuple.of(place), ar -> {
            if (ar.succeeded()) {
              String show = "";
              RowSet<Row> rows = ar.result();
              for (Row row : rows) {
                show += "weather of ";
                // System.out.print("weather of " );
                for (int i = 0; i < 7; i++) {
                  show += row.getString(i);
                  show += " ";
                  // System.out.print(row.getString(i) + " ");
                }
                System.out.print("\n");
              }
              System.out.println("Got weather data of " + place + " " + ar.result() + " " + show);
              ctx.response().end(show);

            } else {
              System.out.println("Failure: " + ar.cause().getMessage());
              client2.close();
            }
          });

    });

    router.get("/api/v1/zip/:country/:zipcode/search").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");
      client2.getConnection();
      client2
          .preparedQuery(
              "SELECT * FROM  weather_data WHERE country=? AND c_zip=?;")
          .execute(Tuple.of(countryCode, zipcode), ar -> {
            if (ar.succeeded()) {
              String show = "";
              RowSet<Row> rows = ar.result();
              for (Row row : rows) {
                show += "weather of ";
                for (int i = 0; i < 7; i++) {
                  show += row.getString(i);
                  show += " ";
                }
                System.out.print("\n");
              }
              System.out.println("Got weather data of " + zipcode + " " + ar.result() + " " + show);
              ctx.response().end(show);

            } else {
              System.out.println("Failure: " + ar.cause().getMessage());
              client2.close();
            }
          });

    });

    router.get("/api/v1/latlon/:lat/:lon/search").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      client2.getConnection();
      client2
          .preparedQuery(
              "SELECT * FROM  weather_data WHERE lat=? AND lon=?;")
          .execute(Tuple.of(lat, lon), ar -> {
            if (ar.succeeded()) {
              String show = "";
              RowSet<Row> rows = ar.result();
              for (Row row : rows) {
                show += "weather of ";
                // System.out.print("weather of " );
                for (int i = 0; i < 7; i++) {
                  show += row.getString(i);
                  show += " ";
                  // System.out.print(row.getString(i) + " ");
                }
                System.out.print("\n");
              }
              System.out.println("Got weather data of " + lat + " " + lon + " " + ar.result() + " " + show);
              ctx.response().end(show);

            } else {
              System.out.println("Failure: " + ar.cause().getMessage());
              client2.close();
            }
          });

    });

    //////////////////////////////// UPDATE APIS /////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    router.get("/api/v1/place/:place/update").handler(ctx -> {

      String place = ctx.pathParam("place");
      String nextUrl = "/data/2.5/weather/?q=" + place + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();
            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");

            client2.getConnection();
            client2
                .preparedQuery(
                    "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
                .execute(Tuple.of(weather, id, name), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });

            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of Place");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));
    });

    router.get("/api/v1/zip/:country/:zipcode/update").handler(ctx -> {
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");

      String nextUrl = "/data/2.5/weather/?zip=" + zipcode + "," + countryCode
          + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();
            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");

            client2.getConnection();
            client2
                .preparedQuery(
                    "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
                .execute(Tuple.of(weather, id, name), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });

            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of zipcode");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));
    });

    router.get("/api/v1/latlon/:lat/:lon/update").handler(ctx -> {
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String nextUrl = "/data/2.5/weather/?lat=" + lat + "&lon=" + lon + "&appid=59f3f7f4b2dbbb7193795ccdb35f74b7";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
          .onSuccess(response -> {
            JsonObject body2 = response.body();

            String name = body2.getString("name");
            String weather = body2.getString("weather");
            String id = body2.getString("id");
            client2.getConnection();
            client2
                .preparedQuery(
                    "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
                .execute(Tuple.of(weather, id, name), ar -> {
                  if (ar.succeeded()) {
                    System.out.println("Got Entry ");
                  } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                  }
                });
            String show = body2.toString();
            System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of lat & long");
            ctx.response().end(Json.encodePrettily(body2));

          })
          .onFailure(err -> System.out.println("Something Went Wrong"));
    });

    vertx.createHttpServer().requestHandler(router).listen(7080, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 7080");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
}
