import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

public class ExampleFilesRaw {

    public static final String SERVER = "stream.abusix.net:61612";
    public static final boolean SSL = true;
    public static final String CREDENTIALS = "<user>:<pass>";
    public static final String TOPIC = "<topic>";

    public static final SimpleDateFormat OUTPUT_DIR_FORMAT = new SimpleDateFormat("'filtered'/yyyy-MM-dd/HH/");

    public static void onMessage(Frame frame) {
        Path target = Path.of(OUTPUT_DIR_FORMAT.format(new Date()));
        if (!target.toFile().exists()) {
            target.toFile().mkdirs();
        }

        try {
            Files.write(target.resolve(frame.getBody().hashCode() + ".file"),
                    Base64.getDecoder().decode(frame.getBodyAsByteArray()));
        } catch (IOException e) {
            System.err.println("Could not write file.");
            e.printStackTrace();
        }
    }

    public static void listen() {
        StompClientOptions options = new StompClientOptions();
        options.setHeartbeat(new JsonObject().put("x", 10000).put("y", 10000));
        options.setHost(SERVER.split(":")[0]);
        options.setPort(Integer.parseInt(SERVER.split(":")[1]));
        options.setSsl(SSL);
        options.setLogin(CREDENTIALS.split(":")[0]);
        options.setPasscode(CREDENTIALS.split(":")[1]);

        StompClient client = StompClient.create(Vertx.vertx(), options);
        client.connect(ar -> {
            if (ar.succeeded()) {
                StompClientConnection conn = ar.result();

                Map<String, String> headers = new HashMap<>();
                headers.put("id", "1234"); // something unique

                // To disable load-balancing (shared subscriptions):
                //headers.put("channel", CREDENTIALS.split(":")[0] + "something_unique");

                // Subscribe to topic
                conn.subscribe(TOPIC, headers, ExampleFilesRaw::onMessage);
            }
        });
        client.close();
    }

    public static void main(String[] args) {
        listen();
    }
}
