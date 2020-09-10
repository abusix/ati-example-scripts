import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.StompClient;
import io.vertx.ext.stomp.StompClientConnection;
import io.vertx.ext.stomp.StompClientOptions;

public class ExampleFilesJSON {

    public static final String SERVER = "stream.abusix.net:61612";
    public static final boolean SSL = true;
    public static final String CREDENTIALS = "<user>:<pass>";
    public static final String TOPIC = "<topic>";

    public static final boolean WRITE_RAW_FILES = true;

    public static final SimpleDateFormat OUTPUT_DIR_FORMAT = new SimpleDateFormat("'filtered'/yyyy-MM-dd/HH/");

    public static final Map<String, List<String>> ALLOWLIST_FILTERS = new HashMap<>();
    public static final Map<String, List<String>> BLOCKLIST_FILTERS = new HashMap<>();

    static {
        /*
        Possible fields:
        - content_type
        - data_origin
        - detected_extension
        - filename
        - smtp_mail_from
        - source_ip
        - source_ip_country_iso
        - source_ip_rir
        - source_port
         */
        // ALLOWLIST_FILTERS.put("filename", Arrays.asList("attachment.txt"));
        // BLOCKLIST_FILTERS.put("detected_extension", Arrays.asList("exe"));
    }

    public static void onMessage(Frame frame) {
        JsonObject json = frame.getBody().toJsonObject();

        for (Map.Entry<String, List<String>> entry : ALLOWLIST_FILTERS.entrySet()) {
            String field = entry.getKey();
            List<String> allowed = entry.getValue();
            if (!allowed.contains(json.getString(field))) {
                return;
            }
        }

        for (Map.Entry<String, List<String>> entry : BLOCKLIST_FILTERS.entrySet()) {
            String field = entry.getKey();
            List<String> forbidden = entry.getValue();
            if (forbidden.contains(json.getString(field))) {
                return;
            }
        }

        Path target = Path.of(OUTPUT_DIR_FORMAT.format(new Date()));
        if (!target.toFile().exists()) {
            target.toFile().mkdirs();
        }

        try {
            if (WRITE_RAW_FILES) {
                Files.write(target.resolve(json.hashCode() + ".file"),
                        Base64.getDecoder().decode(json.getString("attachment_base64_encoded")));
            } else {
                Files.write(target.resolve(json.hashCode() + ".json"),
                        json.toString().getBytes());
            }
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
                conn.subscribe(TOPIC, headers, ExampleFilesJSON::onMessage);
            }
        });
        client.close();
    }

    public static void main(String[] args) {
        listen();
    }
}
