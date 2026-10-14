package bg.environment.monitoring.engine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;


import static bg.environment.monitoring.engine.PhpNodeEngine.getSofiaTime;

public class SensorDataToWeb {

    //private static final String DB_URL = "jdbc:sqlite:air_quality.db";
    private static final String DB_URL = "jdbc:sqlite:/app/data/air_quality.db";

    private static final String DATA_URL =
            "https://meter.ac/gs/health/H82/data3.raw";
    private static final String JSON_PATH = "live.json";
    private static final ZoneId BG_ZONE = ZoneId.of("Europe/Sofia");

    public static void main(String[] args) {

        // =========================================================
        // СТАРТИРАНЕ НА ВГРАДЕНИЯ УЕБ СЪРВЪР
        // =========================================================
        try {
            // Railway подава системна променлива PORT. Ако липсва, ползваме 8080
            String portEnv = System.getenv("PORT");
            int port = (portEnv != null) ? Integer.parseInt(portEnv) : 8080;

            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

            // Създаваме ендпоинт /live.json
            server.createContext("/live.json", new LiveJsonHandler());
            server.createContext("/", new StaticFileHandler());

            server.setExecutor(null); // ползва стандартния изпълнител
            server.start();

            System.out.println(getSofiaTime() + " 🌐 Уеб сървърът стартира успешно на порт: " + port);
        } catch (Exception e) {
            System.err.println("❌ Неуспешно стартиране на уеб сървъра: " + e.getMessage());
        }


        initDatabase();

        // =========================================================
        // ТЕСТ НА WEATHER API ПРИ СТАРТ
        // =========================================================

        System.out.println(
                getSofiaTime() +
                        "🚀 Тест на WeatherAPI в реално време..."
        );

        WeatherSnapshot testWind = fetchCurrentWindAndTemp();

        System.out.println(
                getSofiaTime() +
                        " 💨 Посока на вятъра: " +
                        testWind.direction()
        );

        System.out.println(
                getSofiaTime() +
                        " 💨 Скорост на вятъра: " +
                        (testWind.speedKmh() != null
                                ? testWind.speedKmh() + " km/h"
                                : "Няма данни")
        );

        System.out.println(
                getSofiaTime() +
                        " 💨 Градус на вятъра: " +
                        (testWind.degree() != null
                                ? testWind.degree() + "°"
                                : "Няма данни")
        );

        System.out.println(
                getSofiaTime() +
                        " 🌡️ Температура WeatherAPI: " +
                        (testWind.temp() != null
                                ? testWind.temp() + " °C"
                                : "Няма данни")
        );

        System.out.println(
                getSofiaTime() +
                        " 💧 Влажност WeatherAPI: " +
                        (testWind.humidity() != null
                                ? testWind.humidity() + " %"
                                : "Няма данни")
        );

        // Попълване на липсващ вятър в базата
        fillMissingWindDegree(testWind);

        // Първоначален JSON
        exportDataToJson();

        // =========================================================
        // SCHEDULER
        // =========================================================

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(3);

        // 1. H82
        scheduler.scheduleAtFixedRate(
                SensorDataToWeb::processUpdate,
                0,
                5,
                TimeUnit.MINUTES
        );

        // 2. PHP станции
        PhpNodeEngine phpEngine =
                new PhpNodeEngine(SensorDataToWeb::exportDataToJson);

        scheduler.scheduleAtFixedRate(
                phpEngine::checkAndProcess,
                0,
                5,
                TimeUnit.MINUTES
        );

        // 3. Sensor.Community
        SensorCommunityEngine sensorCommunityEngine =
                new SensorCommunityEngine(
                        SensorDataToWeb::exportDataToJson
                );

        scheduler.scheduleAtFixedRate(
                sensorCommunityEngine::checkAndProcess,
                0,
                5,
                TimeUnit.MINUTES
        );

        BobovDolEngine bobovDolEngine =
                new BobovDolEngine(() -> {
                    try {
                        exportDataToJson();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
        scheduler.scheduleAtFixedRate(
                bobovDolEngine::checkAndProcess,
                0,
                1,
                TimeUnit.MINUTES
        );
    }

    // =============================================================
    // DATABASE
    // =============================================================

    private static void initDatabase() {

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS pollution (" +
                            "timestamp TEXT, " +
                            "pm1 REAL, " +
                            "pm25 REAL, " +
                            "pm10 REAL, " +
                            "temp REAL, " +
                            "humidity REAL, " +
                            "pressure REAL, " +
                            "station_name TEXT, " +
                            "wind_dir TEXT, " +
                            "wind_speed REAL, " +
                            "wind_degree REAL, " +
                            "PRIMARY KEY (timestamp, station_name));"
            );

            migrateToCompositeKey(stmt);
            ensureWindColumns(stmt);
            ensureAirPollutionColumns();

            System.out.println(
                    getSofiaTime() +
                            " ✅ Базата данни е инициализирана."
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void migrateToCompositeKey(Statement stmt)
            throws Exception {

        try (ResultSet rs = stmt.executeQuery(
                "SELECT sql FROM sqlite_master " +
                        "WHERE type='table' AND name='pollution'")) {

            if (!rs.next()) {
                return;
            }

            String createSql = rs.getString("sql");

            if (createSql != null &&
                    createSql.contains(
                            "PRIMARY KEY (timestamp, station_name)")) {
                return;
            }
        }

        stmt.execute(
                "CREATE TABLE IF NOT EXISTS pollution_new (" +
                        "timestamp TEXT, " +
                        "pm1 REAL, " +
                        "pm25 REAL, " +
                        "pm10 REAL, " +
                        "temp REAL, " +
                        "humidity REAL, " +
                        "pressure REAL, " +
                        "station_name TEXT, " +
                        "wind_dir TEXT, " +
                        "PRIMARY KEY (timestamp, station_name));"
        );

        stmt.execute(
                "INSERT OR IGNORE INTO pollution_new " +
                        "SELECT timestamp, pm1, pm25, pm10, temp, " +
                        "humidity, pressure, station_name, wind_dir " +
                        "FROM pollution"
        );

        stmt.execute("DROP TABLE pollution");

        stmt.execute(
                "ALTER TABLE pollution_new RENAME TO pollution"
        );

        System.out.println(
                getSofiaTime() +
                        " ✅ Схемата е обновена: ключ (timestamp, station_name)."
        );
    }

    private static void ensureWindColumns(Statement stmt) {

        for (String column :
                new String[]{"wind_speed REAL", "wind_degree REAL"}) {

            try {

                stmt.execute(
                        "ALTER TABLE pollution ADD COLUMN " + column
                );

                System.out.println(
                        getSofiaTime() +
                                " ✅ Добавена е колона " + column + "."
                );

            } catch (Exception ignored) {
                // Колоната вече съществува
            }
        }
    }
    private static void ensureAirPollutionColumns() {
        String[] columns = {"so2", "no2"};

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            for (String column : columns) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE pollution ADD COLUMN " + column + " REAL");
                    System.out.println("✅ Добавена колона: " + column);
                } catch (SQLException e) {
                    if (e.getMessage() != null &&
                            e.getMessage().toLowerCase().contains("duplicate column name")) {
                        // Колоната вече съществува — всичко е наред.
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Грешка при добавяне на SO2/NO2 колоните: " + e.getMessage());
        }
    }

    // =============================================================
    // H82 DATA
    // =============================================================

    private static void processUpdate() {

        try {

            System.out.println(
                    getSofiaTime() +
                            " ⏱️ [SensorDataToWeb] Изтегляне на H82..."
            );

            HttpClient client = HttpClient.newHttpClient();

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(DATA_URL))
                            .GET()
                            .build();

            HttpResponse<Stream<String>> response =
                    client.send(
                            request,
                            HttpResponse.BodyHandlers.ofLines()
                    );

            if (response.statusCode() == 200) {

                try (Stream<String> lines = response.body()) {

                    List<String[]> allRows =
                            lines
                                    .map(line -> line.split(","))
                                    .filter(words -> words.length >= 32)
                                    .filter(words ->
                                            !words[31].contains("time") &&
                                                    !words[31].contains("timestamp"))
                                    .toList();

                    if (!allRows.isEmpty()) {
                        saveAndExportBatch(allRows);
                    }
                }

            } else {

                System.err.println(
                        getSofiaTime() +
                                " ❌ H82 HTTP статус: " +
                                response.statusCode()
                );
            }

        } catch (Exception e) {

            System.err.println(
                    getSofiaTime() +
                            " ❌ Грешка при обработка на H82: " +
                            e.getMessage()
            );
        }
    }

    private static void saveAndExportBatch(List<String[]> rows)
            throws Exception {

        String sql =
                "INSERT OR IGNORE INTO pollution(" +
                        "timestamp, pm1, pm25, pm10, temp, humidity, " +
                        "pressure, station_name, wind_dir, " +
                        "wind_speed, wind_degree, " + "so2, no2" +
                        ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int inserted = 0;

        WeatherSnapshot currentWeather =
                fetchCurrentWindAndTemp();

        try (Connection conn =
                     DriverManager.getConnection(DB_URL)) {

            conn.setAutoCommit(false);

            try (PreparedStatement pstmt =
                         conn.prepareStatement(sql)) {

                for (String[] words : rows) {

                    try {

                        long unixTime =
                                Long.parseLong(words[31].trim());

                        LocalDateTime time =
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochSecond(unixTime),
                                        BG_ZONE
                                );

                        String formattedTime =
                                time.toString().replace("T", " ");

                        double pm1 =
                                Double.parseDouble(words[6].trim());

                        double pm25 =
                                Double.parseDouble(words[7].trim());

                        double pm10 =
                                Double.parseDouble(words[9].trim());

                        double humidity =
                                Double.parseDouble(words[10].trim());

                        double pressure =
                                Double.parseDouble(words[14].trim());

                        String rawTemp =
                                words[15]
                                        .trim()
                                        .replace("+", "");

                        double temp =
                                Double.parseDouble(rawTemp);

                        double so2 = Double.parseDouble(words[23].trim());
                        double no2 = Double.parseDouble(words[29].trim());

                        pstmt.setString(1, formattedTime);
                        pstmt.setDouble(2, pm1);
                        pstmt.setDouble(3, pm25);
                        pstmt.setDouble(4, pm10);
                        pstmt.setDouble(5, temp);
                        pstmt.setDouble(6, humidity);
                        pstmt.setDouble(7, pressure);


                        pstmt.setString(
                                8,
                                "Големо село 1 (H82)"
                        );

                        bindWind(
                                pstmt,
                                9,
                                currentWeather
                        );
                        pstmt.setDouble(12, so2);
                        pstmt.setDouble(13, no2);

                        pstmt.addBatch();
                        inserted++;

                    } catch (Exception e) {

                        System.err.println(
                                "⚠️ Пропуснат ред от H82: " +
                                        e.getMessage()
                        );
                    }
                }

                pstmt.executeBatch();
                conn.commit();

                System.out.println(
                        getSofiaTime() +
                                " 📊 [SensorDataToWeb] " +
                                "Успешно обработени " +
                                inserted +
                                " реда за H82."
                );

            } catch (Exception e) {

                conn.rollback();

                System.err.println(
                        "❌ Грешка при транзакцията на H82: " +
                                e.getMessage()
                );
            }
        }

        exportDataToJson();
    }

    // =============================================================
    // WIND
    // =============================================================

    public static void bindWind(
            PreparedStatement pstmt,
            int startIndex,
            WeatherSnapshot wind
    ) throws Exception {

        if (wind != null) {

            if (wind.direction() != null) {
                pstmt.setString(
                        startIndex,
                        wind.direction()
                );
            } else {
                pstmt.setNull(
                        startIndex,
                        java.sql.Types.VARCHAR
                );
            }

            if (wind.speedKmh() != null) {
                pstmt.setDouble(
                        startIndex + 1,
                        wind.speedKmh()
                );
            } else {
                pstmt.setNull(
                        startIndex + 1,
                        java.sql.Types.DOUBLE
                );
            }

            if (wind.degree() != null) {
                pstmt.setDouble(
                        startIndex + 2,
                        wind.degree()
                );
            } else {
                pstmt.setNull(
                        startIndex + 2,
                        java.sql.Types.DOUBLE
                );
            }

        } else {

            pstmt.setNull(
                    startIndex,
                    java.sql.Types.VARCHAR
            );

            pstmt.setNull(
                    startIndex + 1,
                    java.sql.Types.DOUBLE
            );

            pstmt.setNull(
                    startIndex + 2,
                    java.sql.Types.DOUBLE
            );
        }
    }

    // =============================================================
    // WEATHER API
    // =============================================================

    public static WeatherSnapshot fetchCurrentWindAndTemp() {

        String apiKey =
                System.getenv("WEATHER_API_KEY");

        if (apiKey == null || apiKey.isBlank()) {

            System.err.println(
                    "❌ WEATHER_API_KEY не е зададен!"
            );

            return new WeatherSnapshot(
                    "Няма данни",
                    null,
                    null,
                    null,
                    null
            );
        }

        String urlStr =
                "https://api.weatherapi.com/v1/current.json" +
                        "?key=" + apiKey +
                        "&q=42.2933,23.0419" +
                        "&aqi=no";

        try {

            java.net.URL url =
                    new java.net.URL(urlStr);

            java.net.HttpURLConnection conn =
                    (java.net.HttpURLConnection)
                            url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty(
                    "User-Agent",
                    "Mozilla/5.0"
            );

            int responseCode =
                    conn.getResponseCode();

            if (responseCode != 200) {

                System.err.println(
                        "❌ WeatherAPI HTTP статус: " +
                                responseCode
                );

                return new WeatherSnapshot(
                        "Няма данни",
                        null,
                        null,
                        null,
                        null
                );
            }

            StringBuilder response =
                    new StringBuilder();

            try (java.io.BufferedReader in =
                         new java.io.BufferedReader(
                                 new java.io.InputStreamReader(
                                         conn.getInputStream(),
                                         java.nio.charset.StandardCharsets.UTF_8
                                 )
                         )) {

                String inputLine;

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
            }

            JsonObject json =
                    JsonParser.parseString(
                            response.toString()
                    ).getAsJsonObject();

            if (!json.has("current")) {

                return new WeatherSnapshot(
                        "Няма данни",
                        null,
                        null,
                        null,
                        null
                );
            }

            JsonObject currentObj =
                    json.getAsJsonObject("current");

            Double speedKmh =
                    optionalJsonDouble(
                            currentObj,
                            "wind_kph"
                    );

            Double degree =
                    optionalJsonDouble(
                            currentObj,
                            "wind_degree"
                    );

            Double temp =
                    optionalJsonDouble(
                            currentObj,
                            "temp_c"
                    );

            Double humidity =
                    optionalJsonDouble(
                            currentObj,
                            "humidity"
                    );

            String translated =
                    "Пулсиращ / Променлив";

            if (currentObj.has("wind_dir") &&
                    !currentObj.get("wind_dir").isJsonNull()) {

                String dir =
                        currentObj
                                .get("wind_dir")
                                .getAsString()
                                .trim()
                                .toUpperCase();

                translated =
                        translateWindDirection(dir);
            }

            System.out.println(
                    getSofiaTime() +
                            " 🌤️ WeatherAPI: " +
                            temp +
                            " °C / " +
                            humidity +
                            " % / " +
                            translated +
                            " / " +
                            speedKmh +
                            " km/h / " +
                            degree +
                            "°"
            );

            return new WeatherSnapshot(
                    translated,
                    speedKmh,
                    degree,
                    temp,
                    humidity
            );

        } catch (Exception e) {

            System.err.println(
                    "❌ Грешка при четене от WeatherAPI: " +
                            e.getMessage()
            );

            return new WeatherSnapshot(
                    "Пулсиращ / Променлив",
                    null,
                    null,
                    null,
                    null
            );
        }
    }

    public static String translateWindDirection(String dir) {

        return switch (dir) {

            case "N" ->
                    "Северен (С)";

            case "NNE" ->
                    "Север-североизток (С-СИ)";

            case "NE" ->
                    "Североизточен (СИ)";

            case "ENE" ->
                    "Изток-североизток (И-СИ)";

            case "E" ->
                    "Източен (И)";

            case "ESE" ->
                    "Изток-югоизток (И-ЮИ)";

            case "SE" ->
                    "Югоизточен (ЮИ)";

            case "SSE" ->
                    "Юг-югоизток (Ю-ЮИ)";

            case "S" ->
                    "Южен (Ю)";

            case "SSW" ->
                    "Юг-югозапад (Ю-ЮЗ)";

            case "SW" ->
                    "Югозападен (ЮЗ)";

            case "WSW" ->
                    "Запад-югозапад (З-ЮЗ)";

            case "W" ->
                    "Западен (З)";

            case "WNW" ->
                    "Запад-северозапад (З-СЗ)";

            case "NW" ->
                    "Северозападен (СЗ)";

            case "NNW" ->
                    "Север-северозапад (С-СЗ)";

            default ->
                    dir;
        };
    }

    private static Double optionalJsonDouble(
            JsonObject obj,
            String field
    ) {

        if (!obj.has(field) ||
                obj.get(field).isJsonNull()) {

            return null;
        }

        return obj.get(field).getAsDouble();
    }

    // =============================================================
    // EXPORT TO JSON
    // =============================================================


    public static void exportDataToJson() {

        List<Map<String, Object>> alerts = new ArrayList<>();
        List<Map<String, Object>> recent = new ArrayList<>();

        final String BOBOV_DOL = "ТЕЦ Бобов дол - Големо село";

        /*
         * ALERTS:
         * - Другите станции: съществуващите минутни/периодични записи.
         * - Бобов дол: средна за завършен 5-минутен интервал.
         * - Прагът се прилага върху средната стойност.
         */
        String queryAlerts =
                "WITH bobovdol_avg AS ( " +
                        " SELECT " +
                        "   strftime('%Y-%m-%d %H:', timestamp) || " +
                        "   printf('%02d', " +
                        "       (CAST(strftime('%M', timestamp) AS INTEGER) / 5) * 5" +
                        "   ) || ':00' AS timestamp, " +
                        "   station_name, " +
                        "   AVG(pm25) AS pm25, " +
                        "   AVG(pm10) AS pm10, " +
                        "   AVG(wind_speed) AS wind_speed, " +
                        "   AVG(wind_degree) AS wind_degree, " +
                        "   COUNT(*) AS samples " +
                        " FROM pollution " +
                        " WHERE station_name = '" + BOBOV_DOL + "' " +
                        "   AND timestamp >= datetime('now', '-48 hours', 'localtime') " +
                        " GROUP BY " +
                        "   strftime('%Y-%m-%d %H:', timestamp), " +
                        "   (CAST(strftime('%M', timestamp) AS INTEGER) / 5) " +
                        " HAVING COUNT(*) = 5 " +
                        ") " +

                        "SELECT timestamp, pm25, pm10, station_name, " +
                        "       wind_dir, wind_speed, wind_degree " +
                        "FROM pollution " +
                        "WHERE station_name <> '" + BOBOV_DOL + "' " +
                        "  AND timestamp >= datetime('now', '-48 hours', 'localtime') " +
                        "  AND (pm10 > 50.0 OR pm25 > 25.0) " +

                        "UNION ALL " +

                        "SELECT timestamp, pm25, pm10, station_name, " +
                        "       NULL AS wind_dir, wind_speed, wind_degree " +
                        "FROM bobovdol_avg " +
                        "WHERE pm10 > 50.0 OR pm25 > 25.0 " +

                        "ORDER BY timestamp DESC";

        /*
         * RECENT:
         * Другите станции се показват както досега.
         * Бобов дол се показва със средни стойности за 5 минути.
         */
        String queryRecent =
                "WITH bobovdol_avg AS ( " +
                        " SELECT " +
                        "   strftime('%Y-%m-%d %H:', timestamp) || " +
                        "   printf('%02d', " +
                        "       (CAST(strftime('%M', timestamp) AS INTEGER) / 5) * 5" +
                        "   ) || ':00' AS timestamp, " +
                        "   station_name, " +
                        "   AVG(pm25) AS pm25, " +
                        "   AVG(pm10) AS pm10, " +
                        "   AVG(temp) AS temp, " +
                        "   AVG(humidity) AS humidity, " +
                        "   AVG(pressure) AS pressure, " +
                        "   AVG(wind_speed) AS wind_speed, " +
                        "   AVG(wind_degree) AS wind_degree, " +
                        "   COUNT(*) AS samples " +
                        " FROM pollution " +
                        " WHERE station_name = '" + BOBOV_DOL + "' " +
                        " GROUP BY " +
                        "   strftime('%Y-%m-%d %H:', timestamp), " +
                        "   (CAST(strftime('%M', timestamp) AS INTEGER) / 5) " +
                        " HAVING COUNT(*) = 5 " +
                        ") " +

                        "SELECT timestamp, pm25, pm10, temp, humidity, pressure, " +
                        "       station_name, wind_dir, wind_speed, wind_degree " +
                        "FROM pollution " +
                        "WHERE station_name <> '" + BOBOV_DOL + "' " +

                        "UNION ALL " +

                        "SELECT timestamp, pm25, pm10, temp, humidity, pressure, " +
                        "       station_name, NULL AS wind_dir, wind_speed, wind_degree " +
                        "FROM bobovdol_avg " +

                        "ORDER BY timestamp DESC " +
                        "LIMIT 15";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            // =====================================================
            // ALERTS
            // =====================================================
            try (ResultSet rs = stmt.executeQuery(queryAlerts)) {

                while (rs.next()) {

                    Map<String, Object> row = new HashMap<>();

                    row.put("timestamp", rs.getString("timestamp"));
                    row.put("pm25", nullableDouble(rs, "pm25"));
                    row.put("pm10", nullableDouble(rs, "pm10"));
                    row.put("station", rs.getString("station_name"));
                    row.put("wind_dir", rs.getString("wind_dir"));
                    row.put("wind_speed", nullableDouble(rs, "wind_speed"));
                    row.put("wind_degree", nullableDouble(rs, "wind_degree"));

                    alerts.add(row);
                }
            }

            // =====================================================
            // RECENT
            // =====================================================
            try (ResultSet rs = stmt.executeQuery(queryRecent)) {

                while (rs.next()) {

                    Map<String, Object> row = new HashMap<>();

                    row.put("timestamp", rs.getString("timestamp"));
                    row.put("pm25", nullableDouble(rs, "pm25"));
                    row.put("pm10", nullableDouble(rs, "pm10"));
                    row.put("temp", nullableDouble(rs, "temp"));
                    row.put("humidity", nullableDouble(rs, "humidity"));
                    row.put("pressure", nullableDouble(rs, "pressure"));
                    row.put("station", rs.getString("station_name"));
                    row.put("wind_dir", rs.getString("wind_dir"));
                    row.put("wind_speed", nullableDouble(rs, "wind_speed"));
                    row.put("wind_degree", nullableDouble(rs, "wind_degree"));

                    recent.add(row);
                }
            }

            // =====================================================
            // ROOT JSON
            // =====================================================
            Map<String, Object> response = new HashMap<>();

            response.put(
                    "last_updated",
                    LocalDateTime.now(BG_ZONE)
                            .toString()
                            .replace("T", " ")
            );

            response.put("alerts", alerts);
            response.put("recent", recent);

            Gson gson = new GsonBuilder()
                    .setPrettyPrinting()
                    .create();

            try (FileWriter writer = new FileWriter(JSON_PATH)) {
                gson.toJson(response, writer);
            }

            System.out.println(
                    getSofiaTime()
                            + " 🌐 JSON обновен: "
                            + alerts.size()
                            + " аларми, "
                            + recent.size()
                            + " последни измервания."
            );


        } catch (Exception e) {

            System.err.println(
                    getSofiaTime()
                            + " ❌ Грешка при експорт на JSON: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }

    // =============================================================
    // NULLABLE DB VALUE
    // =============================================================

    private static Double nullableDouble(
            ResultSet rs,
            String column
    ) throws Exception {

        return rs.getObject(column) == null
                ? null
                : rs.getDouble(column);
    }

    // =============================================================
    // WIND DEGREE
    // =============================================================

    private static void fillMissingWindDegree(
            WeatherSnapshot testWind
    ) {

        // Засега не променяме съществуващата логика.
        // Тук по-късно може да се попълват липсващи
        // wind_degree стойности.

    }

    // =============================================================
    // WEATHER SNAPSHOT
    // =============================================================

    public record WeatherSnapshot(
            String direction,
            Double speedKmh,
            Double degree,
            Double temp,
            Double humidity
    ) {
    }
    // =============================================================
    // HTTP HANDLER ЗА LIVE.JSON
    // =============================================================
    static class LiveJsonHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Добавяме CORS хедъри, за да може фронтендът да чете файла от друг домейн
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                try {
                    // Прочитаме съдържанието на live.json
                    byte[] response = Files.readAllBytes(Paths.get(JSON_PATH));
                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (IOException e) {
                    // Ако файлът все още не е генериран при първото пускане
                    String errorMsg = "{\"error\": \"Данните все още се генерират. Моля изчакайте малко.\"}";
                    byte[] response = errorMsg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(404, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                }
            } else {
                // Разрешаваме само GET заявки
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
    // =============================================================
    // HTTP HANDLER ЗА СТАТИЧНИ ФАЙЛОВЕ (index.html, script.js и др.)
    // =============================================================
    static class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();

            // Ако потребителят отвори само главния адрес (https://.../), му даваме index.html
            if (path.equals("/")) {
                path = "/index.html";
            }

            // Пътят до файла в контейнера (приемаме, че са в папка 'web' или в главната)
            // За този пример ще четем от главната папка на приложението:
            java.io.File file = new java.io.File("." + path);

            if (file.exists() && !file.isDirectory()) {
                // Определяне на Content-Type според разширението на файла
                String contentType = "text/plain";
                if (path.endsWith(".html")) contentType = "text/html; charset=UTF-8";
                else if (path.endsWith(".js")) contentType = "application/javascript; charset=UTF-8";
                else if (path.endsWith(".css")) contentType = "text/css; charset=UTF-8";

                exchange.getResponseHeaders().set("Content-Type", contentType);
                byte[] response = Files.readAllBytes(file.toPath());
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            } else {
                // Ако файлът не съществува, връщаме грешка 404
                String notFound = "404 Not Found";
                exchange.sendResponseHeaders(404, notFound.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(notFound.getBytes());
                }
            }
        }
    }


}