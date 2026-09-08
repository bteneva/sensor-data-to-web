package bg.environment.monitoring.engine;

//import bg.environment.monitoring.model.AirDataPoint;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SensorDataToWeb {
    private static final String DB_URL = "jdbc:sqlite:air_quality.db";
    private static final String DATA_URL = "https://meter.ac/gs/health/H82/data3.raw";
    //https://meter.ac/gs/nodes/N330/data.raw.php

    private static final String JSON_PATH = "live.json";
    private static final ZoneId BG_ZONE = ZoneId.of("Europe/Sofia");

    public static void main(String[] args) {
        initDatabase();

        // ТЕСТОВИ РЕДОВЕ: Проверяваме вятъра и обновяваме live.json ВЕДНАГА
        System.out.println("🚀 Тест на WeatherAPI в реално време...");
        WindSnapshot testWind = fetchCurrentWind();
        System.out.println("💨 Отчетена посока на вятъра: " + testWind.direction());
        System.out.println("💨 Скорост на вятъра: " +
                (testWind.speedKmh() != null ? testWind.speedKmh() + " km/h" : "Няма данни"));
        System.out.println("💨 Градус на вятъра: " +
                (testWind.degree() != null ? testWind.degree() + "°" : "Няма данни"));
        fillMissingWindDegree(testWind);
        exportDataToJson(); // Презаписва live.json на момента

        // Инициализираме пул с 2 нишки, за да работят двата енджина едновременно без да се засичат
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        // 1. Стартираме таймера за ПЪРВАТА (основната) станция (както си работеше досега)
        scheduler.scheduleAtFixedRate(SensorDataToWeb::processUpdate, 0, 5, TimeUnit.MINUTES);

        // 2. Създаваме обекта на НОВИЯ енджин и му подаваме метода за уеб експорт като задача
        PhpNodeEngine phpEngine = new PhpNodeEngine(SensorDataToWeb::exportDataToJson);

        // Пускаме втория енджин да проверява за нови 10-минутни данни на неговия си линк
        scheduler.scheduleAtFixedRate(phpEngine::checkAndProcess, 0, 5, TimeUnit.MINUTES);
    }


    private static void initDatabase() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS pollution (" +
                    "timestamp TEXT, pm1 REAL, pm25 REAL, pm10 REAL, " +
                    "temp REAL, humidity REAL, pressure REAL, station_name TEXT, wind_dir TEXT, " +
                    "wind_speed REAL, wind_degree REAL, " +
                    "PRIMARY KEY (timestamp, station_name));");
            migrateToCompositeKey(stmt);
            ensureWindColumns(stmt);
            System.out.println("✅ Базата данни е инициализирана с колона за вятър.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void migrateToCompositeKey(Statement stmt) throws Exception {
        try (ResultSet rs = stmt.executeQuery(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='pollution'")) {
            if (!rs.next()) {
                return;
            }
            String createSql = rs.getString("sql");
            if (createSql != null && createSql.contains("PRIMARY KEY (timestamp, station_name)")) {
                return;
            }
        }

        stmt.execute("CREATE TABLE IF NOT EXISTS pollution_new (" +
                "timestamp TEXT, pm1 REAL, pm25 REAL, pm10 REAL, " +
                "temp REAL, humidity REAL, pressure REAL, station_name TEXT, wind_dir TEXT, " +
                "PRIMARY KEY (timestamp, station_name));");
        stmt.execute("INSERT OR IGNORE INTO pollution_new " +
                "SELECT timestamp, pm1, pm25, pm10, temp, humidity, pressure, station_name, wind_dir FROM pollution");
        stmt.execute("DROP TABLE pollution");
        stmt.execute("ALTER TABLE pollution_new RENAME TO pollution");
        System.out.println("✅ Схемата е обновена: ключ (timestamp, station_name).");
    }

    private static void ensureWindColumns(Statement stmt) {
        for (String column : new String[]{"wind_speed REAL", "wind_degree REAL"}) {
            try {
                stmt.execute("ALTER TABLE pollution ADD COLUMN " + column);
                System.out.println("✅ Добавена е колона " + column + ".");
            } catch (Exception ignored) {
                // Колоната вече съществува
            }
        }
    }


    private static void processUpdate() {
        try {
            System.out.println("Изтегляне на пресни данни от Бобов дол (Големо село)...");
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(DATA_URL)).GET().build();
            HttpResponse<Stream<String>> response = client.send(request, HttpResponse.BodyHandlers.ofLines());

            if (response.statusCode() == 200) {
                try (Stream<String> lines = response.body()) {
                    List<String[]> allRows = lines
                            .map(line -> line.split(","))
                            .filter(words -> words.length >= 32)
                            // Филтрираме заглавния ред, ако има букви
                            .filter(words -> !words[31].contains("time") && !words[31].contains("timestamp"))
                            .toList();

                    if (!allRows.isEmpty()) {
                        saveAndExportBatch(allRows);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Грешка при обработка: " + e.getMessage());
        }
    }

    private static void saveAndExportBatch(List<String[]> rows) throws Exception {
        //String sql = "INSERT OR IGNORE INTO pollution(timestamp, pm1, pm25, pm10, temp, humidity, pressure, station_name) VALUES(?,?,?,?,?,?,?,?)";
        String sql = "INSERT OR IGNORE INTO pollution(timestamp, pm1, pm25, pm10, temp, humidity, pressure, station_name, wind_dir, wind_speed, wind_degree) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        int inserted = 0;
        WindSnapshot currentWind = fetchCurrentWind();

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (String[] words : rows) {
                    try {
                        long unixTime = Long.parseLong(words[31].trim());
                        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime), BG_ZONE);

                        double pm1 = Double.parseDouble(words[6].trim());
                        double pm25 = Double.parseDouble(words[7].trim());
                        double pm10 = Double.parseDouble(words[9].trim());
                        double humidity = Double.parseDouble(words[10].trim());
                        double pressure = Double.parseDouble(words[14].trim());

                        // СИГУРНО ПАРСВАНЕ: Премахваме знака "+" и интервалите от температурата
                        String rawTemp = words[15].trim().replace("+", "");
                        double temp = Double.parseDouble(rawTemp);

                        String formattedTime = time.toString().replace("T", " ");



                        pstmt.setString(1, formattedTime);
                        pstmt.setDouble(2, pm1);
                        pstmt.setDouble(3, pm25);
                        pstmt.setDouble(4, pm10);
                        pstmt.setDouble(5, temp);
                        pstmt.setDouble(6, humidity);
                        pstmt.setDouble(7, pressure);
                        pstmt.setString(8, "Големо село 1 (H82)");
                        bindWind(pstmt, 9, currentWind);

                        pstmt.addBatch();
                        inserted++;
                    } catch (Exception ignored) {}
                }
                pstmt.executeBatch();
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        fillMissingWindDegree(currentWind);
        System.out.println("Базата данни е обновена (Станция 1). Успешно обработени редове: " + inserted);
        exportDataToJson();
    }


    public static void exportDataToJson() {
        List<java.util.Map<String, Object>> alerts = new ArrayList<>();
        List<java.util.Map<String, Object>> recent = new ArrayList<>();

//        String queryAlerts = "SELECT timestamp, pm25, pm10, station_name FROM pollution " +
//                "WHERE (pm10 > 50.0 OR pm25 > 25.0) " +
//                "AND date(timestamp) >= date('now', '-2 day', 'localtime') " +
//                "ORDER BY timestamp DESC";
        String queryAlerts = "SELECT timestamp, pm25, pm10, station_name, wind_dir, wind_speed, wind_degree FROM pollution " +
                "WHERE (pm10 > 50.0 OR pm25 > 25.0) " +
                "AND date(timestamp) >= date('now', '-2 day', 'localtime') " +
                "ORDER BY timestamp DESC";


        String queryRecent = "SELECT timestamp, pm25, pm10, temp, humidity, pressure, station_name, wind_dir, wind_speed, wind_degree FROM pollution " +
                "ORDER BY timestamp DESC LIMIT 15";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery(queryAlerts)) {
                while (rs.next()) {
                    java.util.Map<String, Object> row = new java.util.HashMap<>();
                    row.put("timestamp", rs.getString("timestamp"));
                    row.put("pm25", rs.getDouble("pm25"));
                    row.put("pm10", rs.getDouble("pm10"));
                    row.put("station", rs.getString("station_name"));
                    row.put("wind_dir", rs.getString("wind_dir"));
                    row.put("wind_speed", nullableDouble(rs, "wind_speed"));
                    row.put("wind_degree", nullableDouble(rs, "wind_degree"));

                    alerts.add(row);
                }
            }

            try (ResultSet rs = stmt.executeQuery(queryRecent)) {
                while (rs.next()) {
                    java.util.Map<String, Object> row = new java.util.HashMap<>();
                    row.put("timestamp", rs.getString("timestamp"));
                    row.put("pm25", rs.getDouble("pm25"));
                    row.put("pm10", rs.getDouble("pm10"));
                    row.put("temp", rs.getDouble("temp"));
                    row.put("humidity", rs.getDouble("humidity"));
                    row.put("pressure", rs.getDouble("pressure"));
                    row.put("station", rs.getString("station_name"));
                    row.put("wind_dir", rs.getString("wind_dir"));
                    row.put("wind_speed", nullableDouble(rs, "wind_speed"));
                    row.put("wind_degree", nullableDouble(rs, "wind_degree"));
                    recent.add(row);
                }
            }

            java.util.Map<String, Object> response = new java.util.HashMap<>();
            response.put("last_updated",
                    LocalDateTime.now(BG_ZONE).toString().replace("T", " "));

            response.put("alerts", alerts);
            response.put("recent", recent);

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (FileWriter writer = new FileWriter(JSON_PATH)) {
                gson.toJson(response, writer);
            }
        } catch (Exception e) {
            System.err.println("Грешка при експорт на JSON: " + e.getMessage());
        }
    }
    public record WindSnapshot(String direction, Double speedKmh, Double degree) {}

    static void fillMissingWindDegree(WindSnapshot wind) {
        if (wind == null || wind.degree() == null) {
            return;
        }
        String sql = "UPDATE pollution SET wind_degree = ?, " +
                "wind_speed = COALESCE(wind_speed, ?) " +
                "WHERE wind_degree IS NULL AND rowid IN (" +
                "SELECT rowid FROM pollution ORDER BY timestamp DESC LIMIT 40)";
        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, wind.degree());
            if (wind.speedKmh() != null) {
                pstmt.setDouble(2, wind.speedKmh());
            } else {
                pstmt.setNull(2, java.sql.Types.REAL);
            }
            int updated = pstmt.executeUpdate();
            if (updated > 0) {
                System.out.println("✅ Попълнен wind_degree за " + updated + " скорошни записа.");
            }
        } catch (Exception e) {
            System.err.println("❌ Неуспешно попълване на wind_degree: " + e.getMessage());
        }
    }

    static void bindWind(PreparedStatement pstmt, int startIndex, WindSnapshot wind) throws Exception {
        pstmt.setString(startIndex, wind.direction());
        if (wind.speedKmh() != null) {
            pstmt.setDouble(startIndex + 1, wind.speedKmh());
        } else {
            pstmt.setNull(startIndex + 1, java.sql.Types.REAL);
        }
        if (wind.degree() != null) {
            pstmt.setDouble(startIndex + 2, wind.degree());
        } else {
            pstmt.setNull(startIndex + 2, java.sql.Types.REAL);
        }
    }

    private static Double nullableDouble(ResultSet rs, String column) throws Exception {
        return rs.getObject(column) == null ? null : rs.getDouble(column);
    }

    private static Double optionalJsonDouble(com.google.gson.JsonObject obj, String field) {
        if (!obj.has(field) || obj.get(field).isJsonNull()) {
            return null;
        }
        return obj.get(field).getAsDouble();
    }

    public static WindSnapshot fetchCurrentWind() {
        // Координати на с. Големо село през безплатно отворено WeatherAPI
        String apiKey = System.getenv("WEATHER_API_KEY");

        if (apiKey == null || apiKey.isBlank()) {
            System.err.println("❌ WEATHER_API_KEY не е зададен!");
            return new WindSnapshot("Няма данни", null, null);
        }

        String urlStr = "https://api.weatherapi.com/v1/current.json"
                + "?key=" + apiKey
                + "&q=42.2933,23.0419"
                + "&aqi=no";
        try {
            java.net.URL url = new java.net.URL(urlStr);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");

            if (conn.getResponseCode() == 200) {
                java.io.BufferedReader in = new java.io.BufferedReader(
                        new java.io.InputStreamReader(conn.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
                StringBuilder response = new StringBuilder();
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                // Парсваме с чист Gson без Regex
                com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(response.toString()).getAsJsonObject();
                if (json.has("current")) {
                    com.google.gson.JsonObject currentObj = json.getAsJsonObject("current");
                    Double speedKmh = optionalJsonDouble(currentObj, "wind_kph");
                    Double degree = optionalJsonDouble(currentObj, "wind_degree");
                    if (currentObj.has("wind_dir")) {
                        String dir = currentObj.get("wind_dir").getAsString().trim().toUpperCase();

                        // Български екологичен превод на посоките
                        String translated = switch (dir) {
                            case "N" -> "Северен (С)";
                            case "NNE" -> "Север -североизток (С-СИ)";
                            case "NE" -> "Североизточен (СИ)";
                            case "ENE" -> "Изток-Североизток (И-СИ)";
                            case "E" -> "Източен (И)";
                            case "ESE" -> "Изток-Югоизток (И-ЮИ)";
                            case "SE" -> "Югоизточен (ЮИ)";
                            case "SSE" -> "Юг-Югоизток (Ю-ЮИ)";
                            case "S" -> "Южен (Ю)";
                            case "SSW" -> "Юг-Югозапад (Ю-ЮЗ)";
                            case "SW" -> "Югозапад (ЮЗ)";
                            case "WSW" -> "Запад - Югозапад (З-ЮЗ)";
                            case "W" -> "Западен (З)";
                            case "WNW" -> "Запад-Северозапад (З-СЗ)";
                            case "NW" -> "Северозападен (СЗ)";
                            case "NNW" -> "Север -Северозапад (С-СЗ)";
                            default -> dir;
                        };
                        return new WindSnapshot(translated, speedKmh, degree);
                    }
                    if (speedKmh != null || degree != null) {
                        return new WindSnapshot("Пулсиращ / Променлив", speedKmh, degree);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Грешка при четене от новия WeatherAPI: " + e.getMessage());
        }
        return new WindSnapshot("Пулсиращ / Променлив", null, null);
    }






}
