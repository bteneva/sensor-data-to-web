package bg.environment.monitoring.engine;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static bg.environment.monitoring.engine.PhpNodeEngine.getSofiaTime;

public class SensorCommunityEngine {
    private static final String DB_URL = "jdbc:sqlite:air_quality.db";
    private static final String API_URL = "https://data.sensor.community/airrohr/v1/sensor/";


    //private final HttpClient client = HttpClient.newHttpClient();
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    private final Runnable onNewDataCallback;



    // Оставяме САМО праховите сензори (SDS011) за следене на ТЕЦ-а
    private record ScSensor(String id, String mappedStationName) {}
    private static final List<ScSensor> SENSORS = List.of(
            new ScSensor("97673", "Мало/Големо село - ТЕЦ (97673)"),
            new ScSensor("43449", "Дупница - Град (43449)")
    );

    public SensorCommunityEngine(Runnable onNewDataCallback) {
        this.onNewDataCallback = onNewDataCallback;
    }

    public void checkAndProcess() {
        boolean anyNew = false;
        long nowUnix = Instant.now().getEpochSecond();

        System.out.println(getSofiaTime() + " >>> ПРЕДИ fetchCurrentWindAndTemp()");
        SensorDataToWeb.WeatherSnapshot currentWind =
                SensorDataToWeb.fetchCurrentWindAndTemp();
        System.out.println(getSofiaTime() + " <<< СЛЕД fetchCurrentWindAndTemp()");

        for (ScSensor sensor : SENSORS) {
            try {
                System.out.println(getSofiaTime() +"⏱️  [SensorCommunity] Проверка за пушек от ТЕЦ през сензор ID " + sensor.id() + sensor.mappedStationName()+"...");
                System.out.println(API_URL+sensor.id()+"/");


//                HttpRequest request = HttpRequest.newBuilder()
//                        .uri(URI.create(String.format(API_URL+sensor.id()+"/")))
//                        .timeout(Duration.ofSeconds(10))
//                        .GET()
//                        .build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(API_URL + sensor.id() + "/"))
                        .timeout(Duration.ofSeconds(20))
                        .header("User-Agent", "Mozilla/5.0")
                        .GET()
                        .build();
                // Добавяме User-Agent header, за да изглеждаме като истински браузър и да прескочим блокажа
//                HttpRequest request = HttpRequest.newBuilder()
//                        .uri(URI.create(String.format(API_URL, sensor.id())))
//                        .timeout(Duration.ofSeconds(10))
//                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
//                        .GET()
//                        .build();
                System.out.println(request);

                System.out.println(getSofiaTime()
                        + " >>> ПРЕДИ HTTP Sensor.Community " + sensor.id());


                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                System.out.println(getSofiaTime()
                        + " <<< СЛЕД HTTP Sensor.Community " + sensor.id()
                        + " status=" + response.statusCode());
                
                if (response.statusCode() == 200 && response.body() != null && !response.body().trim().equals("[]")) {
                    if (parseAndSaveJson(response.body(), sensor.mappedStationName(), currentWind, nowUnix)) {
                        anyNew = true;
                    }
                }
                System.out.println(response.body());
            } catch (Exception e) {
                System.err.println(getSofiaTime() +"❌ Грешка при извличане на данни от сензор " + sensor.id() + ": " + e.getMessage());
            }
        }

        if (anyNew && onNewDataCallback != null) {
            onNewDataCallback.run();
        }
    }

    private boolean parseAndSaveJson(String jsonText, String stationName, SensorDataToWeb.WeatherSnapshot currentWeather, long nowUnix) {
        // Оправихме кавичките и премахнахме грешното закоментиране (//) в SQL низа
        String sql =
                "INSERT INTO pollution(" +
                        "timestamp, pm1, pm25, pm10, temp, humidity, pressure, " +
                        "station_name, wind_dir, wind_speed, wind_degree) " +
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?) " +
                        "ON CONFLICT(timestamp, station_name) DO UPDATE SET " +
                        "pm25 = excluded.pm25, " +
                        "pm10 = excluded.pm10, " +
                        "temp = excluded.temp, " +
                        "humidity = excluded.humidity, " +
                        "wind_dir = COALESCE(pollution.wind_dir, excluded.wind_dir), " +
                        "wind_speed = COALESCE(pollution.wind_speed, excluded.wind_speed), " +
                        "wind_degree = COALESCE(pollution.wind_degree, excluded.wind_degree)";
        try {
            java.io.StringReader stringReader = new java.io.StringReader(jsonText);
            com.google.gson.stream.JsonReader jsonReader = new com.google.gson.stream.JsonReader(stringReader);
            jsonReader.setLenient(true);

            JsonArray array = JsonParser.parseReader(jsonReader).getAsJsonArray();
            if (array.isEmpty()) return false;

            // Вземаме последното измерване от устройството
            JsonObject newestRecord = array.get(0).getAsJsonObject();
            String rawTimestamp = newestRecord.get("timestamp").getAsString(); // напр. "2026-09-09 16:07:26"

// 2. Обявяваме го като UTC време и го превръщаме в софийско локално време (UTC + 3)
            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            java.time.LocalDateTime utcTime = java.time.LocalDateTime.parse(rawTimestamp, formatter);
            java.time.ZonedDateTime bgTime = utcTime.atZone(java.time.ZoneOffset.UTC).withZoneSameInstant(java.time.ZoneId.of("Europe/Sofia"));

// 3. Форматираме го обратно в чист текст за SQLite
            String formattedTime = bgTime.format(formatter);
            JsonArray valuesArray = newestRecord.getAsJsonArray("sensordatavalues");

            double pm25 = 0.0, pm10 = 0.0;

            // Извличаме P1 и P2 през Gson
            for (JsonElement elem : valuesArray) {
                JsonObject valObj = elem.getAsJsonObject();
                String type = valObj.get("value_type").getAsString();
                double value = valObj.get("value").getAsDouble();

                if (type.equals("P1")) {
                    pm10 = value;
                } else if (type.equals("P2")) {
                    pm25 = value;
                }
            }

            try (Connection conn = DriverManager.getConnection(DB_URL);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setString(1, formattedTime);
                pstmt.setDouble(2, 0.0);
                pstmt.setDouble(3, pm25);
                pstmt.setDouble(4, pm10);
                if (currentWeather != null && currentWeather.temp() != null) {
                    pstmt.setDouble(5, currentWeather.temp());
                } else {
                    pstmt.setNull(5, java.sql.Types.DOUBLE);
                }

                if (currentWeather != null && currentWeather.humidity() != null) {
                    pstmt.setDouble(6, currentWeather.humidity());
                } else {
                    pstmt.setNull(6, java.sql.Types.DOUBLE);
                }

// Sensor.Community не предоставя налягане в този код
                pstmt.setNull(7, java.sql.Types.DOUBLE);
                pstmt.setString(8, stationName);

                SensorDataToWeb.bindWind(pstmt, 9, currentWeather);


                int affectedRows = pstmt.executeUpdate();

                // Промяна: Приемаме всяко изпълнение без грешка (>= 0) за успешно,
                // тъй като при дублирани данни SQLite връща 0 засегнати реда, което НЕ е грешка!
                return affectedRows >= 0;
            }
        } catch (Exception e) {
            System.err.println(getSofiaTime() +"❌ Бъг при обработка на праховите данни за " + stationName + ": " + e.getMessage());
            e.printStackTrace(); // Добавяме пълната следа от грешката в конзолата, за да виждаме веднага ако има друг проблем
            return false;
        }
    }


}
