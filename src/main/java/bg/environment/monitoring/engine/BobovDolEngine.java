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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static bg.environment.monitoring.engine.PhpNodeEngine.getSofiaTime;

public class BobovDolEngine {

    private static final String DB_URL = System.getenv("RAILWAY_ENVIRONMENT") != null
            ? "jdbc:sqlite:/app/data/air_quality.db"
            : "jdbc:sqlite:air_quality.db";

    private static final String API_URL =
            "http://88.80.111.23:50024/moment_data?update_type=last_minute";

    private static final String STATION_NAME =
            "ТЕЦ Бобов дол - Големо село";

    private static final ZoneId SOFIA_ZONE =
            ZoneId.of("Europe/Sofia");

    private static final DateTimeFormatter SOURCE_DATE_FORMAT =
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");

    private final HttpClient client;
    private final Runnable onNewDataCallback;

    public BobovDolEngine(Runnable onNewDataCallback) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        this.onNewDataCallback = onNewDataCallback;
    }

    /**
     * Извлича текущите данни от станцията и ги записва в SQLite.
     */
    public void checkAndProcess() {

        try {
            System.out.println(
                    getSofiaTime()
                            + "⏱️  [BobovDol] Проверка на станция "
                            + STATION_NAME + "..."
            );

            System.out.println(API_URL);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    client.send(
                            request,
                            HttpResponse.BodyHandlers.ofString()
                    );

            if (response.statusCode() != 200) {
                System.err.println(
                        getSofiaTime()
                                + "❌ [BobovDol] HTTP грешка: "
                                + response.statusCode()
                );
                return;
            }

            if (response.body() == null ||
                    response.body().trim().isEmpty()) {

                System.err.println(
                        getSofiaTime()
                                + "❌ [BobovDol] Празен отговор от станцията."
                );
                return;
            }

            boolean saved = parseAndSaveJson(response.body());

            if (saved) {

                System.out.println(
                        getSofiaTime()
                                + "✅ [BobovDol] Данните са записани успешно."
                );

                if (onNewDataCallback != null) {
                    onNewDataCallback.run();
                }
            }

        } catch (Exception e) {

            System.err.println(
                    getSofiaTime()
                            + "❌ [BobovDol] Грешка: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }

    /**
     * Парсира JSON от:
     *
     * /moment_data?update_type=last_minute
     *
     * и записва последния наличен запис.
     */
    private boolean parseAndSaveJson(String jsonText) {

        try {

            JsonObject root =
                    JsonParser.parseString(jsonText).getAsJsonObject();

            if (!root.has("min_values_data")) {

                System.err.println(
                        getSofiaTime()
                                + "❌ [BobovDol] Липсва min_values_data."
                );

                return false;
            }

            JsonArray data =
                    root.getAsJsonArray("min_values_data");

            if (data.size() == 0) {

                System.err.println(
                        getSofiaTime()
                                + "⚠️ [BobovDol] Няма налични измервания."
                );

                return false;
            }

            JsonObject measurement =
                    data.get(0).getAsJsonObject();

            /*
             * Извличане на стойностите.
             */

            String sourceTimestamp =
                    getString(measurement, "DateRef");

            Double pm25 =
                    getDouble(measurement, "FP2_5");

            Double pm10 =
                    getDouble(measurement, "FP10");

            Double so2 =
                    getDouble(measurement, "SO2");

            Double no2 =
                    getDouble(measurement, "NO2");

            Double temperature =
                    getDouble(measurement, "T");

            Double humidity =
                    getDouble(measurement, "H2O");

            Double pressure =
                    getDouble(measurement, "P");

            Double windSpeed =
                    getDouble(measurement, "WIND_SPEED");

            Double windDegree =
                    getDouble(measurement, "WIND_ANGLE");

            /*
             * Проверка на timestamp.
             */

            if (sourceTimestamp == null ||
                    sourceTimestamp.isBlank()) {

                System.err.println(
                        getSofiaTime()
                                + "❌ [BobovDol] Липсва DateRef."
                );

                return false;
            }

            /*
             * DateRef от станцията е във формат:
             *
             * dd.MM.yyyy HH:mm
             *
             * Например:
             * 11.09.2026 13:52
             *
             * Запазваме го като локално време за Europe/Sofia.
             */

            String timestamp =
                    normalizeTimestamp(sourceTimestamp);

            /*
             * Скоростта на вятъра от станцията е в m/s.
             *
             * В базата я пазим в km/h, за да бъде
             * съвместима с WeatherAPI данните.
             */

            Double windSpeedKmh = null;

            if (windSpeed != null) {
                windSpeedKmh = windSpeed * 3.6;
            }

            /*
             * wind_dir е текстовото поле.
             *
             * Не правим интерпретация на посоката.
             * Просто записваме числовия ъгъл от станцията
             * като текст, например "189.1°".
             */

            String windDirection = null;

            if (windDegree != null) {
                windDirection =
                        String.format(
                                Locale.US,
                                "%.1f°",
                                windDegree
                        );
            }

            /*
             * SQLite INSERT / UPSERT.
             *
             * Вече имаме 13 колони:
             *
             * 1  timestamp
             * 2  pm1
             * 3  pm25
             * 4  pm10
             * 5  temp
             * 6  humidity
             * 7  pressure
             * 8  station_name
             * 9  wind_dir
             * 10 wind_speed
             * 11 wind_degree
             * 12 so2
             * 13 no2
             *
             * Следователно VALUES трябва да има точно 13 ?.
             */

            String sql =
                    "INSERT INTO pollution(" +
                            "timestamp, " +
                            "pm1, " +
                            "pm25, " +
                            "pm10, " +
                            "temp, " +
                            "humidity, " +
                            "pressure, " +
                            "station_name, " +
                            "wind_dir, " +
                            "wind_speed, " +
                            "wind_degree, " +
                            "so2, " +
                            "no2" +
                            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) " +

                            "ON CONFLICT(timestamp, station_name) DO UPDATE SET " +

                            "pm25 = excluded.pm25, " +
                            "pm10 = excluded.pm10, " +
                            "temp = excluded.temp, " +
                            "humidity = excluded.humidity, " +
                            "pressure = excluded.pressure, " +
                            "wind_dir = excluded.wind_dir, " +
                            "wind_speed = excluded.wind_speed, " +
                            "wind_degree = excluded.wind_degree, " +
                            "so2 = excluded.so2, " +
                            "no2 = excluded.no2";

            try (
                    Connection conn =
                            DriverManager.getConnection(DB_URL);

                    PreparedStatement pstmt =
                            conn.prepareStatement(sql)
            ) {

                /*
                 * 1. timestamp
                 */

                pstmt.setString(1, timestamp);

                /*
                 * 2. pm1
                 *
                 * Тази станция не подава PM1.
                 */

                pstmt.setNull(
                        2,
                        java.sql.Types.DOUBLE
                );

                /*
                 * 3. PM2.5
                 */

                setNullableDouble(pstmt, 3, pm25);

                /*
                 * 4. PM10
                 */

                setNullableDouble(pstmt, 4, pm10);

                /*
                 * 5. Temperature
                 */

                setNullableDouble(
                        pstmt,
                        5,
                        temperature
                );

                /*
                 * 6. Humidity
                 */

                setNullableDouble(
                        pstmt,
                        6,
                        humidity
                );

                /*
                 * 7. Pressure
                 */

                setNullableDouble(
                        pstmt,
                        7,
                        pressure
                );

                /*
                 * 8. Station name
                 */

                pstmt.setString(
                        8,
                        STATION_NAME
                );

                /*
                 * 9. Wind direction as text
                 */

                if (windDirection != null) {

                    pstmt.setString(
                            9,
                            convertDegreesToWindDirection(windDegree)
                    );

                } else {

                    pstmt.setNull(
                            9,
                            java.sql.Types.VARCHAR
                    );
                }

                /*
                 * 10. Wind speed
                 *
                 * m/s -> km/h
                 */

                setNullableDouble(
                        pstmt,
                        10,
                        windSpeedKmh
                );

                /*
                 * 11. Wind degree
                 */

                setNullableDouble(
                        pstmt,
                        11,
                        windDegree
                );

                /*
                 * 12. SO2
                 */

                setNullableDouble(
                        pstmt,
                        12,
                        so2
                );

                /*
                 * 13. NO2
                 */

                setNullableDouble(
                        pstmt,
                        13,
                        no2
                );

                int affectedRows =
                        pstmt.executeUpdate();

                if (affectedRows > 0) {

                    System.out.println(
                            getSofiaTime()
                                    + "💾 [BobovDol] "
                                    + "Запис: "
                                    + timestamp
                                    + " | PM2.5="
                                    + pm25
                                    + " | PM10="
                                    + pm10
                                    + " | SO2="
                                    + so2
                                    + " | NO2="
                                    + no2
                                    + " | T="
                                    + temperature
                                    + " | H="
                                    + humidity
                                    + " | P="
                                    + pressure
                                    + " | Wind="
                                    + windSpeedKmh
                                    + " km/h"
                                    + " | "
                                    + convertDegreesToWindDirection(windDegree)
                                    + "°"
                    );

                    return true;
                }
            }

        } catch (Exception e) {

            System.err.println(
                    getSofiaTime()
                            + "❌ [BobovDol] Грешка при запис: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }

        return false;
    }

    /**
     * Нормализира DateRef към формата,
     * която използваме в SQLite.
     */
    private String normalizeTimestamp(
            String sourceTimestamp
    ) {

        try {

            LocalDateTime localDateTime =
                    LocalDateTime.parse(
                            sourceTimestamp,
                            SOURCE_DATE_FORMAT
                    );

            /*
             * DateRef вече е локално време на станцията.
             *
             * Записваме го в стандартен SQLite формат:
             *
             * yyyy-MM-dd HH:mm:ss
             */

            return localDateTime.format(
                    DateTimeFormatter.ofPattern(
                            "yyyy-MM-dd HH:mm:ss"
                    )
            );

        } catch (Exception e) {

            /*
             * Ако форматът се промени,
             * не прекъсваме целия запис.
             */

            System.err.println(
                    getSofiaTime()
                            + "⚠️ [BobovDol] "
                            + "Непознат формат на DateRef: "
                            + sourceTimestamp
            );

            return sourceTimestamp;
        }
    }

    /**
     * Безопасно извличане на String от JSON.
     */
    private String getString(
            JsonObject object,
            String key
    ) {

        try {

            if (!object.has(key) ||
                    object.get(key).isJsonNull()) {

                return null;
            }

            return object.get(key).getAsString();

        } catch (Exception e) {

            return null;
        }
    }

    /**
     * Безопасно извличане на Double от JSON.
     */
    private Double getDouble(
            JsonObject object,
            String key
    ) {

        try {

            if (!object.has(key) ||
                    object.get(key).isJsonNull()) {

                return null;
            }

            JsonElement element =
                    object.get(key);

            if (!element.isJsonPrimitive()) {
                return null;
            }

            return element.getAsDouble();

        } catch (Exception e) {

            return null;
        }
    }

    /**
     * Записва Double или SQL NULL.
     */
    private void setNullableDouble(
            PreparedStatement pstmt,
            int index,
            Double value
    ) throws SQLException {

        if (value != null) {

            pstmt.setDouble(
                    index,
                    value
            );

        } else {

            pstmt.setNull(
                    index,
                    java.sql.Types.DOUBLE
            );
        }
    }
    /**
     * Превръща градусите на вятъра (0-360) в текстова посока на български език.
     *
     * @param degrees градуси на вятъра
     * @return текстово описание на български
     */
    public static String convertDegreesToWindDirection(double degrees) {
        // Нормализираме градусите в диапазона [0, 360)
        degrees = (degrees % 360 + 360) % 360;

        // Списък с 16-те посоки на български език
        String[] directions = {
                "Север", "Север-Североизток", "Североизток", "Изток-Североизток",
                "Изток", "Изток-Югоизток", "Югоизток", "Юг-Югоизток",
                "Юг", "Юг-Югозапад", "Югозапад", "Запад-Югозапад",
                "Запад", "Запад-Северозапад", "Северозапад", "Север-Северозапад"
        };

        // Всеки сектор заема точно 22.5 градуса (360 / 16)
        // Добавяме 11.25 за правилно закръгляне към най-близкия сектор
        int index = (int) Math.floor((degrees + 11.25) / 22.5);

        // Ако индексът достигне 16 (т.е. над 348.75 градуса), това отново е Север (индекс 0)
        return directions[index % 16];
    }

}