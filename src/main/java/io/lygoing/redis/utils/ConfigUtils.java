package io.lygoing.redis.utils;

import io.vertx.core.json.JsonObject;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

/**
 * @author Mr.Luo
 */
public class ConfigUtils {

    @SuppressWarnings("unchecked")
    public static <T> T getConfigValue(JsonObject config, String item) {
        if (config == null) {
            return null;
        }
        if (item == null || item.isBlank()) {
            return null;
        }
        String[] items = item.split("\\.");
        Object value = null;
        for (int i = 0; i < items.length; i++) {
            String key = items[i];
            if (i == 0) {
                value = config.getValue(key);
            } else {
                if (value == null) {
                    return null;
                }
                value = ((JsonObject) value).getValue(key);
            }
        }
        return (T) value;
    }

    public static JsonObject readConfigInClasspath(String... configFileList) throws Exception {
        ClassLoader cl = ConfigUtils.class.getClassLoader();
        for (int i = 0; i < configFileList.length; i++) {
            try (InputStream in = cl.getResourceAsStream(configFileList[i])) {
                if (in != null) {
                    return new JsonObject(readYaml(in));
                }
            }
        }
        return null;
    }

    public static Map<String, Object> readYaml(InputStream inputStream) {
        Yaml yaml = new Yaml();
        return yaml.load(inputStream);
    }
}
