package com.flinkstress.harness.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads a named scenario profile from {@code /scenarios/<name>.yaml} on the
 * classpath and flattens it into dot-notation parameter keys that
 * {@link HarnessConfig} understands.
 */
public final class ScenarioLoader {

    private ScenarioLoader() {
    }

    /** Loads and flattens the scenario; throws if the resource is missing. */
    public static Map<String, String> load(String scenarioName) {
        String resource = "/scenarios/" + scenarioName + ".yaml";
        try (InputStream in = ScenarioLoader.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("Scenario not found on classpath: " + resource);
            }
            Object root = new Yaml().load(in);
            Map<String, String> flat = new LinkedHashMap<>();
            flatten("", root, flat);
            return flat;
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to read scenario " + resource, e);
        }
    }

    @SuppressWarnings("unchecked")
    static void flatten(String prefix, Object node, Map<String, String> out) {
        if (node instanceof Map) {
            for (Map.Entry<String, Object> e : ((Map<String, Object>) node).entrySet()) {
                String key = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
                flatten(key, e.getValue(), out);
            }
        } else if (node != null) {
            out.put(prefix, String.valueOf(node));
        }
    }

    /** Convenience for tests: flatten an in-memory map. */
    public static Map<String, String> flattenMap(Map<String, Object> nested) {
        Map<String, String> out = new HashMap<>();
        flatten("", nested, out);
        return out;
    }
}
