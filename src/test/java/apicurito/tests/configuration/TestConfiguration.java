package apicurito.tests.configuration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Slf4j
public class TestConfiguration {

    public static final String APICURITO_IMAGE_VERSION = "2.0";

    public static final String OPENSHIFT_URL = "apicurito.config.openshift.url";
    public static final String OPENSHIFT_TOKEN = "apicurito.config.openshift.token";
    public static final String OPENSHIFT_NAMESPACE = "apicurito.config.openshift.namespace";
    public static final String OPENSHIFT_NAMESPACE_CLEANUP_AFTER = "apicurito.config.openshift.namespace.cleanup.after";
    public static final String OPENSHIFT_ROUTE_SUFFIX = "apicurito.config.openshift.route.suffix";
    public static final String OPENSHIFT_REINSTALL = "apicurito.config.openshift.reinstall";

    public static final String APICURITO_TEMPLATE_URL = "apicurito.config.template.url";
    public static final String APICURITO_IS_TEMPLATE_URL = "apicurito.config.inputstream.template.url";
    public static final String APICURITO_UI_BROWSER = "apicurito.config.ui.browser";
    public static final String APICURITO_UI_URL = "apicurito.config.ui.url";

    public static final String TESTSUITE_TIMEOUT = "apicurito.config.timeout";

    public static final String APP_ROOT = "apicurito.config.app.root";

    public static final String APICURITO_USE_OPERATOR = "apicurito.config.use.operator";
    public static final String APICURITO_OPERATOR_CRD_URL = "apicurito.config.operator.crd";
    public static final String APICURITO_OPERATOR_DEPLOYMENT_URL = "apicurito.config.operator.deployment.url";
    public static final String APICURITO_OPERATOR_IMAGE_URL = "apicurito.config.operator.image.url";
    public static final String APICURITO_OPERATOR_SERVICE_URL = "apicurito.config.operator.service";
    public static final String APICURITO_OPERATOR_ROLE_URL = "apicurito.config.operator.role";
    public static final String APICURITO_OPERATOR_ROLE_BINDING_URL = "apicurito.config.operator.rolebinding";
    public static final String APICURITO_OPERATOR_CR_URL = "apicurito.config.operator.cr";
    public static final String APICURITO_UI_IMAGE = "apicurito.config.ui.image";
    public static final String APICURITO_PULL_SECRET = "apicurito.config.pull.secret";
    public static final String APICURITO_DEPLOY_OPERATORHUB = "apicurito.config.deploy.operatorhub";

    public static final String APICURITO_UI_USERNAME = "apicurito.config.ui.username";
    public static final String APICURITO_UI_PASSWORD = "apicurito.config.ui.password";

    public static final String QUAY_USERNAME = "apicurito.config.quay.username";
    public static final String QUAY_PASSWORD = "apicurito.config.quay.password";
    public static final String QUAY_NAMESPACE = "apicurito.config.quay.namespace";
    public static final String QUAY_TOKEN = "apicurito.config.quay.auth.token";
    public static final String QUAY_OPSRC_TOKEN = "apicurito.config.quay.opsrc.token";

    public static final String PULL_SECRET = "apicurito.config.pull.secret";

    private static final TestConfiguration INSTANCE = new TestConfiguration();

    private final Properties properties = new Properties();

    private TestConfiguration() {
        // first let's try product properties
        copyValues(fromPath("test.properties"), true);

        // then product properties
        copyValues(fromPath("../test.properties"));

        // then system variables
        copyValues(System.getProperties());

        // then environment variables
        // TODO: copyValues(fromEnvironment());

        // then defaults
        copyValues(defaultValues());
    }

    public static TestConfiguration get() {
        return INSTANCE;
    }

    public static String openShiftUrl() {
        return get().readValue(OPENSHIFT_URL);
    }

    public static String openShiftToken() {
        return get().readValue(OPENSHIFT_TOKEN);
    }

    public static String openShiftNamespace() {
        return get().readValue(OPENSHIFT_NAMESPACE, "apicurito");
    }

    public static String templateUrl() {
        return get()
            .readValue(APICURITO_TEMPLATE_URL, "https://raw.githubusercontent.com/jboss-fuse/application-templates/2.1.x.redhat-7-x/fuse-apicurito.yml");
    }

    public static String templateInputStreamUrl() {
        return get()
                .readValue(APICURITO_IS_TEMPLATE_URL, "https://raw.githubusercontent.com/jboss-fuse/application-templates/master/fis-image-streams.json");
    }

    public static String openShiftRouteSuffix() {
        return get().readValue(OPENSHIFT_ROUTE_SUFFIX);
    }

    public static boolean useOperator() {
        return Boolean.parseBoolean(get().readValue(APICURITO_USE_OPERATOR));
    }

    public static boolean deployOperatorHub() {
        return Boolean.parseBoolean(get().readValue(APICURITO_DEPLOY_OPERATORHUB, "false"));
    }

    // pull secret for quay (not for pulling images)
    public static String getPullSecret() {
        return get().readValue(PULL_SECRET);
    }

    public static String apicuritoOperatorCrdUrl() {
        return get().readValue(APICURITO_OPERATOR_CRD_URL);
    }

    public static String apicuritoOperatorCrUrl() {
        return get().readValue(APICURITO_OPERATOR_CR_URL);
    }

    public static String apicuritoOperatorDeploymentUrl() {
        return get().readValue(APICURITO_OPERATOR_DEPLOYMENT_URL);
    }

    public static String apicuritoOperatorImageUrl() {
        return get().readValue(APICURITO_OPERATOR_IMAGE_URL);
    }

    public static String apicuritoOperatorServiceUrl() {
        return get().readValue(APICURITO_OPERATOR_SERVICE_URL);
    }

    public static String apicuritoOperatorRoleUrl() {
        return get().readValue(APICURITO_OPERATOR_ROLE_URL);
    }

    public static String apicuritoOperatorRoleBindingUrl() {
        return get().readValue(APICURITO_OPERATOR_ROLE_BINDING_URL);
    }

    public static String apicuritoUiImage() {
        return get().readValue(APICURITO_UI_IMAGE);
    }

    public static int getConfigTimeout() {
        return Integer.parseInt(get().readValue(TESTSUITE_TIMEOUT, "300"));
    }

    public static String apicuritoBrowser() {
        return get().readValue(APICURITO_UI_BROWSER, "firefox");
    }

    public static boolean namespaceCleanupAfter() {
        return Boolean.parseBoolean(get().readValue(OPENSHIFT_NAMESPACE_CLEANUP_AFTER, "false"));
    }

    public static String apicuritoUrl() {
        return get().readValue(APICURITO_UI_URL);
    }

    public static String doReinstall() {
        return get().readValue(OPENSHIFT_REINSTALL, "true");
    }

    public static String getAppRoot() {
        return get().readValue(APP_ROOT, "app-root");
    }

    public static String getQuayUsername() {
        return get().readValue(QUAY_USERNAME);
    }

    public static String getQuayPassword() {
        return get().readValue(QUAY_PASSWORD);
    }

    public static String getQuayNamespace() {
        return get().readValue(QUAY_NAMESPACE);
    }

    public static String getQuayOpsrcToken() {
        return get().readValue(QUAY_OPSRC_TOKEN);
    }

    public static String getQuayToken() {
        return get().readValue(QUAY_TOKEN);
    }

    public static String getOpenshiftUsername() {
        // This is used as openshift admin account when testing operatorhub
        return get().readValue(APICURITO_UI_USERNAME);
    }

    public static String getOpenshiftPassword() {
        // This is used as openshift admin account when testing operatorhub
        return get().readValue(APICURITO_UI_PASSWORD);
    }

    // pull secret for pulling images
    public static String apicuritoPullSecret() {
        return get().readValue(APICURITO_PULL_SECRET);
    }

    private Properties defaultValues() {
        final Properties props = new Properties();

        props.setProperty(OPENSHIFT_URL, "");
        props.setProperty(OPENSHIFT_TOKEN, "");

        if (props.getProperty(APICURITO_OPERATOR_CRD_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_CRD_URL, String.format(
                    "https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/crds/apicur_v1alpha1_apicurito_crd.yaml"));
        }
        if (props.getProperty(APICURITO_OPERATOR_DEPLOYMENT_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_DEPLOYMENT_URL,
                String.format("https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/operator.yaml"));
        }
        if (props.getProperty(APICURITO_OPERATOR_CR_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_CR_URL, String.format(
                    "https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/crds/apicur_v1alpha1_apicurito_cr.yaml"));
        }
        if (props.getProperty(APICURITO_OPERATOR_SERVICE_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_SERVICE_URL,
                    String.format("https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/service_account.yaml"));
        }
        if (props.getProperty(APICURITO_OPERATOR_ROLE_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_ROLE_URL,
                    String.format("https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/role.yaml"));
        }
        if (props.getProperty(APICURITO_OPERATOR_ROLE_BINDING_URL) == null) {
            props.setProperty(APICURITO_OPERATOR_ROLE_BINDING_URL,
                    String.format("https://raw.githubusercontent.com/Apicurio/apicurio-operators/master/apicurito/deploy/role_binding.yaml"));
        }

        props.setProperty(APICURITO_USE_OPERATOR, "true");

        // Copy syndesis properties to their xtf counterparts - used by binary oc client
        if (System.getProperty("xtf.openshift.url") == null) {
            System.setProperty("xtf.openshift.url", properties.getProperty(OPENSHIFT_URL));
            System.setProperty("xtf.openshift.master.username", properties.getProperty(APICURITO_UI_USERNAME));
            System.setProperty("xtf.openshift.master.password", properties.getProperty(APICURITO_UI_PASSWORD));

            System.setProperty("xtf.openshift.namespace", properties.getProperty(OPENSHIFT_NAMESPACE));

            // Set oc version - this version of the client will be used as the binary client
            System.setProperty("xtf.openshift.version", "4.3");
        }
        return props;
    }

    public String readValue(final String key) {
        return readValue(key, null);
    }

    public String readValue(final String key, final String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    private Properties fromPath(final String path) {
        final Properties props = new Properties();

        final Path propsPath = Paths.get(path)
                .toAbsolutePath();
        if (Files.isReadable(propsPath)) {
            try (InputStream is = Files.newInputStream(propsPath)) {
                props.load(is);
            } catch (final IOException ex) {
                log.warn("Unable to read properties from '{}'", propsPath);
                log.debug("Exception", ex);
            }
        }

        return props;
    }

    private void copyValues(final Properties source) {
        copyValues(source, false);
    }

    private void copyValues(final Properties source, final boolean overwrite) {
        source.stringPropertyNames().stream()
                .filter(key -> overwrite || !this.properties.containsKey(key))
                .forEach(key -> this.properties.setProperty(key, source.getProperty(key)));
    }

    public static void printDivider(String label) {
        log.info("__________________________________________________________________");
        log.info("##################################################################");
        log.info("### -----> " + label);
        log.info("##################################################################");
    }
}
