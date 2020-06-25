package apicurito.tests.configuration.templates;

import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import apicurito.tests.configuration.Component;
import apicurito.tests.configuration.TestConfiguration;
import apicurito.tests.utils.openshift.OpenShiftUtils;
import cz.xtf.core.waiting.WaiterException;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesList;
import io.fabric8.kubernetes.api.model.apps.ReplicaSet;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.openshift.api.model.Template;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApicuritoTemplate {

    public static Template getTemplate() {
        try (InputStream is = new URL(TestConfiguration.templateUrl()).openStream()) {
            return OpenShiftUtils.getInstance().templates().load(is).get();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to read apicurito template ", ex);
        }
    }

    /**
     * Apply image stream
     * If Apicurito UI image is set in properties, add it with the TestConfiguration.APICURITO_IMAGE_VERSION tag
     */
    public static void setImageStreams() {
        TestConfiguration.printDivider("Setting up input streams");

        log.info("Deploying image stream " + TestConfiguration.templateInputStreamUrl());
        final String output = OpenShiftUtils.binary().execute(
            "apply",
            "-n", TestConfiguration.openShiftNamespace(),
            "-f", TestConfiguration.templateInputStreamUrl()
        );

        if (TestConfiguration.apicuritoUiImage() != null) {
            log.info("UI image specified, updating image stream with {}", TestConfiguration.apicuritoUiImage());
            OpenShiftUtils.binary().execute(
                "tag",
                TestConfiguration.apicuritoUiImage(),
                "apicurito-ui:" + TestConfiguration.APICURITO_IMAGE_VERSION
            );
        }
    }

    public static void deploy() {
        if (TestConfiguration.useOperator()) {
            deployUsingGoOperator();
        } else {
            deployUsingTemplate();
        }
    }

    public static void deployUsingTemplate() {
        TestConfiguration.printDivider("Deploying using template");

        // get the template
        Template template = getTemplate();
        // set params
        Map<String, String> templateParams = new HashMap<>();
        templateParams.put("ROUTE_HOSTNAME", TestConfiguration.openShiftNamespace() + "." + TestConfiguration.openShiftRouteSuffix());
        log.info("Deploying on address: https://" + TestConfiguration.openShiftNamespace() + "." + TestConfiguration.openShiftRouteSuffix());
        templateParams.put("OPENSHIFT_MASTER", TestConfiguration.openShiftUrl());
        templateParams.put("OPENSHIFT_PROJECT", TestConfiguration.openShiftNamespace());
        templateParams.put("IMAGE_STREAM_NAMESPACE", TestConfiguration.openShiftNamespace());

        if (TestConfiguration.apicuritoUiImage() != null) {
            templateParams.put("APP_VERSION", Double.toString(TestConfiguration.APICURITO_IMAGE_VERSION));
        }

        // process & create
        KubernetesList processedTemplate = OpenShiftUtils.getInstance().recreateAndProcessTemplate(template, templateParams);
        for (HasMetadata hasMetadata : processedTemplate.getItems()) {
            OpenShiftUtils.getInstance().createResources(hasMetadata);
        }
    }

    private static void deployUsingGoOperator() {
        log.info("Deploying using GO operator");

        createInOCP("CRD", TestConfiguration.apicuritoOperatorCrdUrl());
        createInOCP("Service", TestConfiguration.apicuritoOperatorServiceUrl());
        createInOCP("Role", TestConfiguration.apicuritoOperatorRoleUrl());
        createInOCP("Role binding", TestConfiguration.apicuritoOperatorRoleBindingUrl());
        createInOCP("Operator", TestConfiguration.apicuritoOperatorUrl());

        applyInOCP("Custom Resource", TestConfiguration.apicuritoOperatorCrUrl());

        if (TestConfiguration.apicuritoUiImage() != null) {
            setTestEnvToOperator("APICURITO_IMAGE", TestConfiguration.apicuritoUiImage());
        }
    }

    private static void setTestEnvToOperator(String nameOfEnv, String valueOfEnv) {
        log.info("Setting test ENV: " + nameOfEnv + "=" + valueOfEnv);
        final String output = OpenShiftUtils.binary().execute(
            "set",
            "env",
            "deployment",
            "apicurito-operator",
            nameOfEnv + "=" + valueOfEnv
        );
    }

    private static void createInOCP(String itemName, String item) {
        log.info("Creating " + itemName + " from: " + item);

        final String output = OpenShiftUtils.binary().execute(
            "create",
            "-n", TestConfiguration.openShiftNamespace(),
            "-f", item
        );
    }

    public static void applyInOCP(String itemName, String item) {
        log.info("Applying {} from: {}", itemName, item);
        final String output = OpenShiftUtils.binary().execute(
            "apply", "-n", TestConfiguration.openShiftNamespace(), "-f", item
        );
    }

    public static void applyInOCP(String itemName, String namespace, String item) {
        log.info("Applying {} from: {}", itemName, item);
        final String output = OpenShiftUtils.binary().execute(
            "apply", "-n", namespace, "-f", item
        );
    }

    public static void cleanNamespace() {
        TestConfiguration.printDivider("Deleting namespace...");

        try {
            OpenShiftUtils.getInstance().customResourceDefinitions().delete();
            //OCP4HACK - openshift-client 4.3.0 isn't supported with OCP4 and can't create/delete templates, following line can be removed later
            OpenShiftUtils.binary().execute("delete", "template", "--all");
            OpenShiftUtils.getInstance().apps().statefulSets().inNamespace(TestConfiguration.openShiftNamespace()).delete();
            OpenShiftUtils.getInstance().apps().deployments().inNamespace(TestConfiguration.openShiftNamespace()).delete();
            OpenShiftUtils.getInstance().serviceAccounts().inNamespace(TestConfiguration.openShiftNamespace()).delete();
        } catch (KubernetesClientException ex) {
            // Probably user does not have permissions to delete.. a nice exception will be printed when deploying
        }
        try {
            OpenShiftUtils.getInstance().clean();

            List<ReplicaSet> operatorReplicaSets =
                OpenShiftUtils.getInstance().apps().replicaSets().inNamespace(TestConfiguration.openShiftNamespace()).list().getItems();

            for (ReplicaSet rs : operatorReplicaSets) {
                OpenShiftUtils.binary().execute("delete", "rs", rs.getMetadata().getName());
            }

            OpenShiftUtils.getInstance().waiters().isProjectClean().waitFor();
        } catch (WaiterException e) {
            log.warn("Project was not clean after 20s, retrying once again");
            OpenShiftUtils.getInstance().clean();
            OpenShiftUtils.getInstance().waiters().isProjectClean().waitFor();
        }
        OpenShiftUtils.xtf().getTemplates().forEach(OpenShiftUtils.xtf()::deleteTemplate);
    }

    public static void waitForApicurito(String key, Integer numberOfPods, Component component) {
        TestConfiguration.printDivider("Waiting for Apicurito to become ready...");

        EnumSet<Component> components = EnumSet.noneOf(Component.class);
        components.add(component);

        ExecutorService executorService = Executors.newFixedThreadPool(components.size());
        components.forEach(c -> {
            Runnable runnable = () ->
                OpenShiftUtils.xtf().waiters()
                    .areExactlyNPodsReady(numberOfPods, key, c.getName())
                    .interval(TimeUnit.SECONDS, 10)
                    .timeout(TimeUnit.MINUTES, 6)
                    .waitFor();
            executorService.submit(runnable);
        });

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(20, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
                fail("Apicurito wasn't initilized in time");
            }
        } catch (InterruptedException e) {
            fail("Apicurito wasn't initilized in time");
        }
    }

    public static void cleanOcpAfterOperatorhubTest() {
        final String output = OpenShiftUtils.binary().execute("delete", "project", "operatorhub");
        final String output2 = OpenShiftUtils.binary().execute("delete", "operatorsource", "fuse-apicurito", "-n", "openshift-marketplace");
        String available = "src/test/resources/operatorhubFiles/availableOH.yaml";
        ApicuritoTemplate.applyInOCP("Available operators", "openshift-marketplace", available);
    }

    public static void createPullSecret() {
        if (TestConfiguration.apicuritoPullSecret() != null) {
            String pullSecretName = "apicurito-pull-secret";
            log.info("Creating a pull secret with name " + pullSecretName);
            OpenShiftUtils.getInstance().secrets().createOrReplaceWithNew()
                .withNewMetadata()
                .withName(pullSecretName)
                .endMetadata()
                .withData(Collections.singletonMap(".dockerconfigjson", TestConfiguration.apicuritoPullSecret()))
                .withType("kubernetes.io/dockerconfigjson")
                .done();
        }
    }
}
