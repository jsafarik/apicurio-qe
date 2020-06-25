package apicurito.tests.utils.openshift;

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Optional;

import apicurito.tests.configuration.TestConfiguration;
import apicurito.tests.utils.HttpUtils;
import cz.xtf.core.openshift.OpenShift;
import cz.xtf.core.openshift.OpenShiftBinary;
import cz.xtf.core.openshift.OpenShifts;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.LocalPortForward;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.client.NamespacedOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftConfigBuilder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import okhttp3.Response;

@Slf4j
public final class OpenShiftUtils {

    private static OpenShift xtfUtils = null;
    private static OpenShiftBinary binary = null;

    public static OpenShift getInstance() {
        if (xtfUtils == null) {
            new OpenShiftUtils();
        }
        return xtfUtils;
    }

    public static OpenShift xtf() {
        return getInstance();
    }

    public static OpenShiftBinary binary() {
        if (binary == null) {
            binary = OpenShifts.masterBinary(TestConfiguration.openShiftNamespace());
        }
        return binary;
    }

    private OpenShiftUtils() {
        if (xtfUtils == null) {
            final OpenShiftConfigBuilder openShiftConfigBuilder = new OpenShiftConfigBuilder()
                .withMasterUrl(TestConfiguration.openShiftUrl())
                .withTrustCerts(true)
                .withRequestTimeout(120_000)
                .withNamespace(TestConfiguration.openShiftNamespace())
                .withUsername(TestConfiguration.getOpenshiftUsername())
                .withPassword(TestConfiguration.getOpenshiftPassword());
            if (!TestConfiguration.openShiftToken().isEmpty()) {
                //if token is provided, lets use it
                //otherwise f8 client should be able to leverage ~/.kube/config or mounted secrets
                openShiftConfigBuilder.withOauthToken(TestConfiguration.openShiftToken());
            }
            xtfUtils = new OpenShift(openShiftConfigBuilder.build());
        }
    }

    public static OpenShift getAnotherOpenShiftUtils(String namespace) {
        final OpenShiftConfigBuilder openShiftConfigBuilder = new OpenShiftConfigBuilder()
            .withMasterUrl(TestConfiguration.openShiftUrl())
            .withTrustCerts(true)
            .withRequestTimeout(120_000)
            .withNamespace(namespace)
            .withUsername(TestConfiguration.getOpenshiftUsername())
            .withPassword(TestConfiguration.getOpenshiftPassword());
        if (!TestConfiguration.openShiftToken().isEmpty()) {
            //if token is provided, lets use it
            //otherwise f8 client should be able to leverage ~/.kube/config or mounted secrets
            openShiftConfigBuilder.withOauthToken(TestConfiguration.openShiftToken());
        }
        return new OpenShift(openShiftConfigBuilder.build());
    }

    /**
     * @deprecated use OpenshiftUtils.getInstance()
     */
    @Deprecated
    public static NamespacedOpenShiftClient client() {
        return getInstance();
    }

    public static LocalPortForward portForward(Pod pod, int remotePort, int localPort) {
        return getPodResource(pod).portForward(remotePort, localPort);
    }

    private static PodResource<Pod, DoneablePod> getPodResource(Pod pod) {
        if (pod.getMetadata().getNamespace() != null) {
            return client().pods().inNamespace(pod.getMetadata().getNamespace()).withName(pod.getMetadata().getName());
        } else {
            return client().pods().withName(pod.getMetadata().getName());
        }
    }

    public static Optional<Pod> getPodByPartialName(String partialName) {
        Optional<Pod> oPod = OpenShiftUtils.getInstance().getPods().stream()
            .filter(p -> p.getMetadata().getName().contains(partialName))
            .filter(p -> !p.getMetadata().getName().contains("deploy"))
            .filter(p -> !p.getMetadata().getName().contains("build"))
            .findFirst();
        return oPod;
    }

    public static int extractPodSequenceNr(Pod pod) {
        String podFullName = pod.getMetadata().getName();
        String[] pole = podFullName.split("-");
        return Integer.parseInt(pole[2]);
    }

    public static String getIntegrationLogs(String integrationName) {
        return getPodLogs(integrationName.replaceAll("[\\s_]", "-").toLowerCase());
    }

    public static String getPodLogs(String podPartialName) {
        Optional<Pod> integrationPod = OpenShiftUtils.getInstance().getPods().stream()
            .filter(p -> !p.getMetadata().getName().contains("build"))
            .filter(p -> !p.getMetadata().getName().contains("deploy"))
            .filter(p -> p.getMetadata().getName().contains(podPartialName)).findFirst();
        if (integrationPod.isPresent()) {
            String logText = OpenShiftUtils.getInstance().getPodLog(integrationPod.get());
            assertThat(logText)
                .isNotEmpty();
            return logText;
        } else {
            fail("No pod found for pod name: " + podPartialName);
        }
        //this can not happen due to assert
        return null;
    }

    /**
     * Some of the resources can't be created by f8 client, therefore we manually post them to corresponding endpoint.
     *
     * @param kind kind of the resource
     * @param o object instance
     */
    public static void create(String kind, Object o) {
        try {
            final String content = Serialization.jsonMapper().writeValueAsString(o);
            StringBuilder url = new StringBuilder();
            String[] kinds = new String[] {"serviceaccount", "role", "rolebinding", "clusterrole", "clusterrolebinding", "deployment"};
            if (StringUtils.equalsAnyIgnoreCase(kind, kinds)) {
                url.append("/api")
                    .append(kind.toLowerCase().equals("serviceaccount") ? "" : "s")
                    .append("/")
                    .append(((HasMetadata) o).getApiVersion())
                    .append("/namespaces/")
                    .append(TestConfiguration.openShiftNamespace())
                    .append("/")
                    .append(kind.toLowerCase())
                    .append("s");
            } else {
                // This can be created by the client, so create it
                getInstance().createResources((HasMetadata) o);
                return;
            }

            Response response = invokeApi(url.toString(), content);
            // 409 means that the resource already exists - this is the case for clusterrole / clusterbinding that are tied to the whole
            // cluster obviously - therefore it is ok to continue with 409
            if (response.code() != 409) {
                assertThat(response.code()).isGreaterThanOrEqualTo(200);
                assertThat(response.code()).isLessThan(300);
            }
        } catch (IOException e) {
            fail("Unable to create resource", e);
        }
    }

    /**
     * Invoke openshift's API. Only part behind master url is necessary and the path must start with slash.
     *
     * @param url api path
     * @param body body to send as JSON
     * @return response object
     */
    public static Response invokeApi(String url, String body) {
        url = TestConfiguration.openShiftUrl() + url;
        log.debug(url);
        Response response = HttpUtils.doPostRequest(
            url,
            body,
            "application/json",
            Headers.of("Authorization", "Bearer " + OpenShiftUtils.getInstance().getConfiguration().getOauthToken())
        );
        log.debug("Response code: " + response.code());
        try {
            log.debug("Response: " + response.body().string());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }
}
