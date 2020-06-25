package apicurito.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import com.codeborne.selenide.Configuration;

import apicurito.tests.configuration.Component;
import apicurito.tests.configuration.TestConfiguration;
import apicurito.tests.configuration.templates.ApicuritoTemplate;
import apicurito.tests.utils.openshift.OpenShiftUtils;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(Cucumber.class)
@CucumberOptions(
        features = "classpath:features",
        tags = {"not @manual", "not @wip", "not @ignore"},
        plugin = {"pretty", "html:target/cucumber/cucumber-html", "junit:target/cucumber/cucumber-junit.xml",
                "json:target/cucumber/cucumber-report.json"})
public class TestRunner {

    @BeforeClass
    public static void beforeTests() {

        boolean createdProject = false;

        if (OpenShiftUtils.xtf().getProject(TestConfiguration.openShiftNamespace()) == null) {
            createdProject = true;
            log.info("Creating new project " + TestConfiguration.openShiftNamespace());
            OpenShiftUtils.getInstance().createProjectRequest(TestConfiguration.openShiftNamespace());
            if (OpenShiftUtils.getInstance().getProject() == null) {
                log.info("Waiting for " + TestConfiguration.openShiftNamespace() + " to be created");
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                log.info(TestConfiguration.openShiftNamespace() + " project created");
            }
        }

        if (Boolean.valueOf(TestConfiguration.doReinstall()) && !createdProject) {
            ApicuritoTemplate.cleanNamespace();
        }

        // TODO: don't rely on namespace name, rather use properties file/maven argument
        if ("apicurito".equals(TestConfiguration.openShiftNamespace())) {
            ApicuritoTemplate.createPullSecret();
            ApicuritoTemplate.setImageStreams();
            ApicuritoTemplate.deploy();
            if (TestConfiguration.useOperator()) {
                ApicuritoTemplate.waitForApicurito("component", 6, Component.SERVICE);
            } else {
                ApicuritoTemplate.waitForApicurito("component", 1, Component.UI);
            }
        }

        //set up Selenide
        Configuration.timeout = TestConfiguration.getConfigTimeout() * 1000;
        //We will now use custom web driver
        //Configuration.browser = TestConfiguration.apicuritoBrowser();
        Configuration.browser = "apicurito.tests.configuration.CustomWebDriverProvider";
        //TODO temporary workaround for jenkins job
        //Configuration.browserSize = "1920x1080";
    }

    @AfterClass
    public static void afterTests() {
        log.info("After Tests");
        if ("operatorhub".equals(TestConfiguration.openShiftNamespace())) {
            ApicuritoTemplate.cleanOcpAfterOperatorhubTest();
        }
        if (TestConfiguration.namespaceCleanupAfter()) {
            TestConfiguration.printDivider("Cleaning namespace after tests");
            OpenShiftUtils.getInstance().clean();
            OpenShiftUtils.getInstance().waiters().isProjectClean().waitFor();
        }
    }
}
