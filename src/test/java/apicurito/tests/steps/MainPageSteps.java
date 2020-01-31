package apicurito.tests.steps;

import static com.codeborne.selenide.Condition.attribute;
import static com.codeborne.selenide.Condition.enabled;
import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Condition.visible;

import apicurito.tests.utils.slenide.OperationUtils;

import org.openqa.selenium.By;

import com.codeborne.selenide.ElementsCollection;
import com.codeborne.selenide.SelenideElement;

import java.util.Arrays;
import java.util.List;

import apicurito.tests.utils.slenide.CommonUtils;
import apicurito.tests.utils.slenide.MainPageUtils;
import cucumber.api.java.en.When;
import io.cucumber.datatable.DataTable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainPageSteps {

    private static class SecurityElements {
        private static By SECURITY_SCHEME_EDITOR = By.cssSelector("security-scheme-editor");
        private static By SECURITY_REQUIREMENT_EDITOR = By.cssSelector("security-requirement-editor");
        private static By DESCRIPTION = By.id("description");
        private static By DROPDOWN = By.className("dropdown");
        private static By API_KEY_AUTH = By.className("apiKey-auth");
        private static By OAUTH2_AUTH = By.className("oauth2-auth");
        private static By SCHEMES_LIST = By.className("list-group-item");
        private static By CHECKBOX = By.cssSelector("input");
        private static By SCOPE = By.className("scope");
    }

    private static class BasicElements {
        private static By BUTTON = By.cssSelector("button");
        private static By A = By.cssSelector("a");
    }

    private static class MainPageElements {
        private static By INFO_SECTION = By.cssSelector("info-section");
        private static By CONTACT_SECTION = By.cssSelector("contact-section");
        private static By LICENSE_SECTION = By.cssSelector("license-section");
        private static By TAGS_SECTION = By.cssSelector("tags-section");
        private static By SECURITY_SECTION = By.cssSelector("security-schemes-section");
        private static By REQUIREMENTS_SECTION = By.cssSelector("security-requirements-section");
    }

    /**
     * Second column: if true then create with link else create with plus sign.
     */
    @When("^create a new path with link$")
    public void createNewPathWithLink(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            if (Boolean.valueOf(dataRow.get(1))) {
                CommonUtils.getClickableLink(CommonUtils.Sections.PATH,
                    CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")))
                    .click();
            } else {
                CommonUtils.getNewPlusSignButton(CommonUtils.Sections.PATH,
                    CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")))
                    .click();
            }

            CommonUtils.getLabelWithName("path", CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")))
                .setValue("/" + dataRow.get(0));
            CommonUtils.getButtonWithText("Add", CommonUtils.getAppRoot()).shouldBe(visible, enabled).shouldNotHave(attribute("disabled"))
                .click();
        }
    }

    @When("^delete path \"([^\"]*)\"$")
    public void deletePath(String path) {
        MainPageUtils.getPathWithName(path).contextClick();
        CommonUtils.getDropdownMenuItem("Delete").shouldBe(visible).click();
    }

    @When("^change API name to \"([^\"]*)\"$")
    public void changeAPINameTo(String newName) {
        SelenideElement se = CommonUtils.getAppRoot().$(By.className("editor-title-bar-edit"));
        se.hover();
        se.click();

        CommonUtils.getLabelWithName("title-input", CommonUtils.getAppRoot())
            .setValue(newName);
        CommonUtils.getButtonWithTitle("Save changes.", CommonUtils.getAppRoot().$("title-bar"))
            .click();
    }

    @When("^set API version to \"([^\"]*)\"$")
    public void setAPIVersionTo(String version) {
        CommonUtils.setValueInLabel(version, CommonUtils.getAppRoot().$(MainPageElements.INFO_SECTION).$(By.className("version")), false);
    }

    @When("^change description to \"([^\"]*)\"$")
    public void changeDescriptionTo(String description) {
        CommonUtils.setValueInTextArea(description, CommonUtils.getAppRoot().$(MainPageElements.INFO_SECTION).$(By.className("description")));
    }

    @When("^add contact info$")
    public void addContactInfo(DataTable table) {
        CommonUtils.getClickableLink(CommonUtils.Sections.CONTACT, MainPageElements.CONTACT_SECTION).click();
        for (List<String> dataRow : table.cells()) {
            CommonUtils.setValueInLabel(dataRow.get(0), CommonUtils.getAppRoot().$(MainPageElements.CONTACT_SECTION).$(By.className("name")), false);
            CommonUtils.setValueInLabel(dataRow.get(1), CommonUtils.getAppRoot().$(MainPageElements.CONTACT_SECTION).$(By.className("email")), false);
            CommonUtils.setValueInLabel(dataRow.get(2), CommonUtils.getAppRoot().$(MainPageElements.CONTACT_SECTION).$(By.className("url")), false);
        }
    }

    @When("^add license \"([^\"]*)\"$")
    public void addLicense(String license) {
        CommonUtils.getClickableLink(CommonUtils.Sections.LICENSE, MainPageElements.LICENSE_SECTION).click();
        MainPageUtils.setLicense(license);
    }

    @When("^add tag \"([^\"]*)\" with description \"([^\"]*)\"$")
    public void addTagWithDescription(String tag, String description) {
        CommonUtils.getClickableLink(CommonUtils.Sections.TAG, MainPageElements.TAGS_SECTION).click();
        MainPageUtils.addTag(tag, description);
    }

    @When("^select path \"([^\"]*)\"$")
    public void selectPath(String path) {
        MainPageUtils.getPathWithName(path).click();
    }

    @When("^set consumes or produces \"([^\"]*)\" to \"([^\"]*)\"$")
    //TODO this is add consume but application/json is still there --> delete it
    public void setConsumesProducesTo(String consumesProduces, String values) {
        log.info("Setting {} to {}", consumesProduces, values);
        CommonUtils.setValueInLabel(values, MainPageUtils.getMainPageRoot().$(By.className(consumesProduces)), true);
    }

    /**
     * @param table parameters:
     * Name
     * Description OPTIONAL (could be empty string)
     * Example in json format OPTIONAL (could be empty string)
     * boolean if should be created with REST resources
     * boolean if should be created with Link
     */
    @When("^create a new data type by link$")
    public void createANewDataType(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            if (Boolean.valueOf(dataRow.get(4))) {
                CommonUtils.getClickableLink(CommonUtils.Sections.DATA_TYPES, CommonUtils.getAppRoot()).click();
            } else {
                CommonUtils.getNewPlusSignButton(CommonUtils.Sections.DATA_TYPES, CommonUtils.getAppRoot()).click();
            }

            MainPageUtils.createDataType(dataRow.get(0), dataRow.get(1), dataRow.get(2), Boolean.valueOf(dataRow.get(3)));
        }
    }

    @When("^search path or data type with substring \"([^\"]*)\"$")
    public void searchPathOrDataTypeWithSubstring(String substring) {
        MainPageUtils.putSearchSubstring(substring);
    }

    @When("^select data type \"([^\"]*)\"$")
    public void selectDataType(String dataTypeName) {
        MainPageUtils.getDataTypeWithName(dataTypeName).click();
    }

    @When("^create basic security scheme with values$")
    public void createBasicSecuritySchemeWithValues(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            CommonUtils.getNewPlusSignButton(CommonUtils.Sections.SCHEME, MainPageUtils.getMainPageRoot().$(MainPageElements.SECURITY_SECTION))
                .click();
            SelenideElement schemeEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_SCHEME_EDITOR);

            CommonUtils.getLabelWithName("schemeName", schemeEditor)
                .setValue(dataRow.get(0));

            if (!dataRow.get(1).isEmpty()) {
                schemeEditor.$(SecurityElements.DESCRIPTION).setValue(dataRow.get(1));
            }

            CommonUtils.setDropDownValue("button", "BASIC", schemeEditor.$(SecurityElements.DROPDOWN));

            CommonUtils.getButtonWithText("Save", schemeEditor)
                .click();
        }
    }

    @When("^create API Key security scheme with values$")
    public void createAPIKeySecuritySchemeWithValues(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            CommonUtils.getNewPlusSignButton(CommonUtils.Sections.SCHEME, MainPageUtils.getMainPageRoot().$(MainPageElements.SECURITY_SECTION))
                .click();
            SelenideElement schemeEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_SCHEME_EDITOR);

            CommonUtils.getLabelWithName("schemeName", schemeEditor)
                .setValue(dataRow.get(0));

            if (!dataRow.get(1).isEmpty()) {
                schemeEditor.$(SecurityElements.DESCRIPTION).setValue(dataRow.get(1));
            }
            CommonUtils.setDropDownValue("button", "API Key", schemeEditor.$(SecurityElements.DROPDOWN));

            if (!dataRow.get(2).isEmpty()) {
                CommonUtils.setDropDownValue("#in20", dataRow.get(2), schemeEditor.$(SecurityElements.API_KEY_AUTH));
            }

            if (!dataRow.get(3).isEmpty()) {
                CommonUtils.getLabelWithType("text", schemeEditor.$(SecurityElements.API_KEY_AUTH))
                    .setValue(dataRow.get(3));
            }
            CommonUtils.getButtonWithText("Save", schemeEditor)
                .click();
        }
    }

    @When("^create OAuth security scheme with values$")
    public void createOAuthSecuritySchemeWithValues(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            CommonUtils.getNewPlusSignButton(CommonUtils.Sections.SCHEME, MainPageUtils.getMainPageRoot().$(MainPageElements.SECURITY_SECTION))
                .click();
            SelenideElement schemeEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_SCHEME_EDITOR);

            CommonUtils.getLabelWithName("schemeName", schemeEditor)
                .setValue(dataRow.get(0));

            if (!dataRow.get(1).isEmpty()) {
                schemeEditor.$(SecurityElements.DESCRIPTION).setValue(dataRow.get(1));
            }
            CommonUtils.setDropDownValue("button", "OAuth 2", schemeEditor.$(SecurityElements.DROPDOWN));

            if (!dataRow.get(2).isEmpty()) {
                CommonUtils.setDropDownValue("#flow", dataRow.get(2), schemeEditor.$(SecurityElements.OAUTH2_AUTH));
            }

            if (!dataRow.get(3).isEmpty()) {
                CommonUtils.getLabelWithName("authorizationUrl", schemeEditor.$(SecurityElements.OAUTH2_AUTH))
                    .setValue(dataRow.get(3));
            }

            if (!dataRow.get(3).isEmpty()) {
                CommonUtils.getLabelWithName("tokenUrl", schemeEditor.$(SecurityElements.OAUTH2_AUTH))
                    .setValue(dataRow.get(4));
            }
            CommonUtils.getButtonWithText("Save", schemeEditor)
                .click();
        }
    }

    @When("^create security requirement with schemes$")
    public void createSecurityRequirementWithSchemes(DataTable table) {
        CommonUtils.getNewPlusSignButton(CommonUtils.Sections.REQUIREMENT, MainPageUtils.getMainPageRoot().$(MainPageElements.REQUIREMENTS_SECTION))
            .click();
        SelenideElement requirementEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_REQUIREMENT_EDITOR);

        for (List<String> dataRow : table.cells()) {
            ElementsCollection listOfSchemes = requirementEditor.$$(SecurityElements.SCHEMES_LIST);
            for (SelenideElement scheme : listOfSchemes) {
                if (scheme.$(By.className("name")).getText().equals(dataRow.get(0)) && !scheme.getAttribute("class").contains("active")) {
                    scheme.$(SecurityElements.CHECKBOX).click();
                    break;
                }
            }
        }
        CommonUtils.getButtonWithText("Save", requirementEditor)
            .click();
    }

    @When("^add scopes to security requirement \"([^\"]*)\" and OAuth scheme \"([^\"]*)\"$")
    public void addScopesToSecurityRequirementAndOAuthScheme(String requirementName, String schemeName, DataTable table) {
        SelenideElement requirement =
            MainPageUtils.getMainPageRoot().$(MainPageElements.REQUIREMENTS_SECTION).$$(By.className("security-requirement"))
                .filter(text(requirementName)).first();
        requirement.$(BasicElements.BUTTON).click();
        requirement.$(BasicElements.A).shouldHave(text("Edit")).click();

        SelenideElement requirementEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_REQUIREMENT_EDITOR);
        SelenideElement scheme = requirementEditor.$$(SecurityElements.SCHEMES_LIST).filter(text(schemeName)).first();
        scheme.$(By.className("list-view-pf-expand")).$("span").click();
        for (List<String> dataRow : table.cells()) {
            scheme.$$(SecurityElements.SCOPE).filter(text(dataRow.get(0))).first().$(SecurityElements.CHECKBOX).click();
        }
        CommonUtils.getButtonWithText("Save", requirementEditor)
            .click();
    }

    @When("^add scopes to scheme \"([^\"]*)\"$")
    public void addScopesToScheme(String scheme, DataTable table) {
        SelenideElement schemeElement =
            MainPageUtils.getMainPageRoot().$(MainPageElements.SECURITY_SECTION).$$("security-scheme-row").filter(text(scheme)).first();
        schemeElement.$(BasicElements.BUTTON).click();
        schemeElement.$(BasicElements.A).shouldHave(text("Edit")).click();

        SelenideElement schemeEditor = MainPageUtils.getMainPageRoot().$(SecurityElements.SECURITY_SCHEME_EDITOR);
        for (List<String> dataRow : table.cells()) {
            CommonUtils.getButtonWithText("Add Scope", schemeEditor).click();
            ElementsCollection scopeElements = schemeEditor.$$(SecurityElements.SCOPE);

            CommonUtils.setValueInLabel(dataRow.get(0), scopeElements.get(scopeElements.size() - 1).$(By.className("scope-name")), false);
            CommonUtils.setValueInLabel(dataRow.get(1), scopeElements.get(scopeElements.size() - 1).$(By.className("scope-description")), false);
        }
        CommonUtils.getButtonWithText("Save", schemeEditor)
            .click();
    }

    /**
     * @param table contains parameters:
     * section : path | data types
     * element name e.g. /myPath
     * action : New Sub-Path | Rename | Clone | Delete
     * new name for element e.g. /mySuperPath
     */
    @When("context click on and manage element")
    public void contextClickOn(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            if (dataRow.get(0).equals("path")) {
                MainPageUtils.getPathWithName(dataRow.get(1)).contextClick();
            } else {
                MainPageUtils.getDataTypeWithName(dataRow.get(1)).contextClick();
            }
            CommonUtils.getDropdownMenuItem(dataRow.get(2)).shouldBe(visible).click();

            if (!dataRow.get(2).equals("Delete")) {
                SelenideElement se =
                    CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")).$(By.className("modal-body")).$("input");
                se.clear();
                se.sendKeys(dataRow.get(3));
                //CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")).$(By.className("modal-body")).$("input")
                // .sendKeys(dataRow.get(3));
                CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")).$(By.className("modal-footer"))
                    .$(By.className("btn-primary")).click();
            }
        }
    }

    /**
     * @param table contains parameters:
     * page where kebab is located:  path | datatypes | main page | operations
     * section where kebab is located: query | header | response | RFD
     * name of element : e.g. headerParameter123
     * kebab operation: Rename | Delete | ...
     * new name for element e.g. /mySuperPath
     */
    @When("click on kebab menu and manage element")
    public void clickOnKebabMenuAndManageElement(DataTable table) {
        for (List<String> dataRow : table.cells()) {
            if (dataRow.get(1).isEmpty()) {
                //Usage for kebab for paths and data types
                CommonUtils.getKebabButtonOnElement(CommonUtils.getPageElement(dataRow.get(0))).click();
            } else {
                By sectionBy = CommonUtils.getSectionBy(dataRow.get(1));
                SelenideElement sectionElement = CommonUtils.getPageElement(dataRow.get(0)).$(sectionBy);
                SelenideElement elementRow;

                if ("response".equals(dataRow.get(1))) {
                    //Usage for responses
                    OperationUtils.selectResponse(dataRow.get(2));
                    elementRow = CommonUtils.getPageElement(dataRow.get(0)).$(sectionBy);
                } else {
                    elementRow = CommonUtils.getElementRow(sectionElement, dataRow.get(1), dataRow.get(2));
                }
                CommonUtils.getKebabButtonOnElement(elementRow).click();
            }

            CommonUtils.getDropdownMenuItem(dataRow.get(3)).click();
            if (!(dataRow.get(3).contains("Delete") || dataRow.get(3).contains("Sections"))) {
                if ("response".equals(dataRow.get(1))) {
                    //Change response status code
                    OperationUtils.getOperationRoot().$("#statusCodeDropDown").$("#statusCodeDropDown").click();
                    OperationUtils.getOperationRoot().$("#statusCodeDropDown").$(By.className("dropdown-menu")).$$(BasicElements.A)
                        .filter(text(dataRow.get(4))).first().click();
                } else {
                    //Change value in input field
                    SelenideElement se =
                        CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")).$(By.className("modal-body"))
                            .$("input");
                    se.clear();
                    Arrays.asList(dataRow.get(4).toCharArray()).forEach((character) -> {
                        se.sendKeys((String.valueOf(character)));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    //                    se.sendKeys(dataRow.get(4));
                }
                CommonUtils.getAppRoot().shouldBe(visible, enabled).shouldNotHave(attribute("disabled")).$(By.className("modal-footer"))
                    .$(By.className("btn-primary")).click();
            }
        }
    }
}
