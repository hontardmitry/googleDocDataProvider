package data.parser;

import static java.lang.String.format;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import data.provider.GoogleDocsDataProvider;
import org.jbehave.core.model.ExamplesTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GoogleDocsDataParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleDocsDataParser.class);
    private Map<String, List<String>> temporaryMapForNestedStructure;
    private String currentNestedStructureName;
    private Map<String, List<List<String>>> generalData = null;
    private Integer counter = 0;

    /**
     * Gets examples table.
     *
     * @param provider                DataProviderObject that already contains spreadsheet Id.
     * @param absolutePathToResources the absolute path to resources
     * @return the examples table object parsed from google spreadsheet
     */
    public ExamplesTable getExamplesTable(GoogleDocsDataProvider provider, String absolutePathToResources) {
        Map<String, List<List<String>>> spreedSheetsData = getSpreadsheetData(provider);
        provider.setDocId(System.getProperty("qaa.generalFile", "17cjvuNclg87xMUMAYU7NFsl3wDGfRF591HRZ75z-X3o"));
        if (generalData == null) {
            LOGGER.info("General file was no loaded yet. Loading general data file with id {}",
                    System.getProperty("qaa.generalFile"));
            generalData = getSpreadsheetData(provider);
        } else {
            LOGGER.info("General data file already loaded.");
        }
        ExamplesTable generalDataTable = parseDataToExampleTable(generalData, absolutePathToResources, null);
        Map<String, String> generalDataMap = generalDataTable.getRow(0);
        ExamplesTable et;
        try {
            et = parseDataToExampleTable(spreedSheetsData, absolutePathToResources, generalDataMap);
        } catch (Throwable throwable) {
            LOGGER.info("Error during reading examples table", throwable);
            throw new RuntimeException(throwable);
        }
        return et;
    }

    /*
     * In this method we are parsing google doc data to less complicated format and converting it to Examples table.
     *
     * @param spreedSheetsData
     * @return Examples table with data from google docs
     */

    private ExamplesTable parseDataToExampleTable(
            Map<String, List<List<String>>> spreedSheetsData,
            String absolutePathToResources,
            Map<String, String> generalDataMap) {
        LOGGER.info("Parsing google doc data to example table.");
        /*
          Table
          Examples:
           |RunName  |Variable      |
           |firstRun |firstVariable |
           |secondRun|secondVariable|

           is equals to that map:
           {
               "RunName": ["firstRun", "secondRun"],
               "Variable":["firstVariable","secondVariable"]
           }
         */
        Map<String, List<String>> exampleTableMapRepresentation = new HashMap<>();
        /*
            We are expecting that google sheet is contains of 3 different columns
            first column is a human readable label
            second column is data for that label
            And third column is labels for parsing created by automation engineer
         */
        spreedSheetsData.forEach((s, lists) -> {
            if (lists.size() != 3) {
                LOGGER.error("Wrong columns count in google spreadsheet");
                throw new RuntimeException("Wrong columns count in google spreadsheet");
            }
        });

        /*
         * Each cycle will parse 1 sheet from google document
         */
        for (Map.Entry<String, List<List<String>>> entry : spreedSheetsData.entrySet()) {
            String key = entry.getKey();
            List<List<String>> labels = entry.getValue();
            /*
             * Adding first column with run names manually
             * This column is needed only to know which data we are using in current test run on report portal
             */
            if (!exampleTableMapRepresentation.containsKey("RunName")) {
                exampleTableMapRepresentation.put("RunName", new ArrayList<>());
            }
            exampleTableMapRepresentation.get("RunName").add(key);

            if (generalDataMap != null) {
                for (Map.Entry<String, String> e : generalDataMap.entrySet()) {
                    String valueLabel = e.getKey();
                    String value = e.getValue();

                    if (!exampleTableMapRepresentation.containsKey(valueLabel)) {
                        exampleTableMapRepresentation.put(valueLabel, new ArrayList<>());
                    }
                    if (!"RunName".equals(valueLabel)) {
                        exampleTableMapRepresentation.get(valueLabel).add(value);
                    }
                }
            }
            //Getting 3rd column that contains tags for parsing
            List<String> get = labels.get(2);
            for (int i = 0; i < get.size(); i++) {
                //Processing each tag in the column.
                String label = get.get(i);
                //If current tag is empty or starts with '!' - we are ignoring that tag
                if (!label.startsWith("!") && !"".equals(label) && !label.startsWith("@")) {
                    //If our map doesn't already contain current tag - we are initializing new Array for that tag
                    if (!exampleTableMapRepresentation.containsKey(label)) {
                        exampleTableMapRepresentation.put(label, new ArrayList<>());
                    }
                    //And after initialization we are adding value from 2nd column to that array for that tag
                    exampleTableMapRepresentation.get(label).add(
                            labels.get(1).get(i).replaceAll("\n", " "));
                    //If tag starts with @ then we are processing that tag as a nested structure
                } else if (label.startsWith("@")) {
                    Map<String, List<String>> result = proceedNestedStructureRow(label, labels, i);
                    if (result != null) {
                        //When we are processed whole structure then we are writing the result to the file
                        String createdFilePath = createTempTableFile(result, absolutePathToResources);
                        if (!exampleTableMapRepresentation.containsKey(currentNestedStructureName)) {
                            exampleTableMapRepresentation.put(currentNestedStructureName, new ArrayList<>());
                        }
                        createdFilePath = createdFilePath.replace("\\", "/");
                        LOGGER.info("Temporary table path: {}", "file:///" + createdFilePath);
                        exampleTableMapRepresentation.get(currentNestedStructureName).add("file:///" + createdFilePath);
                    }
                }
            }
        }
        //When we processed whole google document - we are converting created map to string
        String exampleTableData = convertMapRepresentationToString(exampleTableMapRepresentation);
        return new ExamplesTable(exampleTableData);
    }

    private Map<String, List<String>> proceedNestedStructureRow(String label, List<List<String>> labels, Integer i) {
        //If its start of the structure - we are initializing new map for that structure
        // and adding table headers to that map
        if (label.contains("@startNestedStructure ")) {
            currentNestedStructureName = label.replace("@startNestedStructure", "");
            temporaryMapForNestedStructure = new HashMap<>();
            temporaryMapForNestedStructure.put("label", new ArrayList<>());
            temporaryMapForNestedStructure.put("type", new ArrayList<>());
            temporaryMapForNestedStructure.put("value", new ArrayList<>());
            return null;
        }

        //If its end of the structure we are returning map for further processing
        if ("@endNestedStructure".equals(label)) {
            return temporaryMapForNestedStructure;
        }
        //If its just normal row of nested structure
        label = label.replace("@", "");
        List<String> dataArray = Arrays.asList(label.split("#"));
        //Adding label from parsing tags
        temporaryMapForNestedStructure.get("label").add(dataArray.get(1));
        //Adding type from parsing tags
        temporaryMapForNestedStructure.get("type").add(dataArray.get(0));
        //Adding value from second column
        temporaryMapForNestedStructure.get("value").add(labels.get(1).get(i));

        return null;
    }

    // @return path to created file
    private String createTempTableFile(Map<String, List<String>> result, String absolutePathToResources) {
        //Converting map representation to string
        String data = convertMapRepresentationToString(result);
        //Checking that needed directory exist
        String filePath = absolutePathToResources + "\\data\\tables\\";
        File fileP = new File(filePath);
        if (!fileP.exists()) {
            LOGGER.error("Directories created: {}", fileP.mkdirs());
        }
        //Generating new name for the file
        String fileName = "testData" + UUID.randomUUID().toString() + ".table";
        LOGGER.info("File name generated. {}", fileName);
        Path file = Paths.get(filePath + fileName);
        try {
            File f = new File(filePath + "\\" + fileName);
            //deleting old file if it exist
            if (f.exists()) {
                LOGGER.info("File deleted: {}", f.delete());
            }
            //Writing data to new file
            LOGGER.info("Writing data to file");
            Files.write(file, Collections.singletonList(data), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            LOGGER.info("File created.");
        } catch (IOException e) {
            LOGGER.error("Failed to create temp table.", e);
            throw new RuntimeException("Failed to create temporary table", e);
        }
        return absolutePathToResources + "/data/tables/" + fileName;
    }

    private String convertMapRepresentationToString(Map<String, List<String>> dataToConvert) {
        if (dataToConvert.get("RunName") != null) {
            Integer runsCount = dataToConvert.get("RunName").size();
            Map<String, Integer> invalidLabels = new HashMap<>();
            for (Map.Entry<String, List<String>> e : dataToConvert.entrySet()) {
                String key = e.getKey();
                List<String> value = e.getValue();

                if (value.size() != runsCount) {
                    invalidLabels.put(key, value.size());
                }
            }
            if (!invalidLabels.isEmpty()) {
                LOGGER.info("Invalid labels in google doc data file. Each label should have {} entries.", runsCount);
                LOGGER.info("Invalid labels: ");
                for (Map.Entry<String, Integer> entry : invalidLabels.entrySet()) {
                    String label = entry.getKey();
                    Integer entiresCount = entry.getValue();
                    LOGGER.info("Label: {}, Entries count: {}", label, entiresCount);
                }
                throw new RuntimeException("Wrong labels entries count");
            } else {
                LOGGER.info("Simple google doc validation passed.");
            }
        }

        LOGGER.info("Converting map representation to string.");
        String result = "";
        /*
         * first row is a row of headers
         * and if we have, for example, that map:
         * {
               "RunName": ["firstRun", "secondRun"],
               "Variable":["firstVariable","secondVariable"]
           }
         *
         * Then after this step we will receive String with data:
         *  |RunName  |Variable      |
         */

        for (String header : dataToConvert.keySet()) {
            result += "|" + header;
        }
        result += "|\n";
        //Then we are iterating trough Lists of values and with each iteration we are writing 1 row of data.
        for (int i = 0; i < dataToConvert.values().iterator().next().size(); i++) {
            for (Map.Entry<String, List<String>> entry : dataToConvert.entrySet()) {
                String s = entry.getKey();
                List<String> strings = entry.getValue();

                result += "|" + strings.get(i);
            }
            result += "|\n";
        }
        return result;
    }

    /*
     * In this method we are retrieving data from google docs.
     *
     * @param Data provider
     * @return Map with data from Excel file
     */
    private Map<String, List<List<String>>> getSpreadsheetData(GoogleDocsDataProvider provider) {
        Map<String, List<List<String>>> result;
        LOGGER.info("Loading data from google docs.");
        LOGGER.info("Loaded files count: {}", ++counter);
        try {
            try {
                result = getData(provider);
            } catch (GoogleJsonResponseException | SocketTimeoutException e) {
                LOGGER.info("Failed to load data. {}", e.toString());
                Integer pauseLenght = 960;
                LOGGER.info("Waiting {} seconds to bypass google cloud platform quotas", pauseLenght);
                try {
                    for (Integer i = 1; i <= pauseLenght; i++) {
                        Thread.sleep(1000L);
                        if (i%5==0){
                            LOGGER.info("Waiting {} out of {} sec", i, pauseLenght);
                        }
                    }
                    provider.authorize();
                } catch (InterruptedException er) {
                    LOGGER.info("{}", er);
                }
                LOGGER.info("Waiting completed.");
                result = getData(provider);
            }
        } catch (IOException | GeneralSecurityException e) {
            LOGGER.info("Failed to load data. {}", e.toString());
            throw new RuntimeException("Failed to retrieve data from google docs", e);
        }
        LOGGER.info("Data loaded.");
        try {
            LOGGER.info("Waiting 15 sec to bypass google cloud platform quotas.");
            Thread.sleep(15000L);
        } catch (InterruptedException e) {
            LOGGER.info("Failed to load data. {}", e.toString());
        }
        return result;
    }

    private Map<String, List<List<String>>> getData(GoogleDocsDataProvider provider)
            throws IOException, GeneralSecurityException {
        Map<String, List<List<String>>> result = new HashMap<>();
        Sheets service = provider.getSheetsService();
        LOGGER.info("Getting sheets data.");
        Spreadsheet spreadsheet = service.spreadsheets().get(provider.getDocId()).execute();
        LOGGER.info("Got sheets data.");
        for (Sheet sheet : spreadsheet.getSheets()) {
            String sheetTitle = sheet.getProperties().getTitle();
            // If sheet name starts with '!' char then we are ignoring that list
            if (!sheetTitle.startsWith("!")) {
                String range = format("%s!A:Z", sheetTitle);
                ValueRange response = service
                        .spreadsheets()
                        .values()
                        .get(provider.getDocId(), range)
                        .setMajorDimension("COLUMNS")
                        .execute();
                // Google docs always returning List<List<String>> from Excel sheets,
                // so in my opinion it's okay to cast it like that
                result.put(sheetTitle, (List<List<String>>) (Object) response.getValues());
            }
        }
        return result;
    }
}
