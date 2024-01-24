package com.flipkart.yaktest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yaktest.interruption.models.InterruptionName;
import com.flipkart.yaktest.output.InterruptionStatus;
import com.flipkart.yaktest.output.TestOutput;
import com.flipkart.yaktest.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class HtmlGenerator {

    private static final String HTML_LAST = "</tbody> </table> </body> </html>";

    public static void main(String[] args) throws Exception {
        List<TestOutput> output = readFile(FileUtils.getLogPath() + "final_output.json");

        StringBuilder allRows = new StringBuilder("");
        String col1 = "<td class=\"align-middle text-capitalize text-info\"><h4>{testCaseName}</h4></td>";
        String col2 = "<td class=\"align-middle\">{interruptions}</td>";
        String col2ListItem =
                "<li class=\"list-group-item list-group-item-danger\">{interruption}-{count} <span class=\"badge badge-light badge-lg\">{status}</span></li>";
        String col4 = "<td class=\"align-middle\">{testStatusMetrices}</td>";
        String col6 = "<td class=\"align-middle\"><h4><span class=\"badge\">{overallStatus}</span></h4></td>";

        for (TestOutput testOutput : output) {
            String row = " <tr>";
            row += col1.replaceAll("\\{testCaseName\\}", testOutput.getTestCaseName().name());

            String listHtml = "<ul class=\"list-group\">{list}</ul>";
            StringBuilder listItemHtml = new StringBuilder("");
            for (Map.Entry<InterruptionName, InterruptionStatus> entry : testOutput.getInterruptionsStatus().entrySet()) {
                listItemHtml.append(
                        col2ListItem.replaceAll("\\{interruption\\}", entry.getKey().name()).replaceAll("\\{count\\}", "" + entry.getValue().getCount())
                                .replaceAll("\\{status\\}", "" + entry.getValue().getStatus().name()));
            }
            row += col2.replaceAll("\\{interruptions\\}", listHtml.replaceAll("\\{list\\}", listItemHtml.toString()));

            listItemHtml = new StringBuilder();
            row += col4.replaceAll("\\{testStatusMetrices\\}", listHtml.replaceAll("\\{list\\}", listItemHtml.toString()));
            row += col6.replaceAll("\\{overallStatus\\}", testOutput.getTestStatus().getOverallStatus().name());
            row += "</tr>";
            allRows.append(row);
        }

        String htmlFront = "<!DOCTYPE html>" + "<html>" + "<head>" + "    <link rel=\"stylesheet\" href=\"https://use.fontawesome.com/releases/v5.4.2/css/all.css\""
                + "          integrity=\"sha384-/rXc/GQVaYpyDdyxK+ecHPVYJSN9bmVFBvjA/9eOB+pb3F2w2N6fc5qB9Ew5yIns\" crossorigin=\"anonymous\">"
                + "    <script src=\"https://cdnjs.cloudflare.com/ajax/libs/angular.js/1.6.5/angular.js\"></script>"
                + "    <link rel=\"stylesheet\" href=\"https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css\""
                + "          integrity=\"sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO\" crossorigin=\"anonymous\">"
                + "    <script src=\"https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/js/bootstrap.min.js\""
                + "            integrity=\"sha384-ChfqqxuZUCnJSK3+MXmPNIyE6ZbWh2IMqE241rYiqJxyMiZ6OW/JmZQ5stwEULTy\""
                + "            crossorigin=\"anonymous\"></script>" + "</head>" + "<body>" + "<div align=\"center\">" + "    <br><h1>Yak Testbed </h1><hr>"
                + "    <h5>Total No. of Test - {size} </h5><br>" + "</div>"
                + "<table class=\"table table-responsive table-bordered text-center table-light\">" + "    <thead class=\"thead-dark\">" + "    <tr>"
                + "        <th>Test Name</th>" + "        <th>Interruptions</th>" + "        <th>PreDataLoad</th>" + "        <th>Test Output</th>"
                + "        <th>Running Time</th>" + "        <th>Overall Status</th>" + "        <th>Inconsistency Status</th>"
                + "        <th>Wal Isolation Status</th>" + "        <th>Kafka Connection Status</th>" + "    </tr>" + "    </thead>" + "    <tbody>";
        String finalOutput = htmlFront.replaceAll("\\{size\\}", "" + output.size()) + allRows.toString() + HTML_LAST;
        writeOutputToFile(finalOutput.replace('\\', ' '));
    }

    private static void writeOutputToFile(String htmlString) throws IOException {

        Path path = Paths.get(FileUtils.getLogPath() + "testbed_report.html");
        Files.write(path, htmlString.getBytes());
    }

    private static List<TestOutput> readFile(String filePath) throws IOException {
        return new ObjectMapper().readValue(Paths.get(filePath).toFile(), new TypeReference<List<TestOutput>>() {
        });
    }

}
