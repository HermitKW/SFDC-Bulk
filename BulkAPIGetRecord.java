package com.example.sfdcSyncToMdmSpringBoot.common.service;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BulkAPIGetRecord {
    private static BulkConnection bulkConnection = null;
    public static void main(String[] args) {
        BulkAPIGetRecord.doBulkQuery();
    }
    public static String token;
    static String protocol = "https";
    static String host = "hkbn--escrmuat.sandbox.my.salesforce.com";

    public static URL testUrl;

    public static boolean login() {
        boolean success = false;

        String userId = "dennis.wong.esit@hkbn.com.hk.escrmuat";
        // password = (your password)+(token, can be found at SF). if your password is "123" and token is "ABC", then the string below should be "123ABC"
        String passwd = "Exlogin12345!qOJejou0LBiwpfFim1kdttxcu";
        String soapAuthEndPoint = "https://hkbn--escrmuat.sandbox.my.salesforce.com/services/Soap/u/57.0";
        String bulkAuthEndPoint = "https://hkbn--escrmuat.sandbox.my.salesforce.com/services/async/57.0";
        try {
            ConnectorConfig config = new ConnectorConfig();
            config.setUsername(userId);
            config.setPassword(passwd);
            config.setAuthEndpoint(soapAuthEndPoint);
            config.setCompression(true);
            config.setTraceFile("traceLogs.txt");
            config.setTraceMessage(true);
            config.setPrettyPrintXml(true);
            config.setRestEndpoint(bulkAuthEndPoint);
            System.out.println("AuthEndpoint: " +
                    config.getRestEndpoint());
            PartnerConnection connection = new PartnerConnection(config);
            token = config.getSessionId();
            if (config.getSessionId() == null) {
                throw new AsyncApiException("session ID not found",
                        AsyncExceptionCode.ClientInputError);
            }

            System.out.println("SessionID: " + config.getSessionId());
            bulkConnection = new BulkConnection(config);
            success = true;
        } catch (AsyncApiException aae) {
            aae.printStackTrace();
        } catch (ConnectionException ce) {
            ce.printStackTrace();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }
        return success;
    }

    public static void doBulkQuery(){
        if ( ! login() ) {
        }
        try {
            JobInfo job = new JobInfo();
            job.setObject("Opportunity");

            job.setOperation(OperationEnum.query);
            job.setConcurrencyMode(ConcurrencyMode.Parallel);
            job.setContentType(ContentType.CSV
            );
            job = bulkConnection.createJob(job);
            assert job.getId() != null;

            job = bulkConnection.getJobStatus(job.getId());

            String query =
                    "SELECT Name, Id FROM Opportunity";

            long start = System.currentTimeMillis();
            String jobId = job.getId();
            BatchInfo info = null;
            ByteArrayInputStream bout =
                    new ByteArrayInputStream(query.getBytes());
            info = bulkConnection.createBatchFromStream(job, bout);
            String batchId = info.getId();
            String[] queryResults = null;

            for(int i=0; i<10000; i++) {
                Thread.sleep(i==0 ? 30 * 1000 : 30 * 1000); //30 sec
                info = bulkConnection.getBatchInfo(jobId,
                        info.getId());
                // try this to export excel
                // bulkConnect.(any function can export the result?)
                // Get an active salesforce session ID/token
                //enterprise API - login() SOAP method
                //Get your organization ID ("org ID")
                //Setup > Company Profile > Company Information OR
                //use the enterprise API getUserInfo() SOAP call to retrieve your org ID
                //Send an HTTP GET request to https://{your sf.com instance}.salesforce.com/ui/setup/export/DataExportPage/d?setupid=DataManagementExport
                //Set the request cookie as follows:
                //oid={your org ID}; sid={your session ID};
                //Parse the resulting HTML for instances of <a href="/servlet/servlet.OrgExport?fileName=
                //(The filename begins after fileName=)
                //Plug the file names into this URL to download (and save):
                //https://{your sf.com instance}.salesforce.com/servlet/servlet.OrgExport?fileName={filename}
                //Use the same cookie as in step 3 when downloading the files
                //https://stackoverflow.com/questions/10178279/how-to-automate-download-of-weekly-export-service-files

                if (info.getState() == BatchStateEnum.Completed) {
                    QueryResultList list =
                            bulkConnection.getQueryResultList(jobId,
                                    info.getId());
                    queryResults = list.getResult();
                    break;
                } else if (info.getState() == BatchStateEnum.Failed) {
                    System.out.println("-------------- failed ----------"
                            + info);
                    break;
                } else {
                    System.out.println("-------------- waiting ----------"
                            + info);
                }
            }
            if (queryResults != null) {
                for (String resultId : queryResults) {
                    bulkConnection.getQueryResultStream(jobId,
                            info.getId(), resultId);
                }
                System.out.println("Output complete, please check output file.");
                    System.out.println("jobId: " + jobId);
                    try {
                        getBulkJobResult(jobId);
                        getBatchResult(batchId, jobId);
                    }catch(Exception e){
                        System.out.println("JobId not ready yet,jobid: " + jobId);
                    }
                }
        } catch (AsyncApiException aae) {
            aae.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
    public static void getBulkJobResult(String jobId) throws IOException, InterruptedException, AsyncApiException {
        String file = "/services/data/v57.0/jobs/query/";
        testUrl = new URL(protocol, host, file);
        System.out.println("url.getQuery()" + testUrl.getQuery());
        HttpURLConnection con = (HttpURLConnection) testUrl.openConnection();
        // optional default is GET
        con.setRequestMethod("GET");
        //add request header
        con.setRequestProperty("Authorization", "Bearer "+ token);
        System.out.println("getBulkQueryResult token: " + token);
        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + testUrl); // + url
        System.out.println("Response Code : " + responseCode);
        if(responseCode == 404) {
            System.out.println("Resonse Code 404, probably JobId is null");
            Thread.sleep(10000);
            getBulkJobResult(jobId);
        }

        printResponse(con);
        Thread.sleep(3000);

        System.out.println("Getting Batch Result... ");

        Thread.sleep(10000);
        System.out.println("Closing Job: " + jobId);
        closeJob(bulkConnection, jobId);
    }
    public static void getBatchResult(String batchId, String jobId) throws IOException, InterruptedException, AsyncApiException {
        System.out.println("batchId: " + batchId);
        String batchURL = "/services/async/57.0/job/" + jobId + "/batch/" + batchId + "/result";
        testUrl = new URL(protocol, host, batchURL);
        System.out.println("url.getQuery()" + testUrl.getQuery());
        HttpURLConnection con = (HttpURLConnection) testUrl.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("X-SFDC-Session", token);
        System.out.println("getBatchQueryResult X-SFDC-Session token: " + token);
        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + testUrl); // + url
        System.out.println("Response Code : " + responseCode);

        //get result id
        BufferedReader br = null;
        br = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String strCurrentLine = null;
        String batchResultId = null;
        while ((strCurrentLine = br.readLine()) != null) {
            System.out.println("Batch " + batchId + " result: " + strCurrentLine);
            batchResultId = strCurrentLine.split("<result>")[1].split("</result>")[0];
            System.out.println("Query result Code: " + batchResultId);
        }

        if (responseCode == 404) {
            System.out.println("Resonse Code 404, probably BatchId is wrong");
            Thread.sleep(10000);
            getBatchResult(batchId, jobId);
        }
        printResponse(con);
        getBatchResponseResult(batchResultId, batchId, jobId);
    }

    public static void getBatchResponseResult(String batchResultId, String batchId, String jobId) throws IOException {
        String batchResultURL = "/services/async/57.0/job/" + jobId + "/batch/" + batchId + "/result/" + batchResultId ;
        testUrl = new URL(protocol, host, batchResultURL);
        HttpURLConnection con = (HttpURLConnection) testUrl.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("X-SFDC-Session", token);
        int responseCode = con.getResponseCode();

        System.out.println("getBatchQueryResult X-SFDC-Session token: " + token);
        System.out.println("\nSending 'GET' request to URL : " + testUrl); // + url
        System.out.println("Response Code : " + responseCode);

        printResponse(con);
    }
    //close job, closeJob() is more concise.
    /*
    public static void CloseBulkQueryJob(String JobId) throws IOException, InterruptedException {
        String file = "/services/data/v57.0/jobs/ingest/"+ JobId;
        testUrl = new URL(protocol, host, file);
        System.out.println("url.getQuery()" + testUrl.getQuery());
        HttpURLConnection con = (HttpURLConnection) testUrl.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Authorization", "Bearer "+ token);
        int responseCode = con.getResponseCode();
        System.out.println("getBulkQueryResult token: " + token);
        System.out.println("\nSending 'GET' request to URL : " + testUrl); // + url
        System.out.println("Response Code : " + responseCode);
    }
     */
    private static void closeJob(BulkConnection bulkConnection, String JobId)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(JobId);
        job.setState(JobStateEnum.Closed);
        bulkConnection.updateJob(job);
    }

    public static void printResponse(HttpURLConnection con) throws IOException {
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        System.out.println("response: " + response.toString());
    }
    public static void writeIntoDB() throws SQLException {
        try (
                Connection conn = DriverManager.getConnection(
                        "jdbc:oracle:thin:@//x08ddb02-vip.hkbn.com.hk:1525/ESBNUAT",
                        "edge21", "uat123!123TAU");
                Statement stmt = conn.createStatement();

        ){

        }
    }
}
