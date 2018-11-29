// Creates menu for function execution
function onOpen( ){
  var ui = SpreadsheetApp.getUi();
      ui.createMenu('Custom Functions')
        .addItem('Refresh data', 'refreshData')
        .addToUi();
}

// Fetches scorecard data from BigQuery and writes results to worksheet named data
function refreshData() {
  var projectId = 'storyline-1250';
  var output_sheet = 'data'

  var request = {
    query: "SELECT * FROM [storyline-1250:report_automation_gkpi.processed_table_period_master_final] ORDER BY period_end_date DESC, report_type;"
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }

  if (rows) {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = spreadsheet.getSheetByName('data');
        sheet.clearContents();

    // Append the headers
    var headers = queryResults.schema.fields.map(function(field) {
      return field.name;
    });
    sheet.appendRow(headers);

    // Append the results
    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
    sheet.getRange(2, 1, rows.length, headers.length).setValues(data);

    Logger.log('Results spreadsheet created: %s',
        spreadsheet.getUrl());
  } else {
    Logger.log('No rows returned.');
  }
}

