function copyData() {
  importRange(
    "xxx",
    "yyy",
    "xxx",
    "yyy" );
}
function importRange(sourceID, sourceRange, targetID, targetRange) {
const sourceSS = SpreadsheetApp.openById(sourceID);
const sourceRng = sourceSS. getRange (sourceRange);
const sourceVals = sourceRng.getValues();

const targetSS = SpreadsheetApp.openById(targetID);
const targetRng = targetSS.getRange(targetRange);
const targetSheet = targetSS.getSheetByName(targetRng.getSheet().getName());
targetSheet.clear ()

const targetVals = targetSheet.getRange(
targetRng.getRow(),
targetRng.getColumn(),
sourceVals.length,
sourceVals[0].length 
)
  targetVals.setValues(sourceVals);
}